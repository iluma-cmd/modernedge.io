"""
Main email enrichment service orchestrator with job queuing and batching
Optimized for background service deployment with health monitoring and graceful shutdown
"""
# -*- coding: utf-8 -*-
import asyncio
import signal
import sys
import time
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional
from uuid import UUID

import typer
import uvicorn
from fastapi import FastAPI, HTTPException
from loguru import logger

from config import get_settings
from database import get_db_client
from email_generator import get_email_generator
from hunter_client import get_hunter_client
from job_manager import get_job_manager
from models import EnrichmentJob, Lead, ProcessingStats, ValidatedEmail, EnrichedLead
from perplexity_client import get_perplexity_client
from validator import get_email_validator

# CLI Application
app = typer.Typer(help="Email Enrichment Service - Production Background Service")


class EmailEnrichmentService:
    """Main service orchestrating the email enrichment workflow with job queuing"""

    def __init__(self):
        self.settings = get_settings()
        self.db_client = None
        self.hunter_client = None
        self.perplexity_client = None
        self.email_generator = None
        self.email_validator = None
        self.job_manager = None
        self._shutdown_event = asyncio.Event()
        self._is_healthy = False
        self._startup_complete = False
        self._background_task = None

        # Health check caching to reduce database load
        self._health_cache = {}
        self._health_cache_time = 0
        self._health_cache_ttl = 30  # Cache health status for 30 seconds

    async def _initialize_clients(self):
        """Initialize all API clients and job manager"""
        try:
            logger.info("Initializing service clients...")
            self.db_client = await get_db_client()
            self.hunter_client = await get_hunter_client()
            self.perplexity_client = await get_perplexity_client()
            self.email_generator = get_email_generator()
            self.email_validator = await get_email_validator()
            self.job_manager = await get_job_manager()

            # Ensure jobs table exists
            await self.db_client.create_jobs_table_if_not_exists()

            # Setup enhanced signal handlers for graceful shutdown
            self._setup_signal_handlers()
            
            self._is_healthy = True
            self._startup_complete = True
            logger.info("Service clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize service clients: {e}")
            self._is_healthy = False
            raise

    def _setup_signal_handlers(self):
        """Setup enhanced signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum} ({signal.Signals(signum).name}), initiating graceful shutdown")
            self._shutdown_event.set()
            
            # For immediate shutdown on second signal
            def force_shutdown(signum, frame):
                logger.warning("Received second shutdown signal, forcing immediate exit")
                sys.exit(1)
                
            signal.signal(signum, force_shutdown)

        # Setup signal handlers for different platforms
        signals_to_handle = [signal.SIGTERM]
        if sys.platform != "win32":
            signals_to_handle.extend([signal.SIGINT, signal.SIGHUP])
        else:
            signals_to_handle.append(signal.SIGINT)

        for sig in signals_to_handle:
            try:
                signal.signal(sig, signal_handler)
                logger.debug(f"Registered signal handler for {signal.Signals(sig).name}")
            except (OSError, ValueError) as e:
                logger.warning(f"Failed to register signal handler for {signal.Signals(sig).name}: {e}")

    async def _cleanup_clients(self):
        """Clean up client connections gracefully"""
        logger.info("Cleaning up service clients...")
        self._is_healthy = False
        
        cleanup_tasks = []
        
        if self.hunter_client:
            cleanup_tasks.append(self._safe_close(self.hunter_client, "Hunter client"))
        if self.perplexity_client:
            cleanup_tasks.append(self._safe_close(self.perplexity_client, "Perplexity client"))
        if self.db_client:
            cleanup_tasks.append(self._safe_close(self.db_client, "Database client"))
            
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            
        logger.info("Service clients cleanup completed")

    async def _safe_close(self, client, name: str):
        """Safely close a client with timeout and error handling"""
        try:
            if hasattr(client, 'close'):
                await asyncio.wait_for(client.close(), timeout=10.0)
                logger.debug(f"{name} closed successfully")
        except asyncio.TimeoutError:
            logger.warning(f"Timeout while closing {name}")
        except Exception as e:
            logger.error(f"Error closing {name}: {e}")

    async def health_check(self) -> dict:
        """Comprehensive health check for the service"""
        health_status = {
            "status": "healthy" if self._is_healthy else "unhealthy",
            "startup_complete": self._startup_complete,
            "components": {}
        }
        
        if not self._startup_complete:
            health_status["status"] = "starting"
            return health_status
            
        # Check database connection
        try:
            if self.db_client:
                stats = await asyncio.wait_for(
                    self.db_client.get_processing_stats(verbose=False),
                    timeout=5.0
                )
                health_status["components"]["database"] = {
                    "status": "healthy",
                    "stats": stats
                }
            else:
                health_status["components"]["database"] = {"status": "not_initialized"}
        except Exception as e:
            health_status["components"]["database"] = {
                "status": "unhealthy", 
                "error": str(e)
            }
            health_status["status"] = "unhealthy"

        # Check job manager with caching to reduce database load
        current_time = time.time()
        if current_time - self._health_cache_time > self._health_cache_ttl:
            try:
                if self.job_manager:
                    queue_status = await asyncio.wait_for(
                        self.job_manager.get_job_queue_status(),
                        timeout=5.0
                    )
                    self._health_cache["job_manager"] = {
                        "status": "healthy",
                        "queue_status": queue_status
                    }
                else:
                    self._health_cache["job_manager"] = {"status": "not_initialized"}

                self._health_cache_time = current_time

            except Exception as e:
                self._health_cache["job_manager"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                self._health_cache_time = current_time
                health_status["status"] = "unhealthy"

        # Use cached job manager status
        health_status["components"]["job_manager"] = self._health_cache.get("job_manager", {"status": "unknown"})

        # Check API clients
        health_status["components"]["hunter_client"] = {
            "status": "initialized" if self.hunter_client else "not_initialized"
        }
        health_status["components"]["perplexity_client"] = {
            "status": "initialized" if self.perplexity_client else "not_initialized"
        }
        
        return health_status

    def _convert_enriched_leads_to_leads(self, enriched_leads: List[EnrichedLead]) -> List[Lead]:
        """
        Convert enriched leads to Lead objects for processing
        
        Args:
            enriched_leads: List of enriched leads without emails
            
        Returns:
            List of Lead objects
        """
        leads = []
        for enriched_lead in enriched_leads:
            try:
                # Extract domain from company_website
                domain = None
                if enriched_lead.company_website:
                    # Extract domain from URL
                    from urllib.parse import urlparse
                    parsed = urlparse(enriched_lead.company_website)
                    domain = parsed.netloc.replace('www.', '') if parsed.netloc else None
                
                # If no domain from website, try to infer from company name
                if not domain and enriched_lead.company_name:
                    # Simple domain inference (this could be improved)
                    company_name = enriched_lead.company_name.lower()
                    # Remove common suffixes
                    for suffix in [' llc', ' inc', ' corp', ' ltd', ' pllc', ' pc']:
                        company_name = company_name.replace(suffix, '')
                    # Replace spaces with nothing and add .com
                    domain = company_name.replace(' ', '').replace('.', '') + '.com'
                
                if domain:
                    lead = Lead(
                        id=enriched_lead.lead_id,  # Use the original lead_id
                        company_name=enriched_lead.company_name,
                        domain=domain,
                        industry=None,  # Not available in enriched lead
                        company_size=None,
                        location=f"{enriched_lead.city}, {enriched_lead.state}" if enriched_lead.city else None,
                        created_at=enriched_lead.created_at,
                        updated_at=enriched_lead.updated_at
                    )
                    # Store enriched lead ID for later updates
                    lead._enriched_lead_id = enriched_lead.id
                    leads.append(lead)
                else:
                    logger.warning(f"Could not extract domain for enriched lead {enriched_lead.id}")
                    
            except Exception as e:
                logger.error(f"Failed to convert enriched lead {enriched_lead.id} to lead: {e}")
                
        logger.info(f"Converted {len(leads)} enriched leads to leads for processing")
        return leads

    async def _get_existing_company_data(self, lead_id: UUID) -> dict:
        """
        Get existing company data from other enriched leads for the same lead.
        This ensures company information is shared across different enrichment sources.

        Args:
            lead_id: The lead ID to get company data for

        Returns:
            Dict with company data (company_name, company_website, etc.)
        """
        try:
            # Query existing enriched leads for this lead
            existing_leads = await self.db_client.fetch_enriched_leads(lead_id=lead_id)

            company_data = {}

            # Collect company data from existing records
            for existing_lead in existing_leads:
                if existing_lead.company_name and not company_data.get("company_name"):
                    company_data["company_name"] = existing_lead.company_name
                if existing_lead.company_website and not company_data.get("company_website"):
                    company_data["company_website"] = existing_lead.company_website
                if existing_lead.industry and not company_data.get("industry"):
                    company_data["industry"] = existing_lead.industry
                if existing_lead.linkedin_company_url and not company_data.get("linkedin_company_url"):
                    company_data["linkedin_company_url"] = existing_lead.linkedin_company_url

            return company_data

        except Exception as e:
            logger.warning(f"Failed to get existing company data for lead {lead_id}: {e}")
            return {}

    async def _check_shutdown(self):
        """Check if shutdown has been requested"""
        if self._shutdown_event.is_set():
            raise asyncio.CancelledError("Shutdown requested")

    async def create_enrichment_job(
        self,
        lead_ids: Optional[List[UUID]] = None,
        domains: Optional[List[str]] = None,
        process_all: bool = False,
        skip_existing: bool = True,
        max_concurrent: Optional[int] = None,
        batch_size: Optional[int] = None
    ) -> EnrichmentJob:
        """
        Create a new enrichment job with conflict checking

        Args:
            lead_ids: Specific lead IDs to process
            domains: Specific domains to process
            process_all: Process all leads
            skip_existing: Skip leads with existing emails
            max_concurrent: Maximum concurrent processing
            batch_size: Size of processing batches

        Returns:
            Created job object
        """
        # Initialize clients if not already done
        if self.job_manager is None:
            await self._initialize_clients()

        return await self.job_manager.create_job(
            lead_ids=lead_ids,
            domains=domains,
            process_all=process_all,
            skip_existing=skip_existing,
            max_concurrent=max_concurrent,
            batch_size=batch_size
        )


    async def create_scraping_run_job(
        self,
        scraping_run_id: UUID,
        lead_domain: Optional[str] = None,
        max_concurrent: Optional[int] = None,
        batch_size: Optional[int] = None
    ) -> EnrichmentJob:
        """
        Create a new enrichment job for processing leads from a specific scraping run

        Args:
            scraping_run_id: UUID of the scraping run to process
            lead_domain: Optional specific domain to filter by
            max_concurrent: Maximum concurrent processing
            batch_size: Size of processing batches

        Returns:
            Created job object
        """
        # Initialize clients if not already done
        if self.job_manager is None:
            await self._initialize_clients()

        # Create job with scraping run metadata
        job_metadata = {
            "scraping_run_id": str(scraping_run_id),
            "lead_domain": lead_domain
        }

        return await self.job_manager.create_job(
            process_all=False,  # We'll specify leads via metadata
            skip_existing=True,
            max_concurrent=max_concurrent,
            batch_size=batch_size,
            metadata=job_metadata
        )


    async def process_single_lead(self, lead: Lead, job_id: str, job_metadata: Optional[dict] = None) -> ProcessingStats:
        """
        Process a single lead through the complete enrichment workflow

        Args:
            lead: Lead to process
            job_id: Associated job ID for progress tracking
            job_metadata: Optional job metadata (e.g., scraping_run_id)

        Returns:
            Processing statistics for this lead
        """
        await self._check_shutdown()

        start_time = time.time()
        stats = ProcessingStats()

        # Validate inputs
        if not lead or not lead.id or not lead.domain:
            logger.error("Invalid lead provided for processing")
            stats.errors.append("Invalid lead data provided")
            return stats

        if not job_id or not isinstance(job_id, str):
            logger.error(f"Invalid job_id provided: {job_id}")
            stats.errors.append("Invalid job_id provided")
            return stats

        logger.info(f"Processing lead: {lead.company_name} ({lead.domain})")

        try:
            # Check for existing emails (skip this check if we're updating an enriched lead)
            if not (hasattr(lead, '_enriched_lead_id') and lead._enriched_lead_id):
                existing_emails = await self.db_client.get_existing_emails_for_lead(lead.id)

                if existing_emails and self.settings.skip_existing:
                    logger.info(f"Skipping lead {lead.id} - already has {len(existing_emails)} emails")
                    return stats
            else:
                # We're updating an existing enriched lead, so don't check for existing emails
                existing_emails = []

            validated_emails = []

            # Step 1: Try Hunter.io domain search first
            try:
                hunter_response = await self.hunter_client.domain_search(lead.domain)

                if hunter_response and hunter_response.emails:
                        # Validate Hunter emails (will use generic emails as fallback if no personal emails found)
                        hunter_validated = await self.email_validator.validate_hunter_emails(
                            hunter_response.emails,
                            existing_emails
                        )
                        validated_emails.extend(hunter_validated)
                        stats.emails_found_hunter = len(hunter_validated)
                        stats.api_calls_hunter += 1

                        logger.info(f"Found {len(hunter_validated)} valid emails via Hunter for {lead.domain}")

            except Exception as e:
                logger.warning(f"Hunter domain search failed for {lead.domain}: {e}")
                stats.errors.append(f"Hunter search failed: {str(e)}")

            # Check for shutdown between API calls
            await self._check_shutdown()

            # Step 2: Try Perplexity if Hunter failed, didn't find enough emails, or emails lack personal info
            should_try_perplexity = (
                not validated_emails or  # No emails from Hunter
                len(validated_emails) < self.settings.max_emails_per_domain  # Not enough emails
            )

            if should_try_perplexity:
                try:
                    person_info = await self.perplexity_client.find_person_name(lead.domain)

                    if person_info:
                        # Generate email permutations
                        generated_emails = self.email_generator.generate_email_permutations(
                            person_info,
                            lead.domain,
                            max_emails=self.settings.max_emails_per_domain
                        )

                        if generated_emails:
                            # Validate generated emails
                            perplexity_validated = await self.email_validator.validate_generated_emails(
                                generated_emails,
                                existing_emails + [v.email for v in validated_emails]
                            )
                            validated_emails.extend(perplexity_validated)
                            stats.emails_found_perplexity = len(perplexity_validated)
                            stats.api_calls_perplexity += 1

                            logger.info(f"Found {len(perplexity_validated)} valid emails via Perplexity for {lead.domain}")

                except Exception as e:
                    logger.warning(f"Perplexity fallback failed for {lead.domain}: {e}")
                    stats.errors.append(f"Perplexity fallback failed: {str(e)}")

            # Step 3: Update existing enriched leads or save new ones
            if validated_emails:
                # Check if this is an enriched lead update (has _enriched_lead_id)
                if hasattr(lead, '_enriched_lead_id') and lead._enriched_lead_id:
                    # Update existing enriched lead with email
                    logger.info(f"Updating existing enriched lead {lead._enriched_lead_id} with email")
                    
                    # Use the first validated email to update the enriched lead
                    validated_email = validated_emails[0]
                    
                    # Find personal LinkedIn profile if we have a name
                    person_linkedin = None
                    if validated_email.first_name and validated_email.last_name:
                        full_name = f"{validated_email.first_name} {validated_email.last_name}"
                        logger.info(f"Finding LinkedIn profile for {full_name}...")
                        try:
                            person_linkedin = await self.perplexity_client.find_linkedin_profile(full_name, lead.domain)
                            if person_linkedin:
                                logger.info(f"Found personal LinkedIn: {person_linkedin}")
                        except Exception as e:
                            logger.warning(f"LinkedIn search failed for {full_name}: {e}")
                    
                    # Update the enriched lead
                    logger.debug(f"Updating enriched lead {lead._enriched_lead_id} (type: {type(lead._enriched_lead_id)}) with job_id {job_id}")
                    success = await self.db_client.update_enriched_lead_with_email(
                        enriched_lead_id=lead._enriched_lead_id,
                        email=validated_email.email,
                        first_name=validated_email.first_name,
                        last_name=validated_email.last_name,
                        job_title=validated_email.position,
                        linkedin_person_url=person_linkedin,
                        verification_status=validated_email.hunter_status,
                        verification_score=validated_email.confidence_score,
                        enrichment_source=f"{validated_email.source}_email_enrichment",
                        confidence=validated_email.confidence_score,
                        processed_by_job_id=job_id
                    )
                    
                    if success:
                        stats.emails_validated = 1
                        logger.info(f"Successfully updated enriched lead {lead._enriched_lead_id} with email {validated_email.email}")
                    else:
                        stats.errors.append(f"Failed to update enriched lead {lead._enriched_lead_id}")
                        logger.error(f"Failed to update enriched lead {lead._enriched_lead_id}")
                        
                else:
                    # Original logic for creating new enriched leads
                    # Get existing company data from other enriched leads for this lead
                    existing_company_data = await self._get_existing_company_data(lead.id)

                    # Deduplicate emails to avoid creating duplicate enriched leads
                    seen_emails = set()
                    deduplicated_emails = []
                    for email in validated_emails:
                        if email.email not in seen_emails:
                            seen_emails.add(email.email)
                            deduplicated_emails.append(email)
                        else:
                            logger.debug(f"Deduplicated duplicate email {email.email} for lead {lead.id}")

                    # Create enriched lead objects from deduplicated emails
                    enriched_leads = []
                    for validated_email in deduplicated_emails[:self.settings.max_emails_per_domain]:
                        try:
                            # Validate and sanitize enrichment source
                            enrichment_source = validated_email.source
                            if enrichment_source not in ["hunter_direct", "perplexity_generated"]:
                                logger.warning(f"Invalid enrichment source '{enrichment_source}' for lead {lead.id}, defaulting to 'unknown'")
                                enrichment_source = "unknown"

                            # Handle confidence score safely
                            confidence = validated_email.confidence_score
                            if confidence is None or not isinstance(confidence, int) or confidence < 0 or confidence > 100:
                                confidence = None

                            # Build enrichment reason safely
                            confidence_text = f" with confidence {confidence}" if confidence is not None else ""
                            enrichment_reason = f"Enriched via {enrichment_source}{confidence_text}"

                            # Extract verification data safely
                            verification_score = None
                            email_disposable = None
                            email_webmail = None

                            if validated_email.verification:
                                verification_score = getattr(validated_email.verification, 'score', None)
                                if verification_score is not None and (not isinstance(verification_score, int) or verification_score < 0 or verification_score > 100):
                                    verification_score = None

                                email_disposable = getattr(validated_email.verification, 'disposable', None)
                                email_webmail = getattr(validated_email.verification, 'webmail', None)

                            # Clean up text fields - use existing company data if available
                            company_name = (existing_company_data.get("company_name") or
                                           (lead.company_name.strip() if lead.company_name else None))
                            first_name = validated_email.first_name.strip() if validated_email.first_name else None
                            last_name = validated_email.last_name.strip() if validated_email.last_name else None
                            job_title = validated_email.position.strip() if validated_email.position else None

                            # Extract scraping_run_id from job metadata if available
                            scraping_run_id = None
                            if job_metadata and job_metadata.get("scraping_run_id"):
                                try:
                                    scraping_run_id = UUID(job_metadata["scraping_run_id"])
                                except (ValueError, TypeError):
                                    logger.warning(f"Invalid scraping_run_id in job metadata: {job_metadata.get('scraping_run_id')}")

                            enriched_lead = EnrichedLead(
                                lead_id=lead.id,
                                scraping_run_id=scraping_run_id,
                                email=validated_email.email,
                                first_name=first_name,
                                last_name=last_name,
                                job_title=job_title,
                                company_name=company_name,
                                company_website=existing_company_data.get("company_website"),
                                linkedin_company_url=existing_company_data.get("linkedin_company_url"),
                                enrichment_source=enrichment_source,
                                confidence=confidence,
                                verification_status=validated_email.hunter_status,
                                verification_score=verification_score,
                                email_disposable=email_disposable,
                                email_webmail=email_webmail,
                                processed_by_job_id=job_id,
                                enrichment_reason=enrichment_reason
                            )
                            enriched_leads.append(enriched_lead)

                        except Exception as e:
                            logger.error(f"Failed to create enriched lead object for email {validated_email.email}: {e}")
                            stats.errors.append(f"Failed to create enriched lead for {validated_email.email}: {str(e)}")
                            continue

                    if enriched_leads:
                        success = await self.db_client.save_enriched_leads(enriched_leads)

                        if success:
                            stats.emails_validated = len(enriched_leads)
                            logger.info(f"Successfully saved {len(enriched_leads)} enriched leads for lead {lead.id}")
                        else:
                            stats.errors.append("Failed to save enriched leads to database")
                            logger.error(f"Database save failed for {len(enriched_leads)} enriched leads for lead {lead.id}")
                    else:
                        logger.warning(f"No valid enriched lead objects created for lead {lead.id}")
                        stats.errors.append("No valid enriched lead objects could be created")
            else:
                logger.info(f"No valid emails found for lead {lead.id}")

            # Update processing stats
            stats.processing_time_seconds = time.time() - start_time

            # Update lead status (only if we're not processing an enriched lead)
            if not (hasattr(lead, '_enriched_lead_id') and lead._enriched_lead_id):
                await self.db_client.update_lead_processing_status(
                    lead.id,
                    "completed" if validated_emails else "no_emails_found",
                    {
                        "emails_found": len(validated_emails),
                        "processing_time": stats.processing_time_seconds
                    }
                )

            return stats

        except asyncio.CancelledError:
            logger.info(f"Processing cancelled for lead {lead.id}")
            stats.processing_time_seconds = time.time() - start_time
            raise
        except Exception as e:
            logger.error(f"Failed to process lead {lead.id}: {e}")
            stats.errors.append(f"Processing failed: {str(e)}")
            stats.processing_time_seconds = time.time() - start_time

            # Update lead status to failed (only if we're not processing an enriched lead)
            if not (hasattr(lead, '_enriched_lead_id') and lead._enriched_lead_id):
                try:
                    await self.db_client.update_lead_processing_status(
                        lead.id,
                        "failed",
                        {"error": str(e)}
                    )
                except Exception:
                    pass  # Don't let status update failures crash the process

            return stats

    async def process_leads_batch(self, leads: List[Lead], job_id: str, max_concurrent: Optional[int] = None, batch_size: Optional[int] = None, job_metadata: Optional[dict] = None) -> ProcessingStats:
        """
        Process multiple leads concurrently with job tracking and batching

        Args:
            leads: List of leads to process
            job_id: Associated job ID
            max_concurrent: Maximum concurrent processing tasks
            batch_size: Size of processing batches

        Returns:
            Aggregated processing statistics
        """
        if not leads:
            return ProcessingStats()

        max_concurrent = max_concurrent or self.settings.max_workers
        batch_size = batch_size or self.settings.batch_size

        logger.info(f"Processing batch of {len(leads)} leads for job {job_id} with max {max_concurrent} concurrent tasks")

        semaphore = asyncio.Semaphore(max_concurrent)
        total_stats = ProcessingStats()
        processed_count = 0

        # Process leads in sub-batches to manage memory and provide progress updates
        for i in range(0, len(leads), batch_size):
            await self._check_shutdown()

            batch = leads[i:i + batch_size]
            logger.debug(f"Processing sub-batch {i//batch_size + 1}/{(len(leads) + batch_size - 1)//batch_size} for job {job_id}")

            async def process_with_semaphore_and_progress(lead: Lead) -> ProcessingStats:
                async with semaphore:
                    result = await self.process_single_lead(lead, job_id, job_metadata)

                    # Update job progress
                    nonlocal processed_count
                    processed_count += 1

                    # Update progress every 10 leads or at the end of batch
                    if processed_count % 10 == 0 or processed_count == len(leads):
                        progress = {
                            "processed": processed_count,
                            "total": len(leads),
                            "percentage": round(processed_count / len(leads) * 100, 2)
                        }
                        logger.info(f"Progress update: updating job {job_id} progress (progress: {processed_count}/{len(leads)})")
                        await self.job_manager.update_job_status(
                            job_id,
                            None,  # Don't change status, only update progress
                            progress=progress
                        )

                    return result

            # Process batch concurrently
            tasks = [process_with_semaphore_and_progress(lead) for lead in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Aggregate statistics for this batch
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Lead {batch[j].id} processing failed with exception: {result}")
                    total_stats.errors.append(f"Lead {batch[j].id}: {str(result)}")
                    total_stats.failed_leads += 1
                else:
                    total_stats.leads_processed += 1
                    total_stats.emails_found_hunter += result.emails_found_hunter
                    total_stats.emails_found_perplexity += result.emails_found_perplexity
                    total_stats.emails_validated += result.emails_validated
                    total_stats.emails_failed_validation += result.emails_failed_validation
                    total_stats.processing_time_seconds += result.processing_time_seconds
                    total_stats.api_calls_hunter += result.api_calls_hunter
                    total_stats.api_calls_perplexity += result.api_calls_perplexity
                    total_stats.errors.extend(result.errors)

        logger.info(f"Batch processing complete for job {job_id}: {total_stats.leads_processed} leads processed, "
                   f"{total_stats.emails_validated} emails validated")

        return total_stats

    async def execute_job(self, job: EnrichmentJob) -> ProcessingStats:
        """
        Execute a job with proper locking and progress tracking

        Args:
            job: Job to execute

        Returns:
            Processing statistics
        """
        # First check if job is already running (defensive check)
        current_job = await self.job_manager.get_job(job.job_id)
        if current_job and current_job.status == "running":
            # Check if the job has been running too long (stuck job)
            if current_job.updated_at:
                time_running = (datetime.utcnow() - current_job.updated_at).total_seconds()
                if time_running > 600:  # 10 minutes (matching background service logic)
                    logger.warning(f"Job {job.job_id} has been running for {time_running:.0f} seconds, resetting to pending")
                    await self.job_manager.update_job_status(job.job_id, "pending")
                else:
                    logger.warning(f"Job {job.job_id} is already running, skipping")
                    raise ValueError(f"Job {job.job_id} is already running")
            else:
                logger.warning(f"Job {job.job_id} is already running (no timestamp), skipping")
                raise ValueError(f"Job {job.job_id} is already running")

        # Acquire job lock to prevent concurrent execution
        if not await self.job_manager.acquire_job_lock(job.job_id):
            logger.warning(f"Failed to acquire lock for job {job.job_id}")
            raise ValueError(f"Job {job.job_id} is already locked")

        job_start_time = time.time()  # Track timing for debugging

        try:
            await self._initialize_clients()

            logger.info(f"Starting job {job.job_id}")

            # Fetch enriched leads to process based on job criteria
            try:
                if job.metadata and job.metadata.get("scraping_run_id"):
                    # Handle scraping run jobs - fetch enriched leads without emails
                    scraping_run_id = job.metadata.get("scraping_run_id")
                    lead_domain = job.metadata.get("lead_domain")
                    logger.info(f"Fetching enriched leads without emails for scraping run {scraping_run_id}")
                    if lead_domain:
                        logger.info(f"  Filtering by domain: {lead_domain}")
                    enriched_leads = await self.db_client.fetch_enriched_leads_without_emails_by_scraping_run(
                        scraping_run_id=scraping_run_id,
                        domain_filter=lead_domain
                    )
                    # Convert enriched leads to a format the processor can use
                    leads = self._convert_enriched_leads_to_leads(enriched_leads)
                elif job.process_all:
                    logger.info("Fetching all leads for job processing")
                    leads = await self.db_client.fetch_leads()
                elif job.lead_ids:
                    logger.info(f"Fetching {len(job.lead_ids)} specific leads for job")
                    leads = await self.db_client.fetch_leads(lead_ids=job.lead_ids)
                elif job.domains:
                    logger.info(f"Fetching leads for {len(job.domains)} domains for job")
                    leads = await self.db_client.fetch_leads(domains=job.domains)
                else:
                    raise ValueError("Job has no processing criteria specified")

                if not leads:
                    logger.warning(f"No leads found for job {job.job_id} - marking as completed")
                    logger.info(f"Job {job.job_id} completed (no leads to process)")
                    await self.job_manager.update_job_status(job.job_id, "completed")
                    return ProcessingStats()

                logger.info(f"Job {job.job_id} will process {len(leads)} leads")

            except Exception as e:
                logger.error(f"Failed to fetch leads for job {job.job_id}: {e}")
                await self.job_manager.update_job_status(
                    job.job_id,
                    "failed",
                    error_message=str(e)
                )
                raise

            # Process leads with batching
            try:
                # Update job with total leads count and mark as running when processing actually starts
                time_to_running = time.time() - job_start_time
                logger.info(f"About to set job {job.job_id} to running - leads found: {len(leads)}")
                logger.info(f"Job {job.job_id} ready for processing in {time_to_running:.3f}s, setting status to running")

                await self.job_manager.update_job_status(
                    job.job_id,
                    "running",
                    total_leads=len(leads)
                )

                stats = await self.process_leads_batch(
                    leads,
                    job.job_id,
                    max_concurrent=self.settings.max_workers,  # Use config setting instead of job setting
                    batch_size=job.batch_size,
                    job_metadata=job.metadata
                )

                # Check if processing failed due to API rate limits or other critical errors
                critical_errors = [e for e in stats.errors if 'rate limit' in e.lower() or 'timeout' in e.lower()]
                api_failures = stats.api_calls_hunter + stats.api_calls_perplexity
                success_rate = stats.leads_processed / max(len(leads), 1)

                if critical_errors and len(critical_errors) > len(leads) * 0.5:  # More than 50% critical errors
                    logger.error(f"Job {job.job_id} failed due to excessive API errors ({len(critical_errors)}/{len(leads)})")
                    await self.job_manager.update_job_status(
                        job.job_id,
                        "failed",
                        processed_leads=stats.leads_processed,
                        failed_leads=stats.failed_leads,
                        created_emails=stats.emails_validated,
                        api_calls_hunter=stats.api_calls_hunter,
                        api_calls_perplexity=stats.api_calls_perplexity,
                        processing_time_seconds=stats.processing_time_seconds,
                        error_message=f"Failed due to API rate limits and errors: {len(critical_errors)} critical errors"
                    )
                elif api_failures == 0 and stats.leads_processed == 0:  # No API calls made and nothing processed
                    logger.error(f"Job {job.job_id} failed - no API calls made and no leads processed")
                    await self.job_manager.update_job_status(
                        job.job_id,
                        "failed",
                        error_message="No API calls were made and no leads were processed"
                    )
                elif success_rate < 0.1:  # Less than 10% success rate
                    logger.warning(f"Job {job.job_id} completed with very low success rate ({success_rate:.1%})")
                    await self.job_manager.update_job_status(
                        job.job_id,
                        "completed",
                        processed_leads=stats.leads_processed,
                        failed_leads=stats.failed_leads,
                        created_emails=stats.emails_validated,
                        api_calls_hunter=stats.api_calls_hunter,
                        api_calls_perplexity=stats.api_calls_perplexity,
                        processing_time_seconds=stats.processing_time_seconds,
                        error_message=f"Low success rate: {success_rate:.1%}"
                    )
                else:
                    # Normal completion
                    await self.job_manager.update_job_status(
                        job.job_id,
                        "completed",
                        processed_leads=stats.leads_processed,
                        failed_leads=stats.failed_leads,
                        created_emails=stats.emails_validated,
                        api_calls_hunter=stats.api_calls_hunter,
                        api_calls_perplexity=stats.api_calls_perplexity,
                        processing_time_seconds=stats.processing_time_seconds
                    )

                # Log final statistics
                logger.info("=" * 50)
                logger.info(f"JOB {job.job_id} COMPLETE")
                logger.info(f"Leads processed: {stats.leads_processed}")
                logger.info(f"Emails from Hunter: {stats.emails_found_hunter}")
                logger.info(f"Emails from Perplexity: {stats.emails_found_perplexity}")
                logger.info(f"Emails validated: {stats.emails_validated}")
                logger.info(f"API calls - Hunter: {stats.api_calls_hunter}, Perplexity: {stats.api_calls_perplexity}")
                logger.info(f"Total processing time: {stats.processing_time_seconds:.2f}s")
                logger.info(f"Average time per lead: {stats.processing_time_seconds / max(stats.leads_processed, 1):.2f}s")

                if stats.errors:
                    logger.warning(f"Errors encountered: {len(stats.errors)}")
                    for error in stats.errors[:5]:  # Show first 5 errors
                        logger.warning(f"  - {error}")

                logger.info("=" * 50)

                return stats

            except asyncio.CancelledError:
                logger.info(f"Job {job.job_id} was cancelled")
                await self.job_manager.update_job_status(job.job_id, "cancelled")
                raise
            except Exception as e:
                logger.error(f"Job {job.job_id} failed: {e}")
                await self.job_manager.update_job_status(
                    job.job_id,
                    "failed",
                    error_message=str(e)
                )
                raise

        finally:
            # Always release the job lock and cleanup clients
            self.job_manager.release_job_lock(job.job_id)
            await self._cleanup_clients()

    async def run_background_service(self):
        """Run the service as a background daemon that processes jobs continuously"""
        logger.info("Starting background email enrichment service")
        
        try:
            await self._initialize_clients()
            logger.info("Background service initialized and ready")
            
            # Main service loop
            logger.info("Entering main background service loop")
            loop_count = 0
            while not self._shutdown_event.is_set():
                loop_count += 1
                logger.debug(f"Background service loop iteration #{loop_count}")
                try:
                    # Check for pending jobs and process them
                    logger.debug("Checking for pending jobs...")
                    pending_jobs = await self.job_manager.get_pending_jobs()
                    logger.info(f"Checked for pending jobs: found {len(pending_jobs)}")

                    if pending_jobs:
                        logger.info(f"Found {len(pending_jobs)} pending job(s) to process")

                        for job in pending_jobs:
                            if self._shutdown_event.is_set():
                                logger.info("Shutdown requested during job processing")
                                break

                            try:
                                logger.info(f"Processing job {job.job_id}")
                                await self.execute_job(job)
                                logger.info(f"Completed processing job {job.job_id}")
                            except Exception as e:
                                logger.error(f"Failed to process job {job.job_id}: {e}")
                                # Mark job as failed
                                await self.job_manager.update_job_status(
                                    job.job_id,
                                    "failed",
                                    error_message=str(e)
                                )

                    # Check for stuck jobs (jobs that haven't updated progress in 10+ minutes)
                    # This should run regardless of whether there are pending jobs or not
                    try:
                        stuck_count = await self.db_client.check_stuck_jobs(timeout_minutes=10)
                        if stuck_count > 0:
                            logger.info(f"Reset {stuck_count} stuck jobs back to pending status")
                    except Exception as e:
                        logger.error(f"Error checking for stuck jobs: {e}")

                    # If no pending jobs were found, wait before checking again
                    if not pending_jobs:
                        logger.info(f"No pending jobs found, checking again in {self.settings.job_polling_interval} seconds... (loop #{loop_count})")
                        try:
                            await asyncio.wait_for(
                                self._shutdown_event.wait(),
                                timeout=self.settings.job_polling_interval
                            )
                        except asyncio.TimeoutError:
                            continue  # Normal timeout, continue loop

                except asyncio.CancelledError:
                    logger.info("Background service cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in background service loop: {e}")
                    # Wait before retrying to avoid tight error loops
                    await asyncio.sleep(10)
                    
        except Exception as e:
            logger.error(f"Background service failed to start: {e}")
            self._is_healthy = False
            raise
        finally:
            logger.info("Background service shutting down")
            await self._cleanup_clients()


# Global service instance for health checks
_service_instance: Optional[EmailEnrichmentService] = None


# FastAPI Health Monitoring Application
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan manager for service initialization and cleanup"""
    global _service_instance
    
    # Startup
    logger.info("Starting health monitoring service")
    _service_instance = EmailEnrichmentService()
    
    try:
        await _service_instance._initialize_clients()
        logger.info("Health monitoring service ready")
        yield
    except Exception as e:
        logger.error(f"Failed to initialize health monitoring: {e}")
        raise
    finally:
        # Shutdown
        logger.info("Shutting down health monitoring service")
        if _service_instance:
            await _service_instance._cleanup_clients()


health_app = FastAPI(
    title="Email Enrichment Service Health Monitor",
    description="Health monitoring and metrics for the Email Enrichment Background Service",
    version="1.0.0",
    lifespan=lifespan
)


@health_app.get("/health")
async def health_check():
    """Comprehensive health check endpoint"""
    if not _service_instance:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    health_status = await _service_instance.health_check()
    
    if health_status["status"] == "unhealthy":
        raise HTTPException(status_code=503, detail=health_status)
    
    return health_status


@health_app.get("/health/ready")
async def readiness_check():
    """Kubernetes-style readiness probe"""
    if not _service_instance or not _service_instance._startup_complete:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    return {"status": "ready"}


@health_app.get("/health/live")
async def liveness_check():
    """Kubernetes-style liveness probe"""
    if not _service_instance:
        raise HTTPException(status_code=503, detail="Service not running")
    
    return {"status": "alive"}


@health_app.get("/metrics")
async def get_metrics():
    """Get service metrics and statistics"""
    if not _service_instance:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        # Get database stats
        stats = await _service_instance.db_client.get_processing_stats(verbose=True)

        # Get job queue status
        queue_status = await _service_instance.job_manager.get_job_queue_status()

        return {
            "database_stats": stats,
            "queue_status": queue_status,
            "service_health": await _service_instance.health_check()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")


# HTTP endpoints for API service communication
from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID


class TriggerJobRequest(BaseModel):
    """Request model for triggering jobs via HTTP"""
    lead_ids: Optional[List[str]] = None
    domains: Optional[List[str]] = None
    process_all: bool = False
    skip_existing: bool = True
    max_concurrent: Optional[int] = None
    batch_size: Optional[int] = None
    job_type: str = "enrichment"
    metadata: Optional[dict] = None


class TriggerScrapingJobRequest(BaseModel):
    """Request model for triggering scraping run jobs via HTTP"""
    scraping_run_id: str
    lead_domain: Optional[str] = None
    max_concurrent: Optional[int] = None
    batch_size: Optional[int] = None


class JobTriggerResponse(BaseModel):
    """Response model for job triggering"""
    job_id: str
    status: str
    message: str
    details: dict


@health_app.post("/jobs/trigger", response_model=JobTriggerResponse)
async def trigger_job(request: TriggerJobRequest):
    """Trigger a job processing request from API service"""
    if not _service_instance:
        raise HTTPException(status_code=503, detail="Background service not initialized")

    try:
        # Convert string UUIDs to UUID objects if provided
        lead_uuids = None
        if request.lead_ids:
            try:
                lead_uuids = [UUID(lead_id) for lead_id in request.lead_ids]
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid UUID format in lead_ids: {e}")

        # Create the job
        job = await _service_instance.create_enrichment_job(
            lead_ids=lead_uuids,
            domains=request.domains,
            process_all=request.process_all,
            skip_existing=request.skip_existing,
            max_concurrent=request.max_concurrent,
            batch_size=request.batch_size,
            job_type=request.job_type,
            metadata=request.metadata
        )

        logger.info(f"Job {job.job_id} triggered via HTTP endpoint")

        return JobTriggerResponse(
            job_id=job.job_id,
            status=job.status,
            message="Job triggered successfully and will be processed immediately",
            details={
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "lead_ids": request.lead_ids,
                "domains": request.domains,
                "process_all": request.process_all
            }
        )

    except ValueError as e:
        logger.error(f"Job creation failed: {e}")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to trigger job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger job: {str(e)}")


@health_app.post("/jobs/trigger-scraping", response_model=JobTriggerResponse)
async def trigger_scraping_job(request: TriggerScrapingJobRequest):
    """Trigger a scraping run job processing request from API service"""
    if not _service_instance:
        raise HTTPException(status_code=503, detail="Background service not initialized")

    try:
        # Validate scraping run UUID
        try:
            scraping_uuid = UUID(request.scraping_run_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid scraping_run_id format")

        # Create job with scraping run metadata
        job_metadata = {
            "scraping_run_id": str(scraping_uuid),
            "lead_domain": request.lead_domain
        }

        job = await _service_instance.create_enrichment_job(
            process_all=False,  # We'll specify leads via metadata
            skip_existing=True,
            max_concurrent=request.max_concurrent,
            batch_size=request.batch_size,
            metadata=job_metadata,
            job_type="scraping-run"
        )

        logger.info(f"Scraping run job {job.job_id} triggered via HTTP endpoint for run {scraping_uuid}")

        return JobTriggerResponse(
            job_id=job.job_id,
            status=job.status,
            message="Scraping run job triggered successfully and will be processed immediately",
            details={
                "scraping_run_id": str(scraping_uuid),
                "lead_domain": request.lead_domain,
                "created_at": job.created_at.isoformat() if job.created_at else None
            }
        )

    except ValueError as e:
        logger.error(f"Scraping job creation failed: {e}")
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to trigger scraping job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger scraping job: {str(e)}")

@app.command()
def trigger_scraping_job(
    scraping_run_id: str = typer.Argument(..., help="UUID of the scraping run to process"),
    lead_domain: Optional[str] = typer.Option(None, "--domain", "-d", help="Optional domain filter"),
    max_concurrent: Optional[int] = typer.Option(None, "--max-concurrent", "-c", help="Maximum concurrent processing tasks"),
    batch_size: Optional[int] = typer.Option(None, "--batch-size", "-b", help="Size of processing batches")
):
    """Trigger processing of leads from a specific scraping run - for frontend integration"""
    async def run():
        service = EmailEnrichmentService()
        try:
            # Initialize the service
            await service._initialize_clients()

            # Validate UUID
            try:
                scraping_uuid = UUID(scraping_run_id)
            except ValueError:
                typer.echo(f"❌ Invalid scraping_run_id format: {scraping_run_id}", err=True)
                raise typer.Exit(1)

            # Create the job
            job = await service.create_scraping_run_job(
                scraping_run_id=scraping_uuid,
                lead_domain=lead_domain,
                max_concurrent=max_concurrent,
                batch_size=batch_size
            )

            typer.echo(f"✅ Job created: {job.job_id}")
            typer.echo(f"📊 Status: {job.status}")
            typer.echo(f"🎯 Scraping Run: {scraping_run_id}")
            if lead_domain:
                typer.echo(f"🏢 Domain Filter: {lead_domain}")
            typer.echo(f"🕒 Created: {job.created_at.isoformat()}")

            # Return job info as JSON for frontend parsing
            import json
            job_info = {
                "job_id": job.job_id,
                "status": job.status,
                "scraping_run_id": scraping_run_id,
                "lead_domain": lead_domain,
                "created_at": job.created_at.isoformat()
            }
            typer.echo(f"JSON_OUTPUT: {json.dumps(job_info)}")

        except Exception as e:
            typer.echo(f"❌ Failed to create job: {e}", err=True)
            raise typer.Exit(1)
        finally:
            await service._cleanup_clients()

    asyncio.run(run())


@app.command()
def check_job_status(
    job_id: str = typer.Argument(..., help="Job ID to check status for")
):
    """Check status of a job - for frontend integration"""
    async def run():
        service = EmailEnrichmentService()
        try:
            # Initialize the service
            await service._initialize_clients()

            # Get job status
            job = await service.job_manager.get_job(job_id)
            if not job:
                typer.echo(f"❌ Job not found: {job_id}", err=True)
                raise typer.Exit(1)

            # Display status
            typer.echo(f"📋 Job ID: {job.job_id}")
            typer.echo(f"📊 Status: {job.status}")
            if job.progress:
                progress = job.progress
                typer.echo(f"📈 Progress: {progress.get('processed', 0)}/{progress.get('total', 0)} ({progress.get('percentage', 0)}%)")
            typer.echo(f"📅 Created: {job.created_at.isoformat()}")
            if job.updated_at:
                typer.echo(f"🔄 Updated: {job.updated_at.isoformat()}")
            if job.error_message:
                typer.echo(f"❌ Error: {job.error_message}")
            typer.echo(f"👥 Total Leads: {job.total_leads or 0}")
            typer.echo(f"✅ Processed: {job.processed_leads or 0}")
            typer.echo(f"❌ Failed: {job.failed_leads or 0}")
            typer.echo(f"📧 Emails Created: {job.created_emails or 0}")

            # Return as JSON for frontend
            import json
            status_info = {
                "job_id": job.job_id,
                "status": job.status,
                "progress": job.progress,
                "created_at": job.created_at.isoformat(),
                "updated_at": job.updated_at.isoformat() if job.updated_at else None,
                "error_message": job.error_message,
                "total_leads": job.total_leads,
                "processed_leads": job.processed_leads,
                "failed_leads": job.failed_leads,
                "created_emails": job.created_emails
            }
            typer.echo(f"JSON_OUTPUT: {json.dumps(status_info)}")

        except Exception as e:
            typer.echo(f"❌ Failed to check job status: {e}", err=True)
            raise typer.Exit(1)
        finally:
            await service._cleanup_clients()

    asyncio.run(run())




@app.command()
def serve(
    port: int = typer.Option(8000, "--port", "-p", help="Port for health monitoring server"),
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="Host for health monitoring server"),
    background: bool = typer.Option(True, "--background/--no-background", help="Run background job processor")
):
    """Run the service in production mode with health monitoring"""
    
    async def run_service():
        # Setup production logging
        setup_production_logging()
        
        service = EmailEnrichmentService()
        
        # Start background job processor if requested
        background_task = None
        if background:
            logger.info("Starting background job processor")
            background_task = asyncio.create_task(service.run_background_service())
        
        # Start health monitoring server
        logger.info(f"Starting health monitoring server on {host}:{port}")
        config = uvicorn.Config(
            health_app,
            host=host,
            port=port,
            log_level="info",
            access_log=False  # Reduce noise in production
        )
        server = uvicorn.Server(config)
        
        try:
            if background_task:
                # Run both background service and health server
                await asyncio.gather(
                    server.serve(),
                    background_task,
                    return_exceptions=True
                )
            else:
                # Run only health server
                await server.serve()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            if background_task and not background_task.done():
                background_task.cancel()
                try:
                    await background_task
                except asyncio.CancelledError:
                    pass
            logger.info("Service shutdown complete")
    
    asyncio.run(run_service())


@app.command()
def daemon():
    """Run as a background daemon (job processor only, no health server)"""
    
    async def run_daemon():
        setup_production_logging()

        service = EmailEnrichmentService()
        try:
            await service.run_background_service()
        except KeyboardInterrupt:
            logger.info("Daemon shutdown requested")
        except Exception as e:
            logger.error(f"Daemon failed: {e}")
            logger.info("Daemon will exit due to critical error")
            raise typer.Exit(1)
    
    asyncio.run(run_daemon())


@app.command()
def health_server(
    port: int = typer.Option(8000, "--port", "-p", help="Port for health monitoring server"),
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="Host for health monitoring server")
):
    """Run only the health monitoring server (no background processing)"""
    
    setup_production_logging()
    
    logger.info(f"Starting health monitoring server on {host}:{port}")
    uvicorn.run(
        health_app,
        host=host,
        port=port,
        log_level="info",
        access_log=False
    )


@app.command()
def process_all(
    max_concurrent: Optional[int] = typer.Option(None, "--max-concurrent", "-c", help="Maximum concurrent processing tasks"),
    batch_size: Optional[int] = typer.Option(None, "--batch-size", "-b", help="Size of processing batches"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be processed without actually doing it")
):
    """Create and execute a job to process all leads in the database"""
    async def run():
        service = EmailEnrichmentService()
        try:
            if dry_run:
                typer.echo("DRY RUN: Would create job to process all leads")
                return

            # Create job
            job = await service.create_enrichment_job(
                process_all=True,
                max_concurrent=max_concurrent,
                batch_size=batch_size
            )
            typer.echo(f"Created job {job.job_id} to process all leads")

            # Execute job
            stats = await service.execute_job(job)
            typer.echo(f"Job completed: {stats.leads_processed} leads processed, {stats.emails_validated} emails validated")

        except ValueError as e:
            typer.echo(f"Job creation failed: {e}", err=True)
            raise typer.Exit(1)
        finally:
            await service._cleanup_clients()

    asyncio.run(run())


@app.command()
def process_leads(
    lead_ids: List[str] = typer.Argument(..., help="Lead IDs to process (UUID format)"),
    max_concurrent: Optional[int] = typer.Option(None, "--max-concurrent", "-c", help="Maximum concurrent processing tasks"),
    batch_size: Optional[int] = typer.Option(None, "--batch-size", "-b", help="Size of processing batches"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be processed without actually doing it")
):
    """Create and execute a job to process specific leads by ID"""
    try:
        uuids = [UUID(lead_id) for lead_id in lead_ids]
    except ValueError as e:
        typer.echo(f"Invalid UUID format: {e}", err=True)
        raise typer.Exit(1)

    async def run():
        service = EmailEnrichmentService()
        try:
            if dry_run:
                typer.echo(f"DRY RUN: Would create job to process {len(uuids)} leads")
                return

            # Create job
            job = await service.create_enrichment_job(
                lead_ids=uuids,
                max_concurrent=max_concurrent,
                batch_size=batch_size
            )
            typer.echo(f"Created job {job.job_id} to process {len(uuids)} leads")

            # Execute job
            stats = await service.execute_job(job)
            typer.echo(f"Job completed: {stats.leads_processed} leads processed, {stats.emails_validated} emails validated")

        except ValueError as e:
            typer.echo(f"Job creation failed: {e}", err=True)
            raise typer.Exit(1)
        finally:
            await service._cleanup_clients()

    asyncio.run(run())


@app.command()
def process_domains(
    domains: List[str] = typer.Argument(..., help="Domains to process"),
    max_concurrent: Optional[int] = typer.Option(None, "--max-concurrent", "-c", help="Maximum concurrent processing tasks"),
    batch_size: Optional[int] = typer.Option(None, "--batch-size", "-b", help="Size of processing batches"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be processed without actually doing it")
):
    """Create and execute a job to process leads for specific domains"""
    async def run():
        service = EmailEnrichmentService()
        try:
            if dry_run:
                typer.echo(f"DRY RUN: Would create job to process leads for {len(domains)} domains")
                return

            # Create job
            job = await service.create_enrichment_job(
                domains=domains,
                max_concurrent=max_concurrent,
                batch_size=batch_size
            )
            typer.echo(f"Created job {job.job_id} to process leads for {len(domains)} domains")

            # Execute job
            stats = await service.execute_job(job)
            typer.echo(f"Job completed: {stats.leads_processed} leads processed, {stats.emails_validated} emails validated")

        except ValueError as e:
            typer.echo(f"Job creation failed: {e}", err=True)
            raise typer.Exit(1)
        finally:
            await service._cleanup_clients()

    asyncio.run(run())






@app.command()
def run_job(
    job_id: str = typer.Argument(..., help="Job ID to execute")
):
    """Execute an existing job by ID"""
    async def run():
        service = EmailEnrichmentService()
        job_manager = await get_job_manager()

        try:
            # Get job
            job = await job_manager.get_job(job_id)
            if not job:
                typer.echo(f"Job {job_id} not found", err=True)
                raise typer.Exit(1)

            if job.status in ["running", "completed"]:
                typer.echo(f"Job {job_id} is already {job.status}", err=True)
                raise typer.Exit(1)

            typer.echo(f"Executing job {job_id}...")

            # Execute job
            stats = await service.execute_job(job)
            typer.echo(f"Job completed: {stats.leads_processed} leads processed, {stats.emails_validated} emails validated")

        except ValueError as e:
            typer.echo(f"Job execution failed: {e}", err=True)
            raise typer.Exit(1)
        finally:
            await service._cleanup_clients()

    asyncio.run(run())


@app.command()
def cancel_job(
    job_id: str = typer.Argument(..., help="Job ID to cancel")
):
    """Cancel a running or pending job"""
    async def run():
        job_manager = await get_job_manager()
        try:
            success = await job_manager.cancel_job(job_id)
            if success:
                typer.echo(f"Job {job_id} cancelled successfully")
            else:
                typer.echo(f"Failed to cancel job {job_id}", err=True)
                raise typer.Exit(1)
        finally:
            pass  # Job manager doesn't need explicit cleanup

    asyncio.run(run())


@app.command()
def job_status(
    job_id: Optional[str] = typer.Argument(None, help="Job ID (shows all jobs if not specified)")
):
    """Show job status"""
    async def run():
        job_manager = await get_job_manager()
        try:
            if job_id:
                job = await job_manager.get_job(job_id)
                if not job:
                    typer.echo(f"Job {job_id} not found", err=True)
                    raise typer.Exit(1)

                typer.echo(f"Job {job.job_id}:")
                typer.echo(f"  Status: {job.status}")
                typer.echo(f"  Created: {job.created_at}")
                if job.started_at:
                    typer.echo(f"  Started: {job.started_at}")
                if job.completed_at:
                    typer.echo(f"  Completed: {job.completed_at}")
                typer.echo(f"  Total leads: {job.total_leads}")
                typer.echo(f"  Processed: {job.processed_leads}")
                typer.echo(f"  Failed: {job.failed_leads}")
                typer.echo(f"  Emails created: {job.created_emails}")
                typer.echo(f"  API calls - Hunter: {job.api_calls_hunter}, Perplexity: {job.api_calls_perplexity}")
                typer.echo(f"  Processing time: {job.processing_time_seconds:.2f}s")
                if job.error_message:
                    typer.echo(f"  Error: {job.error_message}")
                if job.progress:
                    typer.echo(f"  Progress: {job.progress}")
            else:
                status = await job_manager.get_job_queue_status()
                typer.echo("Job Queue Status:")
                typer.echo(f"  Total jobs: {status['total_jobs']}")
                for status_name, count in status['status_counts'].items():
                    typer.echo(f"  {status_name.capitalize()}: {count}")

                if status['recent_jobs']:
                    typer.echo("\nRecent Jobs:")
                    for job in status['recent_jobs'][:5]:  # Show last 5
                        typer.echo(f"  {job.job_id}: {job.status} ({job.created_at}) - {job.processed_leads}/{job.total_leads} leads")

        finally:
            pass  # Job manager doesn't need explicit cleanup

    asyncio.run(run())


@app.command()
def stats():
    """Show database statistics"""
    async def run():
        db_client = await get_db_client()
        try:
            stats = await db_client.get_processing_stats(verbose=True)
            typer.echo("Database Statistics:")
            typer.echo(f"  Total leads: {stats['total_leads']}")
            typer.echo(f"  Total enriched leads: {stats['total_enriched_leads']}")
            typer.echo(f"  Hunter direct enriched: {stats['hunter_direct_enriched']}")
            typer.echo(f"  Perplexity generated enriched: {stats['perplexity_generated_enriched']}")
            typer.echo(f"  Google Maps enriched: {stats['google_maps_enriched']}")
            typer.echo(f"  Average confidence score: {stats['average_confidence_score']}%")
            typer.echo(f"  Enriched leads per lead: {stats['enriched_leads_per_lead']}")
        finally:
            await db_client.close()

    asyncio.run(run())


@app.command()
def get_enriched_leads(
    enriched_lead_id: Optional[str] = typer.Option(None, "--id", help="Enriched lead ID (UUID)"),
    lead_id: Optional[str] = typer.Option(None, "--lead-id", help="Lead ID (UUID)"),
    email: Optional[str] = typer.Option(None, "--email", help="Email address"),
    domain: Optional[str] = typer.Option(None, "--domain", help="Domain"),
    source: Optional[str] = typer.Option(None, "--source", help="Enrichment source (hunter_direct, perplexity_generated, google_maps)"),
    limit: Optional[int] = typer.Option(10, "--limit", "-l", help="Maximum number of results to return"),
    offset: Optional[int] = typer.Option(0, "--offset", "-o", help="Number of results to skip"),
    json_output: bool = typer.Option(False, "--json", help="Output in JSON format")
):
    """Retrieve enriched leads from the database"""
    try:
        # Validate UUIDs if provided
        enriched_lead_uuid = None
        lead_uuid = None

        if enriched_lead_id:
            try:
                enriched_lead_uuid = UUID(enriched_lead_id)
            except ValueError:
                typer.echo(f"Invalid enriched lead ID format: {enriched_lead_id}", err=True)
                raise typer.Exit(1)

        if lead_id:
            try:
                lead_uuid = UUID(lead_id)
            except ValueError:
                typer.echo(f"Invalid lead ID format: {lead_id}", err=True)
                raise typer.Exit(1)

    except ValueError as e:
        typer.echo(f"UUID validation error: {e}", err=True)
        raise typer.Exit(1)

    async def run():
        db_client = await get_db_client()
        try:
            enriched_leads = await db_client.fetch_enriched_leads(
                enriched_lead_id=enriched_lead_uuid,
                lead_id=lead_uuid,
                email=email,
                domain=domain,
                enrichment_source=source,
                limit=limit,
                offset=offset
            )

            if not enriched_leads:
                typer.echo("No enriched leads found matching the criteria")
                return

            if json_output:
                import json
                leads_data = []
                for lead in enriched_leads:
                    lead_dict = lead.dict()
                    # Convert UUIDs to strings for JSON serialization
                    if lead_dict.get("id"):
                        lead_dict["id"] = str(lead_dict["id"])
                    if lead_dict.get("lead_id"):
                        lead_dict["lead_id"] = str(lead_dict["lead_id"])
                    if lead_dict.get("scraping_run_id"):
                        lead_dict["scraping_run_id"] = str(lead_dict["scraping_run_id"])
                    leads_data.append(lead_dict)
                typer.echo(json.dumps(leads_data, indent=2, default=str))
            else:
                typer.echo(f"Found {len(enriched_leads)} enriched lead(s):")
                typer.echo("-" * 80)

                for i, lead in enumerate(enriched_leads, 1):
                    typer.echo(f"Enriched Lead #{i}")
                    typer.echo(f"  ID: {lead.id}")
                    typer.echo(f"  Lead ID: {lead.lead_id}")
                    typer.echo(f"  Email: {lead.email}")
                    typer.echo(f"  Name: {lead.first_name or 'N/A'} {lead.last_name or ''}".strip())
                    typer.echo(f"  Job Title: {lead.job_title or 'N/A'}")
                    typer.echo(f"  Company: {lead.company_name or 'N/A'}")
                    typer.echo(f"  Website: {lead.company_website or 'N/A'}")
                    typer.echo(f"  Location: {lead.city or 'N/A'}, {lead.state or 'N/A'}, {lead.country or 'N/A'}")
                    typer.echo(f"  Phone: {lead.phone_number or 'N/A'}")
                    typer.echo(f"  LinkedIn Person: {lead.linkedin_person_url or 'N/A'}")
                    typer.echo(f"  LinkedIn Company: {lead.linkedin_company_url or 'N/A'}")
                    typer.echo(f"  Business Category: {lead.business_category or 'N/A'}")
                    typer.echo(f"  Business Rating: {lead.business_rating or 'N/A'}")
                    typer.echo(f"  Confidence: {lead.confidence or 'N/A'}%")
                    typer.echo(f"  Verification Status: {lead.verification_status or 'N/A'}")
                    typer.echo(f"  Email Disposable: {lead.email_disposable or 'N/A'}")
                    typer.echo(f"  Email Webmail: {lead.email_webmail or 'N/A'}")
                    typer.echo(f"  Enrichment Source: {lead.enrichment_source}")
                    typer.echo(f"  Enrichment Status: {lead.enrichment_status}")
                    typer.echo(f"  Processed At: {lead.processed_at}")
                    typer.echo(f"  Processed By Job: {lead.processed_by_job_id or 'N/A'}")
                    if lead.enrichment_reason:
                        typer.echo(f"  Enrichment Reason: {lead.enrichment_reason}")
                    typer.echo("-" * 80)

        finally:
            await db_client.close()

    asyncio.run(run())


@app.command()
def process_scraping_run(
    scraping_run_id: str = typer.Argument(..., help="Scraping run ID to process"),
    lead_domain: Optional[str] = typer.Option(None, "--domain", "-d", help="Specific domain to process (optional - processes all domains in run if not specified)"),
    max_concurrent: Optional[int] = typer.Option(None, "--max-concurrent", "-c", help="Maximum concurrent processing tasks"),
    batch_size: Optional[int] = typer.Option(None, "--batch-size", "-b", help="Size of processing batches"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be processed without actually doing it")
):
    """Create and execute a job to process leads from a specific scraping run"""
    try:
        run_uuid = UUID(scraping_run_id)
    except ValueError as e:
        typer.echo(f"Invalid scraping run ID format: {e}", err=True)
        raise typer.Exit(1)

    # Validate domain format if provided
    if lead_domain and not lead_domain.strip():
        typer.echo("Domain cannot be empty", err=True)
        raise typer.Exit(1)

    async def run():
        service = EmailEnrichmentService()
        try:
            if dry_run:
                typer.echo(f"DRY RUN: Would process leads from scraping run {scraping_run_id}")
                if lead_domain:
                    typer.echo(f"  Filtering by domain: {lead_domain}")
                return

            # Create job with scraping run parameters
            job = await service.create_scraping_run_job(
                scraping_run_id=run_uuid,
                lead_domain=lead_domain,
                max_concurrent=max_concurrent,
                batch_size=batch_size
            )
            typer.echo(f"Created job {job.job_id} to process scraping run {scraping_run_id}")
            if lead_domain:
                typer.echo(f"  Filtering by domain: {lead_domain}")

            # Execute job
            stats = await service.execute_job(job)
            typer.echo(f"Job completed: {stats.leads_processed} leads processed, {stats.emails_validated} emails validated")

        except ValueError as e:
            typer.echo(f"Job creation failed: {e}", err=True)
            raise typer.Exit(1)
        finally:
            await service._cleanup_clients()

    asyncio.run(run())


def setup_production_logging():
    """Setup optimized logging for production background service"""
    settings = get_settings()
    logger.remove()  # Remove default handler
    
    # Production log format (more compact, structured)
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan> | "
        "<level>{message}</level>"
    )
    
    # Add console handler for production
    logger.add(
        sys.stdout,
        level=settings.log_level,
        format=log_format,
        colorize=True,
        backtrace=False,  # Reduce noise in production
        diagnose=False    # Don't show variable values in production
    )
    
    # Add file handler if enabled and not in debug mode
    if settings.log_file_enabled and not settings.debug_mode:
        import os
        os.makedirs(settings.log_file_path, exist_ok=True)
        
        logger.add(
            f"{settings.log_file_path}/service.log",
            level=settings.log_level,
            format=log_format,
            rotation=settings.log_rotation,
            retention=settings.log_retention,
            compression="gz",
            colorize=False
        )
    
    # Add error file handler
    if settings.log_file_enabled:
        import os
        os.makedirs(settings.log_file_path, exist_ok=True)
        
        logger.add(
            f"{settings.log_file_path}/errors.log",
            level="ERROR",
            format=log_format,
            rotation=settings.log_rotation,
            retention="90 days",
            compression="gz",
            colorize=False
        )
    
    logger.info(f"Production logging configured (level: {settings.log_level})")


if __name__ == "__main__":
    # Setup basic logging for CLI commands
    settings = get_settings()
    logger.remove()  # Remove default handler
    
    # CLI log format (more detailed for debugging)
    cli_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )
    
    logger.add(
        lambda msg: print(msg, end=""),
        level=settings.log_level,
        format=cli_format,
        colorize=True
    )

    app()