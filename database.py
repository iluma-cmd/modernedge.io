"""
Supabase database client and operations
"""
import asyncio
import json
import re
import time
from datetime import datetime
from typing import List, Optional, TYPE_CHECKING
from uuid import UUID

from loguru import logger
from supabase import Client, create_client
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import get_settings
from models import Lead, ValidatedEmail, EnrichedLead, EnrichmentJob

if TYPE_CHECKING:
    pass  # EnrichmentJob already imported above


class CircuitBreaker:
    """Simple circuit breaker to prevent cascading failures"""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half-open

    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit breaker"""
        return (self.state == "open" and
                time.time() - self.last_failure_time >= self.recovery_timeout)

    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == "open":
            if self._should_attempt_reset():
                self.state = "half-open"
                logger.warning("Circuit breaker transitioning to half-open state")
            else:
                raise Exception("Circuit breaker is open - database operations temporarily disabled")

        try:
            result = await func(*args, **kwargs)
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
                logger.info("Circuit breaker reset to closed state")
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "open"
                logger.error(f"Circuit breaker opened after {self.failure_count} failures")

            raise e


class DatabaseClient:
    """Supabase database client with connection pooling and error handling"""

    def __init__(self):
        self.settings = get_settings()
        self._client: Optional[Client] = None
        self._connection_lock = asyncio.Lock()

        # Circuit breakers for different operation types
        self._write_circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
        self._read_circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

    async def get_client(self) -> Client:
        """Get or create Supabase client with connection pooling"""
        if self._client is None:
            async with self._connection_lock:
                if self._client is None:
                    try:
                        self._client = create_client(
                            supabase_url=self.settings.supabase_url,
                            supabase_key=self.settings.supabase_service_role_key,
                        )
                        logger.info("Supabase client initialized successfully")
                    except Exception as e:
                        logger.error(f"Failed to initialize Supabase client: {e}")
                        raise
        return self._client

    async def fetch_leads(
        self,
        lead_ids: Optional[List[UUID]] = None,
        domains: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Lead]:
        """
        Fetch leads from the database with optional filtering

        Args:
            lead_ids: Specific lead IDs to fetch
            domains: Specific domains to fetch leads for
            limit: Maximum number of leads to fetch
            offset: Number of leads to skip

        Returns:
            List of Lead objects
        """
        try:
            client = await self.get_client()
            query = client.table("leads").select("*")

            # Apply filters
            if lead_ids:
                query = query.in_("id", [str(lead_id) for lead_id in lead_ids])

            if domains:
                query = query.in_("domain", domains)

            # Apply pagination
            if limit:
                query = query.limit(limit)
            if offset:
                query = query.range(offset, offset + (limit or 1000) - 1)

            # Execute query (run synchronous operation in thread pool)
            response = await asyncio.to_thread(query.execute)

            if response.data:
                leads = [Lead(**lead_data) for lead_data in response.data]
                logger.info(f"Fetched {len(leads)} leads from database")
                return leads
            else:
                logger.warning("No leads found matching criteria")
                return []

        except Exception as e:
            logger.error(f"Failed to fetch leads: {e}")
            raise


    async def fetch_leads_by_scraping_run(
        self,
        scraping_run_id: str,
        domain_filter: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Lead]:
        """
        Fetch leads that were discovered in a specific scraping run

        This method queries the raw_sources table to find lead domains,
        then fetches the corresponding leads from the leads table.

        Args:
            scraping_run_id: UUID of the scraping run
            domain_filter: Optional specific domain to filter by
            limit: Maximum number of leads to fetch
            offset: Number of leads to skip

        Returns:
            List of Lead objects from the scraping run
        """
        try:
            client = await self.get_client()

            # First, get domains from raw_sources table for this scraping run
            raw_sources_query = client.table("raw_sources").select("lead_domain")

            # Filter by scraping run ID
            raw_sources_query = raw_sources_query.eq("scraping_run_id", scraping_run_id)

            # Apply domain filter if specified
            if domain_filter:
                raw_sources_query = raw_sources_query.eq("lead_domain", domain_filter)

            # Remove duplicates and get unique domains
            raw_sources_query = raw_sources_query.limit(1000)  # Reasonable limit for domains

            raw_sources_response = await asyncio.to_thread(raw_sources_query.execute)

            if not raw_sources_response.data:
                logger.warning(f"No raw sources found for scraping run {scraping_run_id}")
                return []

            # Extract unique domains
            domains = list(set(item["lead_domain"] for item in raw_sources_response.data if item.get("lead_domain")))
            logger.info(f"Found {len(domains)} unique domains in scraping run {scraping_run_id}")

            if not domains:
                logger.warning("No domains found in raw sources")
                return []

            # Now fetch leads for these domains
            leads_query = client.table("leads").select("*")

            # Filter by domains from scraping run
            leads_query = leads_query.in_("domain", domains)

            # Apply pagination
            if limit:
                leads_query = leads_query.limit(limit)
            if offset:
                leads_query = leads_query.range(offset, offset + (limit or 1000) - 1)

            # Execute query
            leads_response = await asyncio.to_thread(leads_query.execute)

            if leads_response.data:
                leads = [Lead(**lead_data) for lead_data in leads_response.data]
                logger.info(f"Fetched {len(leads)} leads from scraping run {scraping_run_id}")
                return leads
            else:
                logger.warning(f"No leads found for domains from scraping run {scraping_run_id}")
                return []

        except Exception as e:
            logger.error(f"Failed to fetch leads by scraping run: {e}")
            raise

    async def fetch_enriched_leads_without_emails_by_scraping_run(
        self,
        scraping_run_id: str,
        domain_filter: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List["EnrichedLead"]:
        """
        Fetch enriched leads without email addresses from a specific scraping run
        
        Args:
            scraping_run_id: UUID of the scraping run
            domain_filter: Optional specific domain to filter by
            limit: Maximum number of leads to fetch
            offset: Number of leads to skip
            
        Returns:
            List of EnrichedLead objects without email addresses
        """
        try:
            client = await self.get_client()
            
            # Query enriched_leads where email is NULL and scraping_run_id matches
            query = client.table("enriched_leads").select("*")
            query = query.eq("scraping_run_id", scraping_run_id)
            query = query.is_("email", "null")  # Filter for NULL email addresses
            
            if domain_filter:
                # Extract domain from company_website or use company_name
                query = query.or_(
                    f"company_website.ilike.%{domain_filter}%,"
                    f"company_name.ilike.%{domain_filter}%"
                )
            
            if limit:
                query = query.limit(limit)
            if offset:
                query = query.offset(offset)
                
            response = await asyncio.to_thread(query.execute)
            
            enriched_leads = []
            if response.data:
                for lead_data in response.data:
                    # Convert UUID strings back to UUID objects
                    if lead_data.get("id"):
                        lead_data["id"] = UUID(lead_data["id"])
                    if lead_data.get("lead_id"):
                        lead_data["lead_id"] = UUID(lead_data["lead_id"])
                    if lead_data.get("scraping_run_id"):
                        lead_data["scraping_run_id"] = UUID(lead_data["scraping_run_id"])
                    if lead_data.get("processed_by_job_id"):
                        lead_data["processed_by_job_id"] = UUID(lead_data["processed_by_job_id"])
                    
                    enriched_leads.append(EnrichedLead(**lead_data))
                    
                logger.info(f"Found {len(enriched_leads)} enriched leads without emails for scraping run {scraping_run_id}")
            else:
                logger.warning(f"No enriched leads without emails found for scraping run {scraping_run_id}")
                
            return enriched_leads
            
        except Exception as e:
            logger.error(f"Failed to fetch enriched leads without emails for scraping run {scraping_run_id}: {e}")
            return []

    async def save_leads(self, leads: List[Lead], batch_size: int = 100) -> bool:
        """
        Save leads to the database in batches

        Args:
            leads: List of Lead objects to save
            batch_size: Number of leads to save per batch

        Returns:
            True if successful, False otherwise
        """
        if not leads:
            logger.warning("No leads provided to save")
            return True

        logger.info(f"Saving {len(leads)} leads in batches of {batch_size}")

        # Process leads in batches
        for i in range(0, len(leads), batch_size):
            batch = leads[i:i + batch_size]
            await self._save_leads_batch(batch)

        logger.info(f"Successfully saved all {len(leads)} leads")
        return True

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def _save_leads_batch(self, leads: List[Lead]) -> None:
        """
        Save a batch of leads to the database

        Args:
            leads: List of Lead objects to save
        """
        await self._write_circuit_breaker.call(self._save_leads_batch_impl, leads)

    async def _save_leads_batch_impl(self, leads: List[Lead]) -> None:
        """
        Implementation of saving a batch of leads (called through circuit breaker)

        Args:
            leads: List of Lead objects to save
        """
        client = await self.get_client()

        # Convert to dict format for Supabase
        leads_data = []
        for lead in leads:
            lead_dict = lead.dict()
            # Remove None values for optional fields that should be generated by DB
            if lead_dict.get("id") is None:
                lead_dict.pop("id", None)
            if lead_dict.get("created_at") is None:
                lead_dict.pop("created_at", None)
            if lead_dict.get("updated_at") is None:
                lead_dict.pop("updated_at", None)
            leads_data.append(lead_dict)

        # Insert leads (this will handle ID generation and timestamps)
        # Use DO NOTHING on conflict to avoid updating existing records
        # Run synchronous Supabase operation in thread pool to avoid blocking
        response = await asyncio.to_thread(
            client.table("leads").insert,
            leads_data,
            on_conflict="DO NOTHING"  # Skip if domain already exists
        )
        response = await asyncio.to_thread(response.execute)

        logger.debug(f"Successfully saved batch of {len(leads)} leads")

    async def save_validated_emails(self, emails: List[ValidatedEmail], batch_size: int = 100) -> bool:
        """
        Save validated emails to the database in batches

        Args:
            emails: List of validated email objects to save
            batch_size: Number of emails to save per batch

        Returns:
            True if successful, False otherwise
        """
        if not emails:
            logger.warning("No emails provided to save")
            return True

        logger.info(f"Saving {len(emails)} validated emails in batches of {batch_size}")

        # Process emails in batches
        for i in range(0, len(emails), batch_size):
            batch = emails[i:i + batch_size]
            await self._save_validated_emails_batch(batch)

        logger.info(f"Successfully saved all {len(emails)} validated emails")
        return True

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def _save_validated_emails_batch(self, emails: List[ValidatedEmail]) -> None:
        """
        Save a batch of validated emails to the database

        Args:
            emails: List of validated email objects to save
        """
        await self._write_circuit_breaker.call(self._save_validated_emails_batch_impl, emails)

    async def _save_validated_emails_batch_impl(self, emails: List[ValidatedEmail]) -> None:
        """
        Implementation of saving a batch of validated emails (called through circuit breaker)

        Args:
            emails: List of validated email objects to save
        """
        client = await self.get_client()

        # Convert to dict format for Supabase
        email_data = []
        for email in emails:
            email_dict = email.dict()
            # Convert lead_id to string if present
            if email_dict.get("lead_id"):
                email_dict["lead_id"] = str(email_dict["lead_id"])
            # Convert datetime objects to ISO format strings for JSON serialization
            for key, value in email_dict.items():
                if hasattr(value, 'isoformat'):  # Check if it's a datetime object
                    email_dict[key] = value.isoformat()
            email_data.append(email_dict)

        # Insert emails (this will handle conflicts via unique constraint)
        # Run synchronous Supabase operation in thread pool to avoid blocking
        response = await asyncio.to_thread(
            client.table("emails").upsert,
            email_data,
            on_conflict="lead_id,email"
        )
        response = await asyncio.to_thread(response.execute)

        logger.debug(f"Successfully saved batch of {len(emails)} validated emails")

    async def save_enriched_leads(self, enriched_leads: List[EnrichedLead], batch_size: int = 100) -> bool:
        """
        Save enriched leads to the database in batches

        Args:
            enriched_leads: List of enriched lead objects to save
            batch_size: Number of enriched leads to save per batch

        Returns:
            True if successful, False otherwise
        """
        if not enriched_leads:
            logger.warning("No enriched leads provided to save")
            return True

        logger.info(f"Saving {len(enriched_leads)} enriched leads in batches of {batch_size}")

        # Process enriched leads in batches
        for i in range(0, len(enriched_leads), batch_size):
            batch = enriched_leads[i:i + batch_size]
            await self._save_enriched_leads_batch(batch)

        logger.info(f"Successfully saved all {len(enriched_leads)} enriched leads")
        return True

    async def update_enriched_lead_with_email(
        self,
        enriched_lead_id: UUID,
        email: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        job_title: Optional[str] = None,
        linkedin_person_url: Optional[str] = None,
        verification_status: Optional[str] = None,
        verification_score: Optional[int] = None,
        email_disposable: Optional[bool] = None,
        email_webmail: Optional[bool] = None,
        enrichment_source: Optional[str] = None,
        confidence: Optional[int] = None,
        processed_by_job_id: Optional[str] = None
    ) -> bool:
        """
        Update an existing enriched lead with email and related information
        
        Args:
            enriched_lead_id: ID of the enriched lead to update
            email: Email address to add
            first_name: First name from email validation
            last_name: Last name from email validation
            job_title: Job title from email validation
            linkedin_person_url: LinkedIn profile URL
            verification_status: Email verification status
            verification_score: Email verification score
            email_disposable: Whether email is disposable
            email_webmail: Whether email is webmail
            enrichment_source: Source of the enrichment
            confidence: Confidence score
            processed_by_job_id: Job ID that processed this
            
        Returns:
            True if update was successful
        """
        try:
            # Validate enriched_lead_id
            if not enriched_lead_id:
                logger.error("No enriched_lead_id provided for update")
                return False

            enriched_lead_id_str = str(enriched_lead_id)
            if not enriched_lead_id_str or len(enriched_lead_id_str) != 36:
                logger.error(f"Invalid enriched_lead_id format: {enriched_lead_id_str}")
                return False

            client = await self.get_client()

            # Prepare update data
            update_data = {
                "email": email,
                "updated_at": datetime.utcnow().isoformat()
            }
            
            # Add optional fields if provided
            if first_name is not None:
                update_data["first_name"] = first_name
            if last_name is not None:
                update_data["last_name"] = last_name
            if job_title is not None:
                update_data["job_title"] = job_title
            if linkedin_person_url is not None:
                update_data["linkedin_person_url"] = linkedin_person_url
            if verification_status is not None:
                update_data["verification_status"] = verification_status
            if verification_score is not None:
                update_data["verification_score"] = verification_score
            if email_disposable is not None:
                update_data["email_disposable"] = email_disposable
            if email_webmail is not None:
                update_data["email_webmail"] = email_webmail
            if enrichment_source is not None:
                update_data["enrichment_source"] = enrichment_source
            if confidence is not None:
                update_data["confidence"] = confidence
            if processed_by_job_id is not None:
                # Extract UUID from job_id string if it contains one
                job_id_str = str(processed_by_job_id).strip()
                
                # Try to extract UUID from job_id (format: bulk-UUID-timestamp or just UUID)
                uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
                uuid_match = re.search(uuid_pattern, job_id_str, re.IGNORECASE)
                
                if uuid_match:
                    # Use the extracted UUID
                    extracted_uuid = uuid_match.group(0)
                    update_data["processed_by_job_id"] = extracted_uuid
                    logger.debug(f"Extracted UUID {extracted_uuid} from job_id {job_id_str}")
                elif len(job_id_str) == 36:
                    # Already a valid UUID format
                    update_data["processed_by_job_id"] = job_id_str
                else:
                    # Store as null if we can't extract a valid UUID
                    logger.warning(f"Could not extract valid UUID from job_id: {job_id_str}, storing as null")
                    update_data["processed_by_job_id"] = None
            
            # Update the enriched lead
            response = await asyncio.to_thread(
                client.table("enriched_leads").update(update_data).eq("id", str(enriched_lead_id)).execute
            )
            
            if response.data:
                logger.info(f"Updated enriched lead {enriched_lead_id} with email {email}")
                return True
            else:
                logger.error(f"No enriched lead found with ID {enriched_lead_id}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to update enriched lead {enriched_lead_id} with email: {e}")
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def _save_enriched_leads_batch(self, enriched_leads: List[EnrichedLead]) -> None:
        """
        Save a batch of enriched leads to the database

        Args:
            enriched_leads: List of enriched lead objects to save
        """
        await self._write_circuit_breaker.call(self._save_enriched_leads_batch_impl, enriched_leads)

    async def _save_enriched_leads_batch_impl(self, enriched_leads: List[EnrichedLead]) -> None:
        """
        Implementation of saving a batch of enriched leads (called through circuit breaker)
        Only updates selected columns: first_name, last_name, email, job_title, linkedin_person_url

        Args:
            enriched_leads: List of enriched lead objects to save
        """
        client = await self.get_client()

        for enriched_lead in enriched_leads:
            # Validate required fields - email is now optional if we have person data
            if not enriched_lead.lead_id or not enriched_lead.enrichment_source:
                logger.warning(f"Skipping enriched lead with missing required fields: lead_id={enriched_lead.lead_id}, email={enriched_lead.email}, source={enriched_lead.enrichment_source}")
                continue

            # If no email but we have person data, that's still valuable
            if not enriched_lead.email and not (enriched_lead.first_name or enriched_lead.last_name or enriched_lead.job_title):
                logger.warning(f"Skipping enriched lead with no email and no person data: lead_id={enriched_lead.lead_id}, source={enriched_lead.enrichment_source}")
                continue

            # Check if enriched lead already exists for this lead_id (regardless of source)
            # We'll merge data from different sources into a single comprehensive record
            existing_leads = await asyncio.to_thread(
                client.table("enriched_leads")
                .select("*")
                .eq("lead_id", str(enriched_lead.lead_id))
                .eq("enrichment_status", "active")
                .execute
            )

            # If we have existing enriched leads, merge data; otherwise prepare for insert
            if existing_leads.data and len(existing_leads.data) > 0:
                # Get the first (and should be only) existing enriched lead
                existing_lead = existing_leads.data[0]

                # Merge data: keep existing values, update with new non-null values
                update_data = {}

                # Merge basic info
                if enriched_lead.first_name is not None or existing_lead.get("first_name"):
                    update_data["first_name"] = enriched_lead.first_name or existing_lead.get("first_name")
                if enriched_lead.last_name is not None or existing_lead.get("last_name"):
                    update_data["last_name"] = enriched_lead.last_name or existing_lead.get("last_name")
                if enriched_lead.email or existing_lead.get("email"):
                    update_data["email"] = enriched_lead.email or existing_lead.get("email")
                if enriched_lead.job_title is not None or existing_lead.get("job_title"):
                    update_data["job_title"] = enriched_lead.job_title or existing_lead.get("job_title")
                if enriched_lead.scraping_run_id or existing_lead.get("scraping_run_id"):
                    update_data["scraping_run_id"] = str(enriched_lead.scraping_run_id or existing_lead.get("scraping_run_id"))

                # Merge company info
                if enriched_lead.company_name or existing_lead.get("company_name"):
                    update_data["company_name"] = enriched_lead.company_name or existing_lead.get("company_name")
                if enriched_lead.company_website or existing_lead.get("company_website"):
                    update_data["company_website"] = enriched_lead.company_website or existing_lead.get("company_website")

                # Merge contact info
                if enriched_lead.phone_number or existing_lead.get("phone_number"):
                    update_data["phone_number"] = enriched_lead.phone_number or existing_lead.get("phone_number")
                if enriched_lead.linkedin_person_url or existing_lead.get("linkedin_person_url"):
                    update_data["linkedin_person_url"] = enriched_lead.linkedin_person_url or existing_lead.get("linkedin_person_url")
                if enriched_lead.linkedin_company_url or existing_lead.get("linkedin_company_url"):
                    update_data["linkedin_company_url"] = enriched_lead.linkedin_company_url or existing_lead.get("linkedin_company_url")

                # Merge location info
                if enriched_lead.city or existing_lead.get("city"):
                    update_data["city"] = enriched_lead.city or existing_lead.get("city")
                if enriched_lead.state or existing_lead.get("state"):
                    update_data["state"] = enriched_lead.state or existing_lead.get("state")
                if enriched_lead.country or existing_lead.get("country"):
                    update_data["country"] = enriched_lead.country or existing_lead.get("country")
                if enriched_lead.address_line or existing_lead.get("address_line"):
                    update_data["address_line"] = enriched_lead.address_line or existing_lead.get("address_line")

                # Merge business info
                if enriched_lead.business_category or existing_lead.get("business_category"):
                    update_data["business_category"] = enriched_lead.business_category or existing_lead.get("business_category")
                if enriched_lead.business_rating or existing_lead.get("business_rating"):
                    update_data["business_rating"] = enriched_lead.business_rating or existing_lead.get("business_rating")
                if enriched_lead.business_reviews or existing_lead.get("business_reviews"):
                    update_data["business_reviews"] = enriched_lead.business_reviews or existing_lead.get("business_reviews")

                # Update confidence and verification if new data is better
                if enriched_lead.confidence is not None:
                    if existing_lead.get("confidence") is None or enriched_lead.confidence > existing_lead.get("confidence", 0):
                        update_data["confidence"] = enriched_lead.confidence
                        update_data["verification_status"] = enriched_lead.verification_status
                        update_data["verification_score"] = enriched_lead.verification_score

                # Update enrichment source to reflect multiple sources (comma-separated)
                existing_sources = existing_lead.get("enrichment_source", "").split(",")
                if enriched_lead.enrichment_source not in existing_sources:
                    existing_sources.append(enriched_lead.enrichment_source)
                    update_data["enrichment_source"] = ",".join(existing_sources)

                # Update reason to reflect latest enrichment
                if enriched_lead.enrichment_reason:
                    update_data["enrichment_reason"] = enriched_lead.enrichment_reason

                # Add processed timestamp
                update_data["processed_at"] = "now()"
                update_data["updated_at"] = "now()"

            else:
                # No existing enriched lead, prepare data for new insert
                update_data = {}
                if enriched_lead.first_name is not None:
                    update_data["first_name"] = enriched_lead.first_name
                if enriched_lead.last_name is not None:
                    update_data["last_name"] = enriched_lead.last_name
                if enriched_lead.email:
                    update_data["email"] = enriched_lead.email
                if enriched_lead.job_title is not None:
                    update_data["job_title"] = enriched_lead.job_title
                if enriched_lead.scraping_run_id:
                    update_data["scraping_run_id"] = str(enriched_lead.scraping_run_id)
                if enriched_lead.company_name:
                    update_data["company_name"] = enriched_lead.company_name
                if enriched_lead.company_website:
                    update_data["company_website"] = enriched_lead.company_website
                if enriched_lead.phone_number:
                    update_data["phone_number"] = enriched_lead.phone_number
                if enriched_lead.linkedin_person_url:
                    update_data["linkedin_person_url"] = enriched_lead.linkedin_person_url
                if enriched_lead.linkedin_company_url:
                    update_data["linkedin_company_url"] = enriched_lead.linkedin_company_url
                if enriched_lead.city:
                    update_data["city"] = enriched_lead.city
                if enriched_lead.state:
                    update_data["state"] = enriched_lead.state
                if enriched_lead.country:
                    update_data["country"] = enriched_lead.country
                if enriched_lead.address_line:
                    update_data["address_line"] = enriched_lead.address_line
                if enriched_lead.business_category:
                    update_data["business_category"] = enriched_lead.business_category
                if enriched_lead.business_rating:
                    update_data["business_rating"] = enriched_lead.business_rating
                if enriched_lead.business_reviews:
                    update_data["business_reviews"] = enriched_lead.business_reviews
                if enriched_lead.confidence is not None:
                    update_data["confidence"] = enriched_lead.confidence
                if enriched_lead.verification_status:
                    update_data["verification_status"] = enriched_lead.verification_status
                if enriched_lead.verification_score is not None:
                    update_data["verification_score"] = enriched_lead.verification_score
                if enriched_lead.enrichment_reason:
                    update_data["enrichment_reason"] = enriched_lead.enrichment_reason

                # Add processed timestamp
                update_data["processed_at"] = "now()"
                update_data["updated_at"] = "now()"

            if existing_leads.data and len(existing_leads.data) > 0:
                # Update existing record with selected columns only
                existing_id = existing_leads.data[0]["id"]
                response = await asyncio.to_thread(
                    client.table("enriched_leads")
                    .update(update_data)
                    .eq("id", existing_id)
                    .execute
                )
                logger.debug(f"Updated existing enriched lead {existing_id} with selected columns")
            else:
                # Insert new record with merged data plus required fields
                insert_data = {
                    "lead_id": str(enriched_lead.lead_id),
                    "enrichment_source": enriched_lead.enrichment_source,
                    "enrichment_status": enriched_lead.enrichment_status or "active",
                    **update_data
                }

                response = await asyncio.to_thread(
                    client.table("enriched_leads").insert(insert_data).execute
                )
                logger.debug(f"Inserted new enriched lead for {enriched_lead.email}")

        logger.debug(f"Successfully processed batch of {len(enriched_leads)} enriched leads")

    async def get_existing_emails_for_lead(self, lead_id: UUID) -> List[str]:
        """
        Get existing validated emails for a specific lead

        Args:
            lead_id: The lead ID to check

        Returns:
            List of existing email addresses for the lead
        """
        if not lead_id:
            logger.debug("Lead ID is falsy, returning empty list for existing emails")
            return []
        return await self._read_circuit_breaker.call(self._get_existing_emails_for_lead_impl, lead_id)

    async def _get_existing_emails_for_lead_impl(self, lead_id: UUID) -> List[str]:
        """
        Implementation of getting existing emails (called through circuit breaker)

        Args:
            lead_id: The lead ID to check

        Returns:
            List of existing email addresses for the lead
        """
        client = await self.get_client()

        response = await asyncio.to_thread(
            client.table("enriched_leads").select("email").eq("lead_id", str(lead_id)).eq("enrichment_status", "active").execute
        )

        if response.data:
            emails = [record["email"] for record in response.data]
            logger.debug(f"Found {len(emails)} existing emails for lead {lead_id}")
            return emails
        else:
            return []

    async def update_lead_processing_status(self, lead_id: UUID, status: str, metadata: Optional[dict] = None) -> bool:
        """
        Update lead processing status and metadata

        Args:
            lead_id: The lead ID to update
            status: Processing status
            metadata: Additional processing metadata

        Returns:
            True if successful
        """
        try:
            client = await self.get_client()

            update_data = {
                "processing_status": status,
                "updated_at": "now()"
            }

            if metadata:
                update_data["processing_metadata"] = metadata

            # Run synchronous Supabase operation in thread pool to avoid blocking
            response = await asyncio.to_thread(
                client.table("leads").update(update_data).eq("id", str(lead_id)).execute
            )

            logger.debug(f"Updated processing status for lead {lead_id} to {status}")
            return True

        except Exception as e:
            logger.error(f"Failed to update lead processing status: {e}")
            raise

    async def get_leads_count(
        self,
        lead_ids: Optional[List[UUID]] = None,
        domains: Optional[List[str]] = None
    ) -> int:
        """
        Get count of leads matching criteria

        Args:
            lead_ids: Specific lead IDs to count
            domains: Specific domains to count leads for

        Returns:
            Count of matching leads
        """
        try:
            client = await self.get_client()
            query = client.table("leads").select("*", count="exact")

            # Apply filters
            if lead_ids:
                query = query.in_("id", [str(lead_id) for lead_id in lead_ids])

            if domains:
                query = query.in_("domain", domains)

            # Run synchronous operation in thread pool
            response = await asyncio.to_thread(query.execute)
            count = response.count

            logger.debug(f"Found {count} leads matching criteria")
            return count or 0

        except Exception as e:
            logger.error(f"Failed to get leads count: {e}")
            raise

    async def fetch_enriched_leads(
        self,
        enriched_lead_id: Optional[UUID] = None,
        lead_id: Optional[UUID] = None,
        email: Optional[str] = None,
        domain: Optional[str] = None,
        enrichment_source: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[EnrichedLead]:
        """
        Fetch enriched leads from the database with optional filtering

        Args:
            enriched_lead_id: Specific enriched lead ID to fetch
            lead_id: Specific lead ID to fetch enriched leads for
            email: Specific email to fetch
            domain: Domain to filter by (matches against lead's domain)
            enrichment_source: Source of enrichment ("hunter_direct", "perplexity_generated", etc.)
            limit: Maximum number of enriched leads to fetch
            offset: Number of enriched leads to skip

        Returns:
            List of EnrichedLead objects
        """
        try:
            client = await self.get_client()
            query = client.table("enriched_leads").select("*")

            # Apply filters
            if enriched_lead_id:
                query = query.eq("id", str(enriched_lead_id))

            if lead_id:
                query = query.eq("lead_id", str(lead_id))

            if email:
                query = query.eq("email", email)

            if domain:
                # Need to join with leads table to filter by domain
                query = query.eq("company_website", domain)

            if enrichment_source:
                query = query.eq("enrichment_source", enrichment_source)

            # Apply pagination
            if limit:
                query = query.limit(limit)
            if offset:
                query = query.range(offset, offset + (limit or 1000) - 1)

            # Execute query
            response = await asyncio.to_thread(query.execute)

            if response.data:
                enriched_leads = [EnrichedLead(**lead_data) for lead_data in response.data]
                logger.info(f"Fetched {len(enriched_leads)} enriched leads from database")
                return enriched_leads
            else:
                logger.debug("No enriched leads found matching criteria")
                return []

        except Exception as e:
            logger.error(f"Failed to fetch enriched leads: {e}")
            raise

    async def get_processing_stats(self, verbose: bool = False) -> dict:
        """
        Get overall processing statistics

        Args:
            verbose: If True, log stats at INFO level. If False, log at DEBUG level.

        Returns:
            Dictionary with processing statistics
        """
        try:
            client = await self.get_client()

            # Get total leads
            leads_response = await asyncio.to_thread(
                client.table("leads").select("*", count="exact").execute
            )
            total_leads = leads_response.count or 0

            # Get total enriched leads
            enriched_response = await asyncio.to_thread(
                client.table("enriched_leads").select("*", count="exact").execute
            )
            total_enriched_leads = enriched_response.count or 0

            # Get enriched leads by source
            hunter_enriched = await asyncio.to_thread(
                client.table("enriched_leads").select("*", count="exact").eq("enrichment_source", "hunter_direct").execute
            )
            perplexity_enriched = await asyncio.to_thread(
                client.table("enriched_leads").select("*", count="exact").eq("enrichment_source", "perplexity_generated").execute
            )
            google_maps_enriched = await asyncio.to_thread(
                client.table("enriched_leads").select("*", count="exact").eq("enrichment_source", "google_maps").execute
            )

            # Get average confidence scores
            confidence_response = await asyncio.to_thread(
                client.table("enriched_leads").select("confidence").execute
            )
            avg_confidence = 0
            if confidence_response.data:
                scores = [record["confidence"] for record in confidence_response.data if record["confidence"] is not None]
                avg_confidence = sum(scores) / len(scores) if scores else 0

            stats = {
                "total_leads": total_leads,
                "total_enriched_leads": total_enriched_leads,
                "hunter_direct_enriched": hunter_enriched.count or 0,
                "perplexity_generated_enriched": perplexity_enriched.count or 0,
                "google_maps_enriched": google_maps_enriched.count or 0,
                "average_confidence_score": round(avg_confidence, 2),
                "enriched_leads_per_lead": round(total_enriched_leads / total_leads, 2) if total_leads > 0 else 0
            }

            if verbose:
                logger.info(f"Retrieved processing stats: {stats}")
            else:
                logger.debug(f"Retrieved processing stats: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Failed to get processing stats: {e}")
            raise

    async def create_jobs_table_if_not_exists(self) -> None:
        """
        Jobs table should already exist - this method is kept for compatibility
        """
        # Since the table already exists (as confirmed by user), we don't need to verify
        logger.debug("Jobs table assumed to exist - proceeding with service startup")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def save_job(self, job: EnrichmentJob) -> None:
        """
        Save job to database

        Args:
            job: EnrichmentJob object to save
        """
        try:
            client = await self.get_client()

            job_data = job.dict()
            # Convert progress dict to JSON string for storage
            job_data["progress"] = json.dumps(job.progress)

            # Convert UUIDs to strings
            if job_data.get("lead_ids"):
                job_data["lead_ids"] = [str(uuid) for uuid in job_data["lead_ids"]]

            # Convert datetime objects to ISO format strings for JSON serialization
            for key, value in job_data.items():
                if hasattr(value, 'isoformat'):  # Check if it's a datetime object
                    job_data[key] = value.isoformat()

            # Run synchronous Supabase operation in thread pool to avoid blocking
            response = await asyncio.to_thread(client.table("jobs").insert, job_data)
            await asyncio.to_thread(response.execute)

        except Exception as e:
            logger.error(f"Failed to save job to database: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def update_job_status(
        self,
        job_id: str,
        status: str,
        progress: Optional[dict] = None,
        error_message: Optional[str] = None,
        **stats_updates
    ) -> None:
        """
        Update job status and progress

        Args:
            job_id: Job ID to update
            status: New status
            progress: Progress information
            error_message: Error message if failed
            stats_updates: Statistical updates
        """
        try:
            client = await self.get_client()

            update_data = {"status": status, "updated_at": "now()"}

            if progress:
                update_data["progress"] = json.dumps(progress)

            if error_message:
                update_data["error_message"] = error_message

            # Handle timestamps
            if status == "running":
                update_data["started_at"] = "now()"
            elif status in ["completed", "failed", "cancelled"]:
                update_data["completed_at"] = "now()"

            # Add statistical updates
            for key, value in stats_updates.items():
                if key in ["total_leads", "processed_leads", "failed_leads",
                          "created_emails", "api_calls_hunter", "api_calls_perplexity",
                          "processing_time_seconds"]:
                    update_data[key] = value

            # Run synchronous Supabase operation in thread pool to avoid blocking
            response = await asyncio.to_thread(
                client.table("jobs").update(update_data).eq("job_id", job_id).execute
            )

            logger.info(f"Updated job {job_id} status to {status}")

        except Exception as e:
            logger.error(f"Failed to update job status: {e}")
            raise

    async def get_job(self, job_id: str) -> Optional[EnrichmentJob]:
        """Get job by ID"""
        try:
            client = await self.get_client()

            # Run synchronous operation in thread pool
            response = await asyncio.to_thread(
                client.table("jobs").select("*").eq("job_id", job_id).execute
            )

            if response.data:
                job_data = response.data[0]
                # Convert progress from JSON string if needed
                if isinstance(job_data.get("progress"), str):
                    job_data["progress"] = json.loads(job_data["progress"])

                return EnrichmentJob(**job_data)

            return None

        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            return None

    async def get_active_jobs(self) -> List[EnrichmentJob]:
        """Get all active (running/pending) jobs from database"""
        try:
            client = await self.get_client()

            # Query for active jobs
            response = await asyncio.to_thread(
                client.table("jobs").select("*").in_("status", ["running", "pending"]).execute
            )

            jobs = []
            if response.data:
                for job_data in response.data:
                    # Convert progress from JSON string if needed
                    if isinstance(job_data.get("progress"), str):
                        job_data["progress"] = json.loads(job_data["progress"])

                    jobs.append(EnrichmentJob(**job_data))

            return jobs

        except Exception as e:
            logger.error(f"Failed to get active jobs from database: {e}")
            return []

    async def get_pending_jobs(self) -> List[EnrichmentJob]:
        """Get all pending jobs that are ready to be processed"""
        try:
            client = await self.get_client()

            # Query for pending jobs only
            response = await asyncio.to_thread(
                client.table("jobs").select("*").eq("status", "pending").order("created_at").execute
            )

            jobs = []
            if response.data:
                for job_data in response.data:
                    # Convert progress from JSON string if needed
                    if isinstance(job_data.get("progress"), str):
                        job_data["progress"] = json.loads(job_data["progress"])

                    jobs.append(EnrichmentJob(**job_data))

            return jobs

        except Exception as e:
            logger.error(f"Failed to get pending jobs from database: {e}")
            return []

    async def get_running_jobs(self) -> List[EnrichmentJob]:
        """Get all jobs currently in 'running' status"""
        try:
            client = await self.get_client()

            # Query for running jobs only
            response = await asyncio.to_thread(
                client.table("jobs").select("*").eq("status", "running").order("created_at").execute
            )

            jobs = []
            if response.data:
                for job_data in response.data:
                    # Convert progress from JSON string if needed
                    if isinstance(job_data.get("progress"), str):
                        job_data["progress"] = json.loads(job_data["progress"])

                    jobs.append(EnrichmentJob(**job_data))

            return jobs

        except Exception as e:
            logger.error(f"Failed to get running jobs from database: {e}")
            return []

    async def get_job_queue_status(self) -> dict:
        """Get overall job queue status"""
        try:
            client = await self.get_client()

            # Count jobs by status
            status_counts = {}
            for status in ["pending", "running", "paused", "completed", "failed", "cancelled"]:
                response = await asyncio.to_thread(
                    client.table("jobs").select("*", count="exact").eq("status", status).execute
                )
                status_counts[status] = response.count or 0

            # Get recent jobs
            recent_jobs_response = await asyncio.to_thread(
                client.table("jobs").select("*").order("created_at", desc=True).limit(10).execute
            )
            recent_jobs = []
            if recent_jobs_response.data:
                for job_data in recent_jobs_response.data:
                    if isinstance(job_data.get("progress"), str):
                        job_data["progress"] = json.loads(job_data["progress"])
                    recent_jobs.append(EnrichmentJob(**job_data))

            return {
                "status_counts": status_counts,
                "recent_jobs": recent_jobs,
                "total_jobs": sum(status_counts.values())
            }

        except Exception as e:
            logger.error(f"Failed to get job queue status: {e}")
            return {"error": str(e)}

    async def close(self):
        """Close database connections"""
        if self._client:
            # Supabase client doesn't have explicit close method, but we can clear the reference
            self._client = None
            logger.info("Database client closed")


# Global database client instance
db_client = DatabaseClient()


async def get_db_client() -> DatabaseClient:
    """Get the global database client instance"""
    return db_client
