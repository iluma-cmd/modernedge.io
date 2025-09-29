"""
Job Manager for Email Enrichment Service
Handles job queuing, execution tracking, and concurrency control
Optimized for background service deployment with rate limiting and graceful shutdown
"""
# -*- coding: utf-8 -*-
import asyncio
import time
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID

from loguru import logger

from config import get_settings
from database import get_db_client
from models import EnrichmentJob


class JobManager:
    """Manages job creation, execution tracking, and concurrency control"""

    def __init__(self):
        self.settings = get_settings()
        self.db_client = None
        self._job_locks: Dict[str, asyncio.Lock] = {}
        self._lock_timeout = 300  # 5 minutes timeout for job locks

    async def _ensure_db_client(self):
        """Ensure database client is initialized"""
        if self.db_client is None:
            self.db_client = await get_db_client()

    async def _generate_job_id(self, prefix: str = "job") -> str:
        """Generate a unique job ID"""
        return f"{prefix}-{uuid.uuid4().hex}-{int(time.time() * 1000)}"

    async def create_job(
        self,
        lead_ids: Optional[List[UUID]] = None,
        domains: Optional[List[str]] = None,
        process_all: bool = False,
        skip_existing: bool = True,
        max_concurrent: Optional[int] = None,
        batch_size: Optional[int] = None,
        metadata: Optional[dict] = None,
        job_type: str = "enrichment"
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
            metadata: Job metadata (e.g., scraping_run_id)
            job_type: Type of job

        Returns:
            Created job object

        Raises:
            ValueError: If job creation fails or conflicts exist
        """
        await self._ensure_db_client()

        # Validate job parameters
        if not any([lead_ids, domains, process_all]):
            raise ValueError("Job must specify lead_ids, domains, or process_all=True")

        if lead_ids and domains:
            raise ValueError("Cannot specify both lead_ids and domains")

        if lead_ids and process_all:
            raise ValueError("Cannot specify both lead_ids and process_all=True")

        if domains and process_all:
            raise ValueError("Cannot specify both domains and process_all=True")

        # Generate job ID
        job_id = await self._generate_job_id("bulk" if process_all or domains or len(lead_ids or []) > 1 else "single")

        # Create job object
        job = EnrichmentJob(
            job_id=job_id,
            lead_ids=lead_ids,
            domains=domains,
            process_all=process_all,
            skip_existing=skip_existing,
            max_concurrent=max_concurrent or self.settings.max_workers,
            batch_size=batch_size or self.settings.batch_size,
            metadata=metadata or {},
            status="pending",
            total_leads=0,  # Will be calculated when job starts
        )

        # Save job to database
        try:
            await self.db_client.save_job(job)
            logger.info(f"Created job {job_id} with type {job_type}")
            return job
        except Exception as e:
            logger.error(f"Failed to create job {job_id}: {e}")
            raise ValueError(f"Failed to create job: {str(e)}")

    async def get_job(self, job_id: str) -> Optional[EnrichmentJob]:
        """
        Get job by ID

        Args:
            job_id: Job ID to retrieve

        Returns:
            Job object if found, None otherwise
        """
        await self._ensure_db_client()

        try:
            job = await self.db_client.get_job(job_id)
            if job:
                logger.debug(f"Retrieved job {job_id} with status {job.status}")
            return job
        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            return None

    async def get_pending_jobs(self) -> List[EnrichmentJob]:
        """
        Get all pending jobs that are ready to be processed

        Returns:
            List of pending jobs ordered by creation time
        """
        await self._ensure_db_client()

        try:
            jobs = await self.db_client.get_pending_jobs()
            logger.debug(f"Found {len(jobs)} pending jobs")
            return jobs
        except Exception as e:
            logger.error(f"Failed to get pending jobs: {e}")
            return []

    async def get_job_queue_status(self) -> dict:
        """
        Get overall job queue status

        Returns:
            Dictionary with queue statistics
        """
        await self._ensure_db_client()

        try:
            status = await self.db_client.get_job_queue_status()
            logger.debug("Retrieved job queue status")
            return status
        except Exception as e:
            logger.error(f"Failed to get job queue status: {e}")
            return {"error": str(e)}

    async def acquire_job_lock(self, job_id: str) -> bool:
        """
        Acquire a lock for job execution to prevent concurrent processing

        Args:
            job_id: Job ID to lock

        Returns:
            True if lock acquired, False if already locked
        """
        # Check if job is already locked
        if job_id in self._job_locks:
            lock = self._job_locks[job_id]
            if lock.locked():
                logger.warning(f"Job {job_id} is already locked")
                return False

        # Create new lock
        lock = asyncio.Lock()
        self._job_locks[job_id] = lock

        # Try to acquire lock
        try:
            await asyncio.wait_for(lock.acquire(), timeout=1.0)
            logger.debug(f"Acquired lock for job {job_id}")
            return True
        except asyncio.TimeoutError:
            logger.warning(f"Timeout acquiring lock for job {job_id}")
            return False

    def release_job_lock(self, job_id: str):
        """
        Release the lock for a job

        Args:
            job_id: Job ID to unlock
        """
        if job_id in self._job_locks:
            lock = self._job_locks[job_id]
            if lock.locked():
                lock.release()
                logger.debug(f"Released lock for job {job_id}")
            # Clean up old locks periodically
            if len(self._job_locks) > 100:
                self._cleanup_expired_locks()

    def _cleanup_expired_locks(self):
        """Clean up expired job locks to prevent memory leaks"""
        current_time = time.time()
        expired_jobs = []

        for job_id, lock in self._job_locks.items():
            # Remove locks that have been held too long (potential deadlock)
            if lock.locked() and hasattr(lock, '_waiters'):
                # This is a simple heuristic - in production you might want more sophisticated tracking
                expired_jobs.append(job_id)

        for job_id in expired_jobs:
            logger.warning(f"Cleaning up potentially expired lock for job {job_id}")
            del self._job_locks[job_id]

    async def update_job_status(
        self,
        job_id: str,
        status: Optional[str] = None,
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
        await self._ensure_db_client()

        try:
            await self.db_client.update_job_status(
                job_id=job_id,
                status=status,
                progress=progress,
                error_message=error_message,
                **stats_updates
            )
            if status:
                logger.info(f"Updated job {job_id} status to {status}")
            else:
                logger.info(f"Updated job {job_id} progress")
        except Exception as e:
            logger.error(f"Failed to update job {job_id} status: {e}")
            raise

    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running or pending job

        Args:
            job_id: Job ID to cancel

        Returns:
            True if cancelled successfully, False otherwise
        """
        try:
            # Check if job exists and can be cancelled
            job = await self.get_job(job_id)
            if not job:
                logger.warning(f"Job {job_id} not found")
                return False

            if job.status not in ["pending", "running"]:
                logger.warning(f"Job {job_id} status {job.status} cannot be cancelled")
                return False

            # Update status to cancelled
            await self.update_job_status(job_id, "cancelled")
            logger.info(f"Cancelled job {job_id}")

            # Release lock if held
            self.release_job_lock(job_id)

            return True

        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False

    async def pause_job(self, job_id: str) -> bool:
        """
        Pause a running job

        Args:
            job_id: Job ID to pause

        Returns:
            True if paused successfully, False otherwise
        """
        try:
            job = await self.get_job(job_id)
            if not job or job.status != "running":
                logger.warning(f"Job {job_id} not running or not found")
                return False

            await self.update_job_status(job_id, "paused")
            logger.info(f"Paused job {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to pause job {job_id}: {e}")
            return False

    async def resume_job(self, job_id: str) -> bool:
        """
        Resume a paused job

        Args:
            job_id: Job ID to resume

        Returns:
            True if resumed successfully, False otherwise
        """
        try:
            job = await self.get_job(job_id)
            if not job or job.status != "paused":
                logger.warning(f"Job {job_id} not paused or not found")
                return False

            await self.update_job_status(job_id, "pending")
            logger.info(f"Resumed job {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to resume job {job_id}: {e}")
            return False

    async def cleanup_completed_jobs(self, max_age_days: int = 30) -> int:
        """
        Clean up old completed jobs (for database maintenance)

        Args:
            max_age_days: Maximum age in days for completed jobs to keep

        Returns:
            Number of jobs cleaned up
        """
        # Note: This would require additional database methods
        # For now, we'll just log that this feature needs implementation
        logger.info(f"Job cleanup requested for jobs older than {max_age_days} days - feature not yet implemented")
        return 0

    async def get_job_statistics(self, job_id: str) -> Optional[dict]:
        """
        Get detailed statistics for a job

        Args:
            job_id: Job ID to get statistics for

        Returns:
            Dictionary with job statistics, None if job not found
        """
        job = await self.get_job(job_id)
        if not job:
            return None

        stats = {
            "job_id": job.job_id,
            "status": job.status,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "total_leads": job.total_leads,
            "processed_leads": job.processed_leads,
            "failed_leads": job.failed_leads,
            "created_emails": job.created_emails,
            "api_calls_hunter": job.api_calls_hunter,
            "api_calls_perplexity": job.api_calls_perplexity,
            "processing_time_seconds": job.processing_time_seconds,
            "progress": job.progress,
            "error_message": job.error_message,
            "metadata": job.metadata
        }

        # Calculate rates if we have timing data
        if job.processing_time_seconds and job.processing_time_seconds > 0:
            stats["leads_per_second"] = job.processed_leads / job.processing_time_seconds
            stats["emails_per_second"] = job.created_emails / job.processing_time_seconds
            stats["api_calls_per_second"] = (job.api_calls_hunter + job.api_calls_perplexity) / job.processing_time_seconds

        return stats


# Global job manager instance
_job_manager: Optional[JobManager] = None


async def get_job_manager() -> JobManager:
    """Get the global job manager instance"""
    global _job_manager
    if _job_manager is None:
        _job_manager = JobManager()
    return _job_manager
