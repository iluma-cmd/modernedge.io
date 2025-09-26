"""
Job management system for preventing overlaps and managing batch processing
"""
import asyncio
import time
from typing import Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from loguru import logger

from database import get_db_client
from models import EnrichmentJob


class JobManager:
    """Manages job queuing, execution, and prevents overlapping jobs"""

    def __init__(self):
        self.db_client = None
        self._active_jobs: Dict[str, EnrichmentJob] = {}
        self._job_locks: Dict[str, asyncio.Lock] = {}

    async def _get_db_client(self):
        """Lazy initialization of database client"""
        if self.db_client is None:
            self.db_client = await get_db_client()
        return self.db_client

    async def create_job(
        self,
        lead_ids: Optional[List[UUID]] = None,
        domains: Optional[List[str]] = None,
        process_all: bool = False,
        skip_existing: bool = True,
        max_concurrent: Optional[int] = None,
        batch_size: Optional[int] = None,
        job_type: str = "enrichment",
        metadata: Optional[dict] = None
    ) -> EnrichmentJob:
        """
        Create a new job with conflict checking

        Args:
            lead_ids: Specific lead IDs to process
            domains: Specific domains to process
            process_all: Process all leads
            skip_existing: Skip leads with existing emails
            max_concurrent: Maximum concurrent processing
            batch_size: Size of processing batches
            job_type: Type of job
            metadata: Optional metadata for the job (e.g., scraping_run_id)

        Returns:
            Created job object

        Raises:
            ValueError: If conflicting job exists
        """
        # Check for conflicting jobs
        await self._check_job_conflicts(lead_ids, domains, process_all)

        job_id = str(uuid4())
        created_at = time.time()

        job = EnrichmentJob(
            job_id=job_id,
            lead_ids=lead_ids,
            domains=domains,
            process_all=process_all,
            skip_existing=skip_existing,
            max_concurrent=max_concurrent,
            batch_size=batch_size,
            metadata=metadata or {},
            created_at=created_at,
            status="pending"
        )

        # Store in database
        db_client = await self._get_db_client()
        await db_client.save_job(job)

        logger.info(f"Created job {job_id} with type {job_type}")
        return job

    async def _check_job_conflicts(
        self,
        lead_ids: Optional[List[UUID]] = None,
        domains: Optional[List[str]] = None,
        process_all: bool = False
    ) -> None:
        """
        Check for conflicting jobs that would overlap

        Args:
            lead_ids: Lead IDs for the new job
            domains: Domains for the new job
            process_all: Whether new job processes all leads

        Raises:
            ValueError: If conflicting job found
        """
        db_client = await self._get_db_client()

        # Get all running or pending jobs
        running_jobs = await self._get_active_jobs_from_db()

        for job in running_jobs:
            if self._jobs_conflict(job, lead_ids, domains, process_all):
                raise ValueError(
                    f"Conflicting job {job.job_id} is already running. "
                    f"Job status: {job.status}, created: {job.created_at}"
                )

    def _jobs_conflict(
        self,
        existing_job: EnrichmentJob,
        new_lead_ids: Optional[List[UUID]] = None,
        new_domains: Optional[List[str]] = None,
        new_process_all: bool = False
    ) -> bool:
        """
        Check if two jobs would conflict

        Args:
            existing_job: Existing job to check against
            new_lead_ids: New job lead IDs
            new_domains: New job domains
            new_process_all: Whether new job processes all

        Returns:
            True if jobs conflict
        """
        # If either job processes all, they conflict
        if existing_job.process_all or new_process_all:
            return True

        # Check lead ID overlaps
        if existing_job.lead_ids and new_lead_ids:
            existing_set = set(str(lid) for lid in existing_job.lead_ids)
            new_set = set(str(lid) for lid in new_lead_ids)
            if existing_set & new_set:  # Intersection
                return True

        # Check domain overlaps
        if existing_job.domains and new_domains:
            existing_set = set(existing_job.domains)
            new_set = set(new_domains)
            if existing_set & new_set:  # Intersection
                return True

        # If one job has domains and other has lead_ids, we can't easily check overlap
        # To be safe, consider this a potential conflict
        if (existing_job.domains and new_lead_ids) or (existing_job.lead_ids and new_domains):
            logger.warning("Potential conflict between domain-based and lead-based jobs")
            return True

        return False

    async def _get_active_jobs_from_db(self) -> List[EnrichmentJob]:
        """Get all active (running/pending) jobs from database"""
        try:
            db_client = await self._get_db_client()
            return await db_client.get_active_jobs()

        except Exception as e:
            logger.error(f"Failed to get active jobs from database: {e}")
            return []


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
            db_client = await self._get_db_client()
            await db_client.update_job_status(job_id, status, progress, error_message, **stats_updates)

        except Exception as e:
            logger.error(f"Failed to update job status: {e}")
            raise

    async def get_job(self, job_id: str) -> Optional[EnrichmentJob]:
        """Get job by ID"""
        try:
            db_client = await self._get_db_client()
            return await db_client.get_job(job_id)

        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            return None

    async def get_active_jobs(self) -> List[EnrichmentJob]:
        """Get all active jobs"""
        return await self._get_active_jobs_from_db()

    async def get_pending_jobs(self) -> List[EnrichmentJob]:
        """Get all pending jobs that are ready to be processed"""
        try:
            db_client = await self._get_db_client()
            return await db_client.get_pending_jobs()
        except Exception as e:
            logger.error(f"Failed to get pending jobs: {e}")
            return []

    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a job if it's pending or running

        Args:
            job_id: Job ID to cancel

        Returns:
            True if cancelled successfully
        """
        try:
            job = await self.get_job(job_id)
            if not job:
                return False

            if job.status not in ["pending", "running"]:
                logger.warning(f"Cannot cancel job {job_id} with status {job.status}")
                return False

            await self.update_job_status(job_id, "cancelled")
            logger.info(f"Cancelled job {job_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False

    async def get_job_queue_status(self) -> dict:
        """Get overall job queue status"""
        try:
            db_client = await self._get_db_client()
            return await db_client.get_job_queue_status()

        except Exception as e:
            logger.error(f"Failed to get job queue status: {e}")
            return {"error": str(e)}

    async def cleanup_old_jobs(self, days_old: int = 30) -> int:
        """
        Clean up old completed/failed jobs

        Args:
            days_old: Remove jobs older than this many days

        Returns:
            Number of jobs removed
        """
        try:
            db_client = await self._get_db_client()

            # This would require a more complex query in Supabase
            # For now, just log that cleanup is needed
            logger.info(f"Job cleanup requested for jobs older than {days_old} days")
            return 0

        except Exception as e:
            logger.error(f"Failed to cleanup old jobs: {e}")
            return 0

    async def acquire_job_lock(self, job_id: str) -> bool:
        """
        Acquire a lock for job processing to prevent concurrent execution

        Args:
            job_id: Job ID to lock

        Returns:
            True if lock acquired, False if already locked
        """
        if job_id not in self._job_locks:
            self._job_locks[job_id] = asyncio.Lock()

        lock = self._job_locks[job_id]

        # Try to acquire lock without blocking
        if lock.locked():
            logger.warning(f"Job {job_id} is already locked (running)")
            return False

        await lock.acquire()
        logger.debug(f"Acquired lock for job {job_id}")
        return True

    def release_job_lock(self, job_id: str) -> None:
        """Release job lock"""
        if job_id in self._job_locks:
            lock = self._job_locks[job_id]
            if lock.locked():
                lock.release()
                logger.debug(f"Released lock for job {job_id}")


# Global job manager instance
job_manager = JobManager()


async def get_job_manager() -> JobManager:
    """Get the global job manager instance"""
    return job_manager
