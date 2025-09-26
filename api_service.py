"""
External API service for triggering email enrichment jobs
Provides HTTP endpoints to create jobs that the background worker will process
"""
import asyncio
import os
from contextlib import asynccontextmanager
from typing import List, Optional
from uuid import UUID

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel

from config import get_settings
from database import get_db_client
from job_manager import get_job_manager


# Pydantic models for API requests
class CreateJobRequest(BaseModel):
    """Request model for creating enrichment jobs"""
    lead_ids: Optional[List[str]] = None
    domains: Optional[List[str]] = None
    process_all: bool = False
    skip_existing: bool = True
    max_concurrent: Optional[int] = None
    batch_size: Optional[int] = None
    job_type: str = "enrichment"


class ScrapingRunJobRequest(BaseModel):
    """Request model for creating scraping run jobs"""
    scraping_run_id: str
    lead_domain: Optional[str] = None
    max_concurrent: Optional[int] = None
    batch_size: Optional[int] = None


class JobResponse(BaseModel):
    """Response model for job creation"""
    job_id: str
    status: str
    message: str
    details: dict


class JobStatusResponse(BaseModel):
    """Response model for job status"""
    job_id: str
    status: str
    progress: Optional[dict] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    total_leads: Optional[int] = None
    processed_leads: Optional[int] = None
    failed_leads: Optional[int] = None
    created_emails: Optional[int] = None
    processing_time_seconds: Optional[float] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan manager"""
    logger.info("Starting Email Enrichment API Service")
    yield
    logger.info("Shutting down Email Enrichment API Service")


# Create FastAPI app
app = FastAPI(
    title="Email Enrichment API",
    description="External API for triggering email enrichment jobs",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/ping")
async def ping():
    """Simple ping endpoint to check service availability"""
    from datetime import datetime
    return {
        "ping": "pong",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "email-enrichment-api"
    }


@app.get("/health")
async def health_check():
    """Basic health check endpoint"""
    return {"status": "healthy", "service": "email-enrichment-api"}


@app.post("/jobs", response_model=JobResponse)
async def create_enrichment_job(request: CreateJobRequest, background_tasks: BackgroundTasks):
    """
    Create a new enrichment job

    The job will be picked up by the background worker for processing.
    """
    try:
        # Validate UUIDs if provided
        lead_uuids = None
        if request.lead_ids:
            try:
                lead_uuids = [UUID(lead_id) for lead_id in request.lead_ids]
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid UUID format: {e}")

        # Initialize job manager
        job_manager = await get_job_manager()

        # Create the job
        job = await job_manager.create_job(
            lead_ids=lead_uuids,
            domains=request.domains,
            process_all=request.process_all,
            skip_existing=request.skip_existing,
            max_concurrent=request.max_concurrent,
            batch_size=request.batch_size,
            job_type=request.job_type
        )

        logger.info(f"Created job {job.job_id} via API")

        return JobResponse(
            job_id=job.job_id,
            status=job.status,
            message="Job created successfully. It will be processed by the background worker.",
            details={
                "lead_ids": [str(uid) for uid in (job.lead_ids or [])],
                "domains": job.domains,
                "process_all": job.process_all,
                "created_at": job.created_at
            }
        )

    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create job: {str(e)}")


@app.post("/jobs/scraping-run", response_model=JobResponse)
async def create_scraping_run_job(request: ScrapingRunJobRequest, background_tasks: BackgroundTasks):
    """
    Create a job to process leads from a specific scraping run

    The job will be picked up by the background worker for processing.
    """
    try:
        # Validate scraping run UUID
        try:
            scraping_uuid = UUID(request.scraping_run_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid scraping_run_id format")

        # Initialize job manager
        job_manager = await get_job_manager()

        # Create job with scraping run metadata
        job_metadata = {
            "scraping_run_id": str(scraping_uuid),
            "lead_domain": request.lead_domain
        }

        job = await job_manager.create_job(
            process_all=False,  # We'll specify leads via metadata
            skip_existing=True,
            max_concurrent=request.max_concurrent,
            batch_size=request.batch_size,
            metadata=job_metadata
        )

        logger.info(f"Created scraping run job {job.job_id} for run {scraping_uuid}")

        return JobResponse(
            job_id=job.job_id,
            status=job.status,
            message="Scraping run job created successfully. It will be processed by the background worker.",
            details={
                "scraping_run_id": str(scraping_uuid),
                "lead_domain": request.lead_domain,
                "created_at": job.created_at
            }
        )

    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create scraping run job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create job: {str(e)}")


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get the status of a specific job"""
    try:
        job_manager = await get_job_manager()
        job = await job_manager.get_job(job_id)

        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        return JobStatusResponse(
            job_id=job.job_id,
            status=job.status,
            progress=job.progress,
            created_at=job.created_at.isoformat() if job.created_at else None,
            started_at=job.started_at.isoformat() if job.started_at else None,
            completed_at=job.completed_at.isoformat() if job.completed_at else None,
            error_message=job.error_message,
            total_leads=job.total_leads,
            processed_leads=job.processed_leads,
            failed_leads=job.failed_leads,
            created_emails=job.created_emails,
            processing_time_seconds=job.processing_time_seconds
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get job status: {str(e)}")


@app.get("/jobs")
async def list_jobs(limit: int = 50, offset: int = 0):
    """List recent jobs"""
    try:
        job_manager = await get_job_manager()
        queue_status = await job_manager.get_job_queue_status()

        return {
            "total_jobs": queue_status["total_jobs"],
            "status_counts": queue_status["status_counts"],
            "recent_jobs": [
                {
                    "job_id": job.job_id,
                    "status": job.status,
                    "created_at": job.created_at.isoformat() if job.created_at else None,
                    "processed_leads": job.processed_leads,
                    "total_leads": job.total_leads
                }
                for job in queue_status["recent_jobs"][offset:offset + limit]
            ]
        }

    except Exception as e:
        logger.error(f"Failed to list jobs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a pending or running job"""
    try:
        job_manager = await get_job_manager()
        success = await job_manager.cancel_job(job_id)

        if success:
            return {"message": f"Job {job_id} cancelled successfully"}
        else:
            raise HTTPException(status_code=400, detail=f"Failed to cancel job {job_id}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to cancel job: {str(e)}")


@app.get("/stats")
async def get_stats():
    """Get database statistics"""
    try:
        db_client = await get_db_client()
        stats = await db_client.get_processing_stats()
        await db_client.close()

        return stats

    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


if __name__ == "__main__":
    # Setup logging
    from main import setup_production_logging
    setup_production_logging()

    # Get port from environment or default to 8000
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")

    logger.info(f"Starting Email Enrichment API Service on {host}:{port}")
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        access_log=False
    )
