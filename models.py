"""
Pydantic models for Email Enrichment Service
"""
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, validator


class Lead(BaseModel):
    """Lead model from Supabase leads table"""
    id: Optional[UUID] = None
    company_name: str
    domain: str
    industry: Optional[str] = None
    company_size: Optional[str] = None
    location: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @validator("domain")
    def validate_domain(cls, v):
        """Basic domain validation"""
        if not v or "." not in v:
            raise ValueError("Invalid domain format")
        return v.lower().strip()


class EmailResult(BaseModel):
    """Email result from Hunter.io domain search"""
    email: EmailStr
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    position: Optional[str] = None
    seniority: Optional[str] = None
    department: Optional[str] = None
    linkedin_url: Optional[str] = None
    twitter_username: Optional[str] = None
    confidence_score: int = Field(..., ge=0, le=100)
    sources: List[str] = Field(default_factory=list)

    @validator("confidence_score")
    def validate_confidence(cls, v):
        if v < 0 or v > 100:
            raise ValueError("Confidence score must be between 0 and 100")
        return v


class HunterDomainResponse(BaseModel):
    """Response from Hunter.io Domain Search API"""
    domain: str
    disposable: bool = False
    webmail: bool = False
    accept_all: bool = False
    pattern: Optional[str] = None
    organization: Optional[str] = None
    description: Optional[str] = None
    industry: Optional[str] = None
    twitter: Optional[str] = None
    facebook: Optional[str] = None
    linkedin: Optional[str] = None
    instagram: Optional[str] = None
    youtube: Optional[str] = None
    emails: List[EmailResult] = Field(default_factory=list)
    country: Optional[str] = None


class VerificationResult(BaseModel):
    """Email verification result from Hunter.io"""
    email: EmailStr
    result: str  # "deliverable", "undeliverable", "risky", "unknown"
    score: int = Field(..., ge=0, le=100)
    disposable: bool = False
    webmail: bool = False
    mx_records: bool = False
    smtp_server: bool = False
    smtp_check: bool = False
    accept_all: bool = False
    block: bool = False
    gibberish: bool = False
    role: bool = False
    sources: List[str] = Field(default_factory=list)

    @validator("result")
    def validate_result(cls, v):
        valid_results = ["deliverable", "undeliverable", "risky", "unknown"]
        if v not in valid_results:
            raise ValueError(f"Result must be one of {valid_results}")
        return v

    @validator("score")
    def validate_score(cls, v):
        if v < 0 or v > 100:
            raise ValueError("Score must be between 0 and 100")
        return v


class PerplexityResponse(BaseModel):
    """Response from Perplexity AI API"""
    content: str
    usage: Optional[dict] = None
    request_id: Optional[str] = None


class PersonInfo(BaseModel):
    """Extracted person information from Perplexity response"""
    first_name: str
    last_name: str
    title: Optional[str] = None
    company: Optional[str] = None
    confidence: float = Field(..., ge=0.0, le=1.0)


class GeneratedEmail(BaseModel):
    """Generated email permutation"""
    email: EmailStr
    pattern: str  # e.g., "firstname.lastname", "f.lastname", etc.
    source: str = "perplexity_generated"


class ValidatedEmail(BaseModel):
    """Final validated email ready for database storage"""
    lead_id: Optional[UUID] = None
    email: EmailStr
    source: str  # "hunter_direct" or "perplexity_generated"
    confidence_score: int = Field(..., ge=0, le=100)
    email_type: str  # "primary" or "secondary"
    hunter_status: str  # "deliverable", "undeliverable", "risky", "unknown"
    verified_at: datetime = Field(default_factory=datetime.utcnow)
    status: str = "active"

    # Person information (from Hunter API)
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    position: Optional[str] = None  # job title

    @validator("source")
    def validate_source(cls, v):
        valid_sources = ["hunter_direct", "perplexity_generated"]
        if v not in valid_sources:
            raise ValueError(f"Source must be one of {valid_sources}")
        return v

    @validator("email_type")
    def validate_email_type(cls, v):
        valid_types = ["primary", "secondary"]
        if v not in valid_types:
            raise ValueError(f"Email type must be one of {valid_types}")
        return v

    @validator("hunter_status")
    def validate_hunter_status(cls, v):
        valid_statuses = ["deliverable", "undeliverable", "risky", "unknown"]
        if v not in valid_statuses:
            raise ValueError(f"Hunter status must be one of {valid_statuses}")
        return v

    @validator("status")
    def validate_status(cls, v):
        valid_statuses = ["active", "inactive"]
        if v not in valid_statuses:
            raise ValueError(f"Status must be one of {valid_statuses}")
        return v


class EnrichedLead(BaseModel):
    """Enriched lead model matching enriched_leads table"""
    id: Optional[UUID] = None
    lead_id: UUID
    scraping_run_id: Optional[UUID] = None  # Link to the scraping run that discovered this lead
    email: Optional[EmailStr] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    company_website: Optional[str] = None
    phone_number: Optional[str] = None
    linkedin_person_url: Optional[str] = None
    linkedin_company_url: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    address_line: Optional[str] = None

    # Google Maps business info
    business_category: Optional[str] = None
    business_rating: Optional[float] = None
    business_reviews: Optional[int] = None
    price_level: Optional[int] = None
    place_id: Optional[str] = None

    # Verification
    confidence: Optional[int] = None
    verification_status: Optional[str] = None
    verification_score: Optional[int] = None
    email_disposable: Optional[bool] = None
    email_webmail: Optional[bool] = None

    # Metadata
    enrichment_source: str
    enrichment_status: str = "active"
    enrichment_reason: Optional[str] = None
    processed_at: datetime = Field(default_factory=datetime.utcnow)
    processed_by_job_id: Optional[str] = None
    scraping_run_id: Optional[UUID] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ProcessingStats(BaseModel):
    """Statistics for processing results"""
    leads_processed: int = 0
    failed_leads: int = 0
    emails_found_hunter: int = 0
    emails_found_perplexity: int = 0
    emails_validated: int = 0
    emails_failed_validation: int = 0
    processing_time_seconds: float = 0.0
    api_calls_hunter: int = 0
    api_calls_perplexity: int = 0
    errors: List[str] = Field(default_factory=list)


class EnrichmentJob(BaseModel):
    """Job configuration for enrichment processing"""
    job_id: str
    lead_ids: Optional[List[UUID]] = None
    domains: Optional[List[str]] = None
    process_all: bool = False
    skip_existing: bool = True
    max_concurrent: Optional[int] = None
    batch_size: Optional[int] = None
    metadata: Optional[dict] = Field(default_factory=dict)  # job metadata (e.g., scraping_run_id)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    status: str = "pending"  # pending, running, paused, completed, failed, cancelled
    progress: dict = Field(default_factory=dict)  # progress tracking
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_leads: int = 0
    processed_leads: int = 0
    failed_leads: int = 0
    created_emails: int = 0
    api_calls_hunter: int = 0
    api_calls_perplexity: int = 0
    processing_time_seconds: float = 0.0
