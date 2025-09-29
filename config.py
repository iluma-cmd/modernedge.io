"""
Configuration management for Email Enrichment Service
"""
import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import Field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load environment variables from .env file
load_dotenv()


class Settings(BaseSettings):
    """Application settings with environment variable support"""

    # Supabase Configuration
    supabase_url: str = Field(alias="NEXT_PUBLIC_SUPABASE_URL")
    supabase_anon_key: str = Field(alias="NEXT_PUBLIC_SUPABASE_ANON_KEY")
    supabase_service_role_key: str = Field(alias="SUPABASE_SERVICE_ROLE_KEY")

    # API Keys
    hunter_api_key: str = Field(alias="HUNTER_API_KEY")
    perplexity_api_key: str = Field(default="dummy-key", alias="PERPLEXITY_API_KEY")

    # Service Configuration
    log_level: str = Field("INFO", env="LOG_LEVEL")
    max_workers: int = Field(5, env="MAX_WORKERS")
    batch_size: int = Field(10, env="BATCH_SIZE")
    request_timeout: int = Field(30, env="REQUEST_TIMEOUT")

    # Processing Limits
    max_emails_per_domain: int = Field(3, env="MAX_EMAILS_PER_DOMAIN")
    hunter_confidence_threshold: int = Field(70, env="HUNTER_CONFIDENCE_THRESHOLD")
    verification_confidence_threshold: int = Field(65, env="VERIFICATION_CONFIDENCE_THRESHOLD")

    # Rate Limiting
    hunter_rate_limit: int = Field(50, env="HUNTER_RATE_LIMIT")  # requests per minute (relaxed for paid plans)
    perplexity_rate_limit: int = Field(30, env="PERPLEXITY_RATE_LIMIT")  # requests per minute

    # Database Configuration
    db_connection_pool_size: int = Field(10, env="DB_CONNECTION_POOL_SIZE")
    db_connection_timeout: int = Field(30, env="DB_CONNECTION_TIMEOUT")

    # Caching (Redis)
    redis_url: Optional[str] = Field(None, env="REDIS_URL")
    cache_ttl: int = Field(3600, env="CACHE_TTL")  # 1 hour default

    # Processing Configuration
    skip_existing: bool = Field(True, env="SKIP_EXISTING")

    # Background Service Configuration
    health_check_port: int = Field(8000, env="HEALTH_CHECK_PORT")
    health_check_host: str = Field("0.0.0.0", env="HEALTH_CHECK_HOST")
    job_polling_interval: int = Field(30, env="JOB_POLLING_INTERVAL")  # seconds
    service_name: str = Field("email-enrichment-service", env="SERVICE_NAME")
    
    # Logging Configuration
    log_file_enabled: bool = Field(True, env="LOG_FILE_ENABLED")
    log_file_path: str = Field("logs", env="LOG_FILE_PATH")
    log_rotation: str = Field("10 MB", env="LOG_ROTATION")
    log_retention: str = Field("30 days", env="LOG_RETENTION")

    # Development
    debug_mode: bool = Field(False, env="DEBUG_MODE")
    dry_run: bool = Field(False, env="DRY_RUN")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Allow extra fields in env file for compatibility
    )

    @validator("log_level")
    def validate_log_level(cls, v):
        """Validate log level is one of the standard Python logging levels"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()

    @validator("max_workers")
    def validate_max_workers(cls, v):
        """Ensure max_workers is reasonable"""
        if v < 1 or v > 50:
            raise ValueError("max_workers must be between 1 and 50")
        return v

    @validator("batch_size")
    def validate_batch_size(cls, v):
        """Ensure batch_size is reasonable"""
        if v < 1 or v > 100:
            raise ValueError("batch_size must be between 1 and 100")
        return v

    @validator("hunter_confidence_threshold", "verification_confidence_threshold")
    def validate_confidence_threshold(cls, v):
        """Ensure confidence thresholds are valid percentages"""
        if v < 0 or v > 100:
            raise ValueError("Confidence threshold must be between 0 and 100")
        return v


# Global settings instance - lazy loaded
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance"""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


# For backward compatibility and easier testing
settings = None


def reload_settings() -> Settings:
    """Reload settings from environment"""
    global settings
    settings = Settings()
    return settings
