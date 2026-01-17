"""
Configuration management for the job queue system.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    # Redis Configuration
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None
    redis_max_connections: int = 50
    
    # PostgreSQL Configuration
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "jobqueue"
    postgres_user: str = "jobqueue"
    postgres_password: str = "jobqueue123"
    postgres_max_connections: int = 20
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4
    api_keys: Optional[List[str]] = None  # List of valid API keys (comma-separated in env)
    
    # Worker Configuration
    worker_concurrency: int = 4
    worker_heartbeat_interval: int = 30
    worker_max_tasks_per_child: int = 1000
    
    # Queue Configuration
    max_retries: int = 3
    retry_backoff_base: int = 2
    task_timeout: int = 300
    result_ttl: int = 3600
    
    # Rate Limiting (tasks per minute)
    rate_limit_high: int = 1000
    rate_limit_medium: int = 500
    rate_limit_low: int = 100
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "json"
    
    @property
    def redis_url(self) -> str:
        """Generate Redis connection URL."""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    @property
    def postgres_url(self) -> str:
        """Generate PostgreSQL connection URL."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


# Global settings instance
settings = Settings()
