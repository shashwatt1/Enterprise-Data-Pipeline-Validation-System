"""
Application configuration loaded from environment variables.
Uses pydantic-settings for validation and type coercion.
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database
    database_url: str = "sqlite:///./pipeline.db"

    # Logging
    log_level: str = "INFO"

    # Optional: Google BigQuery
    bigquery_project_id: Optional[str] = None
    bigquery_dataset: Optional[str] = None
    bigquery_credentials_path: Optional[str] = None

    # App metadata
    app_name: str = "Enterprise Data Pipeline"
    app_version: str = "1.0.0"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
