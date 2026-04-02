"""
Pydantic request/response models for API endpoints.
Provides type-safe serialization and OpenAPI documentation.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# --- Ingestion ---

class IngestJSONRequest(BaseModel):
    """Request body for JSON data ingestion."""
    entity_type: str = Field(
        ..., description="Entity type: 'customers' or 'transactions'"
    )
    records: List[Dict[str, Any]] = Field(
        ..., description="List of record dicts matching the source schema"
    )


class IngestResponse(BaseModel):
    """Response after successful data ingestion."""
    pipeline_run_id: str
    entity_type: str
    rows_ingested: int
    source: str


# --- Health ---

class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str
    database: str
