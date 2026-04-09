"""
Pydantic request/response models for all API endpoints.
Provides type-safe serialization and OpenAPI documentation.
"""

from datetime import datetime
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


# --- Pipeline ---

class PipelineRunResponse(BaseModel):
    """Response after triggering a pipeline run."""
    pipeline_run_id: str
    entity_type: str
    status: str
    steps: Dict[str, Any] = {}
    error: Optional[str] = None


class PipelineLogEntry(BaseModel):
    """A single pipeline log entry."""
    step: str
    status: str
    message: Optional[str] = None
    records_processed: int = 0
    records_failed: int = 0
    timestamp: Optional[datetime] = None


class PipelineLogsResponse(BaseModel):
    """All log entries for a pipeline run."""
    pipeline_run_id: str
    logs: List[PipelineLogEntry]


# --- Data Retrieval ---

class CustomerResponse(BaseModel):
    customer_id: int
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    registration_date: Optional[str] = None


class TransactionResponse(BaseModel):
    transaction_id: int
    customer_id: int
    amount: float
    currency: Optional[str] = None
    transaction_date: Optional[str] = None
    category: Optional[str] = None
    status: Optional[str] = None


class PaginatedResponse(BaseModel):
    """Generic paginated response wrapper."""
    total: int
    skip: int
    limit: int
    data: List[Dict[str, Any]]


class CustomerTransactionsResponse(BaseModel):
    """Customer with their transactions (join result)."""
    customer: Dict[str, Any]
    transactions: List[Dict[str, Any]]
    transaction_count: int
    total_spend: float


# --- Aggregations ---

class SpendByCustomerItem(BaseModel):
    customer_id: int
    name: str
    total_spend: float
    transaction_count: int


class MonthlySummaryItem(BaseModel):
    month: str
    transaction_count: int
    total_amount: float
    avg_amount: float


class CategoryBreakdownItem(BaseModel):
    category: str
    transaction_count: int
    total_amount: float


# --- Validation ---

class ValidationCheckResult(BaseModel):
    check_name: str
    status: str
    details: Optional[str] = None
    affected_rows: int = 0


class ValidationReportResponse(BaseModel):
    pipeline_run_id: str
    entity_type: Optional[str] = None
    overall_status: str
    total_checks: int
    passed: int
    failed: int
    checks: List[ValidationCheckResult]


class ValidationSummaryItem(BaseModel):
    pipeline_run_id: str
    entity_type: str
    total_checks: int
    passed: int
    failed: int
    timestamp: Optional[datetime] = None


# --- Health ---

class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str
    database: str
