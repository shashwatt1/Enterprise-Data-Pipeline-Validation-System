"""
Data Retrieval & Query API Routes
Provides endpoints for paginated data access, joins, and aggregations.
"""

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.schemas.api_schemas import (
    PaginatedResponse,
    CustomerTransactionsResponse,
    SpendByCustomerItem,
    MonthlySummaryItem,
    CategoryBreakdownItem,
)
from app.services.query_service import (
    get_customers,
    get_transactions,
    get_customer_with_transactions,
    get_spend_by_customer,
    get_monthly_summary,
    get_category_breakdown,
)
from app.utils.db import get_db

router = APIRouter(prefix="/api/v1/data", tags=["Data"])


@router.get("/customers", response_model=PaginatedResponse)
def list_customers(
    skip: int = Query(0, ge=0, description="Records to skip"),
    limit: int = Query(50, ge=1, le=500, description="Max records to return"),
    state: Optional[str] = Query(None, description="Filter by state (e.g., CA, TX)"),
    city: Optional[str] = Query(None, description="Filter by city (partial match)"),
    db: Session = Depends(get_db),
):
    """Retrieve paginated customer records with optional filters."""
    result = get_customers(db, skip=skip, limit=limit, state=state, city=city)
    return PaginatedResponse(**result)


@router.get("/transactions", response_model=PaginatedResponse)
def list_transactions(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    status: Optional[str] = Query(None, description="Filter by status"),
    category: Optional[str] = Query(None, description="Filter by category"),
    min_amount: Optional[float] = Query(None, description="Minimum amount"),
    max_amount: Optional[float] = Query(None, description="Maximum amount"),
    db: Session = Depends(get_db),
):
    """Retrieve paginated transaction records with optional filters."""
    result = get_transactions(
        db,
        skip=skip,
        limit=limit,
        status=status,
        category=category,
        min_amount=min_amount,
        max_amount=max_amount,
    )
    return PaginatedResponse(**result)


@router.get("/customer-transactions/{customer_id}")
def customer_transactions(
    customer_id: int,
    db: Session = Depends(get_db),
):
    """
    JOIN query: Get a customer's details along with all their transactions.
    Demonstrates relational data access patterns.
    """
    result = get_customer_with_transactions(db, customer_id)
    if "error" in result:
        return result  # customer not found
    return CustomerTransactionsResponse(**result)


@router.get(
    "/aggregations/spend-by-customer",
    response_model=list[SpendByCustomerItem],
)
def spend_by_customer(
    limit: int = Query(20, ge=1, le=100, description="Top N customers"),
    db: Session = Depends(get_db),
):
    """
    AGGREGATION: Top customers by total spend.
    Uses GROUP BY with SUM and COUNT across a customer-transaction join.
    """
    return get_spend_by_customer(db, limit=limit)


@router.get(
    "/aggregations/monthly-summary",
    response_model=list[MonthlySummaryItem],
)
def monthly_summary(db: Session = Depends(get_db)):
    """
    AGGREGATION: Monthly transaction volume and total amount.
    Groups by year-month for time-series trend analysis.
    """
    return get_monthly_summary(db)


@router.get(
    "/aggregations/category-breakdown",
    response_model=list[CategoryBreakdownItem],
)
def category_breakdown(db: Session = Depends(get_db)):
    """
    AGGREGATION: Spend breakdown by transaction category.
    """
    return get_category_breakdown(db)
