from __future__ import annotations

"""
Query Service
Pre-built SQL queries for data retrieval, joins, and aggregations.
Powers the data-access API endpoints.
"""

from typing import Optional

from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from app.models.database import Customer, Transaction


def get_customers(
    db: Session,
    skip: int = 0,
    limit: int = 50,
    state: Optional[str] = None,
    city: Optional[str] = None,
) -> dict:
    """Retrieve paginated customer records with optional filters."""
    query = db.query(Customer)

    if state:
        query = query.filter(Customer.state == state.upper())
    if city:
        query = query.filter(Customer.city.ilike(f"%{city}%"))

    total = query.count()
    customers = query.order_by(Customer.customer_id).offset(skip).limit(limit).all()

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": [_customer_to_dict(c) for c in customers],
    }


def get_transactions(
    db: Session,
    skip: int = 0,
    limit: int = 50,
    status: Optional[str] = None,
    category: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
) -> dict:
    """Retrieve paginated transaction records with filters."""
    query = db.query(Transaction)

    if status:
        query = query.filter(Transaction.status == status.lower())
    if category:
        query = query.filter(Transaction.category.ilike(f"%{category}%"))
    if min_amount is not None:
        query = query.filter(Transaction.amount >= min_amount)
    if max_amount is not None:
        query = query.filter(Transaction.amount <= max_amount)

    total = query.count()
    transactions = (
        query.order_by(Transaction.transaction_id).offset(skip).limit(limit).all()
    )

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": [_transaction_to_dict(t) for t in transactions],
    }


def get_customer_with_transactions(db: Session, customer_id: int) -> dict:
    """
    JOIN query: fetch a customer along with all their transactions.
    Demonstrates a relational join for the BA/DE portfolio.
    """
    customer = (
        db.query(Customer).filter(Customer.customer_id == customer_id).first()
    )
    if not customer:
        return {"error": f"Customer {customer_id} not found"}

    transactions = (
        db.query(Transaction)
        .filter(Transaction.customer_id == customer_id)
        .order_by(Transaction.transaction_date)
        .all()
    )

    return {
        "customer": _customer_to_dict(customer),
        "transactions": [_transaction_to_dict(t) for t in transactions],
        "transaction_count": len(transactions),
        "total_spend": sum(t.amount for t in transactions if t.amount),
    }


def get_spend_by_customer(db: Session, limit: int = 20) -> list[dict]:
    """
    AGGREGATION: Total spend per customer, ordered by highest spend.
    SQL equivalent:
        SELECT c.customer_id, c.name, SUM(t.amount) as total_spend,
               COUNT(t.id) as txn_count
        FROM customers c JOIN transactions t ON c.customer_id = t.customer_id
        GROUP BY c.customer_id, c.name
        ORDER BY total_spend DESC
    """
    results = (
        db.query(
            Customer.customer_id,
            Customer.name,
            func.sum(Transaction.amount).label("total_spend"),
            func.count(Transaction.id).label("txn_count"),
        )
        .join(Transaction, Customer.customer_id == Transaction.customer_id)
        .group_by(Customer.customer_id, Customer.name)
        .order_by(desc("total_spend"))
        .limit(limit)
        .all()
    )

    return [
        {
            "customer_id": r.customer_id,
            "name": r.name,
            "total_spend": round(float(r.total_spend), 2) if r.total_spend else 0,
            "transaction_count": r.txn_count,
        }
        for r in results
    ]


def get_monthly_summary(db: Session) -> list[dict]:
    """
    AGGREGATION: Monthly transaction volume and total amount.
    Groups transactions by year-month for trend analysis.

    SQL equivalent:
        SELECT strftime('%Y-%m', transaction_date) as month,
               COUNT(*) as txn_count, SUM(amount) as total_amount,
               AVG(amount) as avg_amount
        FROM transactions
        WHERE transaction_date IS NOT NULL
        GROUP BY month ORDER BY month
    """
    # Using func.strftime for SQLite; for PostgreSQL use func.to_char
    results = (
        db.query(
            func.strftime("%Y-%m", Transaction.transaction_date).label("month"),
            func.count(Transaction.id).label("txn_count"),
            func.sum(Transaction.amount).label("total_amount"),
            func.avg(Transaction.amount).label("avg_amount"),
        )
        .filter(Transaction.transaction_date.isnot(None))
        .group_by("month")
        .order_by("month")
        .all()
    )

    return [
        {
            "month": r.month,
            "transaction_count": r.txn_count,
            "total_amount": round(float(r.total_amount), 2) if r.total_amount else 0,
            "avg_amount": round(float(r.avg_amount), 2) if r.avg_amount else 0,
        }
        for r in results
    ]


def get_category_breakdown(db: Session) -> list[dict]:
    """
    AGGREGATION: Spend breakdown by transaction category.
    """
    results = (
        db.query(
            Transaction.category,
            func.count(Transaction.id).label("txn_count"),
            func.sum(Transaction.amount).label("total_amount"),
        )
        .filter(Transaction.category.isnot(None))
        .group_by(Transaction.category)
        .order_by(desc("total_amount"))
        .all()
    )

    return [
        {
            "category": r.category or "Uncategorized",
            "transaction_count": r.txn_count,
            "total_amount": round(float(r.total_amount), 2) if r.total_amount else 0,
        }
        for r in results
    ]


# --- Helper serializers ---

def _customer_to_dict(c: Customer) -> dict:
    return {
        "customer_id": c.customer_id,
        "name": c.name,
        "email": c.email,
        "phone": c.phone,
        "city": c.city,
        "state": c.state,
        "registration_date": (
            c.registration_date.isoformat() if c.registration_date else None
        ),
    }


def _transaction_to_dict(t: Transaction) -> dict:
    return {
        "transaction_id": t.transaction_id,
        "customer_id": t.customer_id,
        "amount": t.amount,
        "currency": t.currency,
        "transaction_date": (
            t.transaction_date.isoformat() if t.transaction_date else None
        ),
        "category": t.category,
        "status": t.status,
    }
