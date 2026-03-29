"""
SQLAlchemy ORM models for the enterprise data pipeline.
Defines normalized tables for raw data, customers, transactions,
validation reports, and pipeline execution logs.
"""

from sqlalchemy import (
    Column, Integer, String, Float, Text, DateTime, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from app.utils.db import Base


class RawData(Base):
    """Stores ingested raw records before transformation."""
    __tablename__ = "raw_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(String(64), nullable=False, index=True)
    entity_type = Column(String(50), nullable=False)  # 'customers' or 'transactions'
    row_index = Column(Integer, nullable=False)
    data_json = Column(Text, nullable=False)  # JSON-serialized row
    source_file = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Customer(Base):
    """Normalized customer records after transformation and validation."""
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(String(64), nullable=False, index=True)
    customer_id = Column(Integer, nullable=False, unique=True)
    name = Column(String(200), nullable=False)
    email = Column(String(200), nullable=True)
    phone = Column(String(30), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    registration_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    transactions = relationship("Transaction", back_populates="customer")


class Transaction(Base):
    """Normalized transaction records linked to customers."""
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(String(64), nullable=False, index=True)
    transaction_id = Column(Integer, nullable=False, unique=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String(10), nullable=True, default="USD")
    transaction_date = Column(DateTime, nullable=True)
    category = Column(String(100), nullable=True)
    status = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    customer = relationship("Customer", back_populates="transactions")

    __table_args__ = (
        Index("ix_txn_customer", "customer_id"),
        Index("ix_txn_date", "transaction_date"),
    )


class ValidationReport(Base):
    """Stores individual validation check results per pipeline run."""
    __tablename__ = "validation_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(String(64), nullable=False, index=True)
    entity_type = Column(String(50), nullable=False)
    check_name = Column(String(100), nullable=False)
    status = Column(String(10), nullable=False)  # PASS or FAIL
    details = Column(Text, nullable=True)
    affected_rows = Column(Integer, default=0)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class PipelineLog(Base):
    """Tracks each step of a pipeline execution."""
    __tablename__ = "pipeline_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(String(64), nullable=False, index=True)
    step = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)  # STARTED, COMPLETED, FAILED
    message = Column(Text, nullable=True)
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
