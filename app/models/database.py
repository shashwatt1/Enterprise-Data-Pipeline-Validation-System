"""
SQLAlchemy ORM models for the enterprise data pipeline.
Defines the raw data staging table for initial data ingestion.
"""

from sqlalchemy import Column, Integer, String, Text, DateTime
from datetime import datetime, timezone
from app.utils.db import Base


class RawData(Base):
    """Stores ingested raw records before transformation."""
    __tablename__ = "raw_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(String(64), nullable=False, index=True)
    entity_type = Column(String(50), nullable=False)
    row_index = Column(Integer, nullable=False)
    data_json = Column(Text, nullable=False)
    source_file = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
