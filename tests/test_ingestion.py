"""
Tests for the data ingestion service.
"""

import json
import os
import tempfile

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.utils.db import Base
from app.models.database import RawData
from app.services.ingestion import (
    ingest_csv,
    ingest_json,
    load_raw_data_as_dataframe,
    generate_run_id,
)


@pytest.fixture
def db_session():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file with sample data."""
    content = (
        "cust_id,full_name,email_address,phone_no,city_name,state_name,reg_date\n"
        "1,Alice Johnson,alice@mail.com,5550101,New York,NY,2023-01-15\n"
        "2,Bob Smith,bob@mail.com,5550102,Los Angeles,CA,2023-02-20\n"
        "3,Charlie Brown,charlie@mail.com,5550103,Chicago,IL,2023-03-10\n"
    )
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False
    ) as f:
        f.write(content)
        return f.name


class TestIngestion:
    def test_generate_run_id(self):
        rid = generate_run_id()
        assert len(rid) == 12
        assert isinstance(rid, str)

    def test_ingest_csv(self, db_session, sample_csv_file):
        result = ingest_csv(db_session, sample_csv_file, "customers")

        assert result["entity_type"] == "customers"
        assert result["rows_ingested"] == 3
        assert "pipeline_run_id" in result

        # Verify raw data in DB
        count = db_session.query(RawData).count()
        assert count == 3

        os.unlink(sample_csv_file)

    def test_ingest_json(self, db_session):
        records = [
            {"cust_id": "10", "full_name": "Test User", "email_address": "t@m.com"},
            {"cust_id": "11", "full_name": "Test Two", "email_address": "t2@m.com"},
        ]
        result = ingest_json(db_session, records, "customers")

        assert result["rows_ingested"] == 2
        assert result["source"] == "api"

    def test_load_raw_data_as_dataframe(self, db_session, sample_csv_file):
        result = ingest_csv(db_session, sample_csv_file, "customers")
        run_id = result["pipeline_run_id"]

        df = load_raw_data_as_dataframe(db_session, run_id, "customers")
        assert len(df) == 3
        assert "cust_id" in df.columns

        os.unlink(sample_csv_file)

    def test_ingest_csv_invalid_file(self, db_session):
        with pytest.raises(ValueError, match="Failed to read CSV"):
            ingest_csv(db_session, "/nonexistent/file.csv", "customers")

    def test_load_raw_data_no_data(self, db_session):
        with pytest.raises(ValueError, match="No raw data found"):
            load_raw_data_as_dataframe(db_session, "nonexistent", "customers")
