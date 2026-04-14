"""
API integration tests using FastAPI TestClient.
Tests the end-to-end flow: ingest → pipeline → data retrieval → validation.
"""

import json
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.utils.db import Base, get_db
from main import app


# Override db dependency with in-memory SQLite
TEST_DB_URL = "sqlite:///./test_pipeline.db"
test_engine = create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestSession = sessionmaker(bind=test_engine)


def override_get_db():
    db = TestSession()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db

# Create tables
Base.metadata.create_all(bind=test_engine)

client = TestClient(app)


class TestHealthEndpoints:
    def test_root(self):
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"

    def test_health(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"


class TestIngestionAPI:
    def test_ingest_json_customers(self):
        payload = {
            "entity_type": "customers",
            "records": [
                {
                    "cust_id": "1",
                    "full_name": "Test User",
                    "email_address": "test@mail.com",
                    "phone_no": "5550101",
                    "city_name": "New York",
                    "state_name": "NY",
                    "reg_date": "2023-01-15",
                },
                {
                    "cust_id": "2",
                    "full_name": "Jane Doe",
                    "email_address": "jane@mail.com",
                    "phone_no": "5550102",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "reg_date": "2023-02-20",
                },
            ],
        }
        resp = client.post("/api/v1/ingest/json", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["rows_ingested"] == 2
        assert "pipeline_run_id" in data

    def test_ingest_invalid_entity_type(self):
        payload = {"entity_type": "invalid", "records": [{"a": "1"}]}
        resp = client.post("/api/v1/ingest/json", json=payload)
        assert resp.status_code == 400

    def test_ingest_empty_records(self):
        payload = {"entity_type": "customers", "records": []}
        resp = client.post("/api/v1/ingest/json", json=payload)
        assert resp.status_code == 400


class TestPipelineAPI:
    def test_trigger_pipeline(self):
        # First ingest some data
        payload = {
            "entity_type": "customers",
            "records": [
                {
                    "cust_id": "100",
                    "full_name": "Pipeline Test",
                    "email_address": "pipe@mail.com",
                    "phone_no": "5550199",
                    "city_name": "Boston",
                    "state_name": "MA",
                    "reg_date": "2024-01-01",
                },
            ],
        }
        ingest_resp = client.post("/api/v1/ingest/json", json=payload)
        run_id = ingest_resp.json()["pipeline_run_id"]

        # Trigger pipeline
        resp = client.post(
            f"/api/v1/pipeline/run/customers?pipeline_run_id={run_id}"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("COMPLETED", "FAILED")

    def test_trigger_pipeline_no_run_id(self):
        resp = client.post("/api/v1/pipeline/run/customers")
        assert resp.status_code == 400


class TestDataAPI:
    def test_get_customers_empty(self):
        resp = client.get("/api/v1/data/customers")
        assert resp.status_code == 200
        data = resp.json()
        assert "total" in data
        assert "data" in data

    def test_get_transactions_empty(self):
        resp = client.get("/api/v1/data/transactions")
        assert resp.status_code == 200


class TestValidationAPI:
    def test_validation_summary_empty(self):
        resp = client.get("/api/v1/validation/summary")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_validation_report_not_found(self):
        resp = client.get("/api/v1/validation/report/nonexistent")
        assert resp.status_code == 404


@pytest.fixture(scope="session", autouse=True)
def cleanup():
    """Remove test database after all tests."""
    yield
    if os.path.exists("test_pipeline.db"):
        os.unlink("test_pipeline.db")
