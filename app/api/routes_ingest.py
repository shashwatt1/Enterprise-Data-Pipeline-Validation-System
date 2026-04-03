"""
Data Ingestion API Routes
Handles CSV file uploads and JSON data submission.
"""

import os
import tempfile
import shutil

from fastapi import APIRouter, Depends, File, Form, UploadFile, HTTPException
from sqlalchemy.orm import Session

from app.schemas.api_schemas import IngestJSONRequest, IngestResponse
from app.services.ingestion import ingest_csv, ingest_json
from app.utils.db import get_db

router = APIRouter(prefix="/api/v1/ingest", tags=["Ingestion"])


@router.post("/csv", response_model=IngestResponse)
async def upload_csv(
    file: UploadFile = File(..., description="CSV file to ingest"),
    entity_type: str = Form(
        ..., description="Entity type: 'customers' or 'transactions'"
    ),
    db: Session = Depends(get_db),
):
    """
    Upload a CSV file for ingestion into the pipeline.
    The file is temporarily saved, parsed, and stored as raw data.
    """
    if entity_type not in ("customers", "transactions"):
        raise HTTPException(
            status_code=400,
            detail="entity_type must be 'customers' or 'transactions'",
        )

    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

    # Save uploaded file to a temp location
    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, file.filename)
    try:
        with open(tmp_path, "wb") as f:
            content = await file.read()
            f.write(content)

        result = ingest_csv(db, tmp_path, entity_type)
        return IngestResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@router.post("/json", response_model=IngestResponse)
def submit_json(
    payload: IngestJSONRequest,
    db: Session = Depends(get_db),
):
    """
    Submit JSON records for ingestion into the pipeline.
    Accepts a list of record dicts matching the source schema.
    """
    if payload.entity_type not in ("customers", "transactions"):
        raise HTTPException(
            status_code=400,
            detail="entity_type must be 'customers' or 'transactions'",
        )

    if not payload.records:
        raise HTTPException(status_code=400, detail="No records provided")

    try:
        result = ingest_json(db, payload.records, payload.entity_type)
        return IngestResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
