"""
Pipeline Management API Routes
Trigger pipelines, check status, and retrieve execution logs.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.models.database import PipelineLog
from app.schemas.api_schemas import (
    PipelineRunResponse,
    PipelineLogEntry,
    PipelineLogsResponse,
)
from app.services.pipeline import run_pipeline
from app.utils.db import get_db

router = APIRouter(prefix="/api/v1/pipeline", tags=["Pipeline"])


@router.post("/run/{entity_type}", response_model=PipelineRunResponse)
def trigger_pipeline(
    entity_type: str,
    pipeline_run_id: str = None,
    db: Session = Depends(get_db),
):
    """
    Trigger the data processing pipeline for a specific entity type.
    Requires that data has already been ingested via /api/v1/ingest/*
    with the returned pipeline_run_id.
    """
    if entity_type not in ("customers", "transactions"):
        raise HTTPException(
            status_code=400,
            detail="entity_type must be 'customers' or 'transactions'",
        )

    if not pipeline_run_id:
        raise HTTPException(
            status_code=400,
            detail="pipeline_run_id query parameter is required. "
                   "Use the ID returned from the ingestion step.",
        )

    try:
        result = run_pipeline(db, pipeline_run_id, entity_type)
        return PipelineRunResponse(**result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/status/{run_id}", response_model=PipelineRunResponse)
def get_pipeline_status(run_id: str, db: Session = Depends(get_db)):
    """
    Get the current status of a pipeline run by checking its log entries.
    """
    logs = (
        db.query(PipelineLog)
        .filter(PipelineLog.pipeline_run_id == run_id)
        .order_by(PipelineLog.timestamp)
        .all()
    )

    if not logs:
        raise HTTPException(
            status_code=404, detail=f"No pipeline run found with ID: {run_id}"
        )

    # Determine overall status from the last log entry
    last_log = logs[-1]
    if last_log.status == "FAILED":
        overall_status = "FAILED"
    elif last_log.step == "STORAGE" and last_log.status == "COMPLETED":
        overall_status = "COMPLETED"
    else:
        overall_status = "IN_PROGRESS"

    steps = {}
    for log in logs:
        steps[log.step] = {
            "status": log.status,
            "message": log.message,
            "records_processed": log.records_processed,
            "records_failed": log.records_failed,
        }

    return PipelineRunResponse(
        pipeline_run_id=run_id,
        entity_type=logs[0].step,  # Best effort; entity not stored in logs
        status=overall_status,
        steps=steps,
    )


@router.get("/logs/{run_id}", response_model=PipelineLogsResponse)
def get_pipeline_logs(run_id: str, db: Session = Depends(get_db)):
    """
    Retrieve all log entries for a specific pipeline run.
    """
    logs = (
        db.query(PipelineLog)
        .filter(PipelineLog.pipeline_run_id == run_id)
        .order_by(PipelineLog.timestamp)
        .all()
    )

    if not logs:
        raise HTTPException(
            status_code=404, detail=f"No logs found for run ID: {run_id}"
        )

    entries = [
        PipelineLogEntry(
            step=log.step,
            status=log.status,
            message=log.message,
            records_processed=log.records_processed,
            records_failed=log.records_failed,
            timestamp=log.timestamp,
        )
        for log in logs
    ]

    return PipelineLogsResponse(pipeline_run_id=run_id, logs=entries)
