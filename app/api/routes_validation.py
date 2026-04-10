"""
Validation Report API Routes
Provides access to validation results per pipeline run.
"""

from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import case, func
from sqlalchemy.orm import Session

from app.models.database import ValidationReport
from app.schemas.api_schemas import (
    ValidationReportResponse,
    ValidationCheckResult,
    ValidationSummaryItem,
)
from app.utils.db import get_db

router = APIRouter(prefix="/api/v1/validation", tags=["Validation"])


@router.get("/report/{run_id}", response_model=ValidationReportResponse)
def get_validation_report(run_id: str, db: Session = Depends(get_db)):
    """
    Retrieve the full validation report for a specific pipeline run.
    Shows each check with pass/fail status, details, and affected rows.
    """
    reports = (
        db.query(ValidationReport)
        .filter(ValidationReport.pipeline_run_id == run_id)
        .order_by(ValidationReport.id)
        .all()
    )

    if not reports:
        raise HTTPException(
            status_code=404,
            detail=f"No validation report found for run ID: {run_id}",
        )

    checks = [
        ValidationCheckResult(
            check_name=r.check_name,
            status=r.status,
            details=r.details,
            affected_rows=r.affected_rows,
        )
        for r in reports
    ]

    passed = sum(1 for c in checks if c.status == "PASS")
    failed = sum(1 for c in checks if c.status == "FAIL")
    overall = "PASS" if failed == 0 else "FAIL"

    return ValidationReportResponse(
        pipeline_run_id=run_id,
        entity_type=reports[0].entity_type,
        overall_status=overall,
        total_checks=len(checks),
        passed=passed,
        failed=failed,
        checks=checks,
    )


@router.get("/summary", response_model=List[ValidationSummaryItem])
def get_validation_summary(db: Session = Depends(get_db)):
    """
    Get a summary of all validation runs: pass/fail counts per pipeline run.
    """
    # Group by pipeline_run_id and entity_type
    results = (
        db.query(
            ValidationReport.pipeline_run_id,
            ValidationReport.entity_type,
            func.count(ValidationReport.id).label("total_checks"),
            func.sum(
                case(
                    (ValidationReport.status == "PASS", 1), else_=0
                )
            ).label("passed"),
            func.sum(
                case(
                    (ValidationReport.status == "FAIL", 1), else_=0
                )
            ).label("failed"),
            func.max(ValidationReport.timestamp).label("timestamp"),
        )
        .group_by(
            ValidationReport.pipeline_run_id,
            ValidationReport.entity_type,
        )
        .order_by(func.max(ValidationReport.timestamp).desc())
        .all()
    )

    return [
        ValidationSummaryItem(
            pipeline_run_id=r.pipeline_run_id,
            entity_type=r.entity_type,
            total_checks=r.total_checks,
            passed=int(r.passed),
            failed=int(r.failed),
            timestamp=r.timestamp,
        )
        for r in results
    ]
