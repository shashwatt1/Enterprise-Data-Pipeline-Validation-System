from __future__ import annotations

"""
Data Validation Service
Implements configurable validation checks on transformed DataFrames:
  - Null checks
  - Uniqueness constraints
  - Range validation (numeric and date)
  - Pattern matching (regex)
  - Schema consistency
Generates a structured validation report per pipeline run.
"""

import re
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.models.database import ValidationReport
from app.services.schema_mapper import get_validation_rules, get_target_schema
from app.utils.logger import logger


def validate(
    df: pd.DataFrame,
    entity_type: str,
    pipeline_run_id: str,
    db: Optional[Session] = None,
) -> dict:
    """
    Run all configured validation checks on the DataFrame.

    Returns:
        {
            "overall_status": "PASS" | "FAIL",
            "checks": [ {check_name, status, details, affected_rows}, ... ],
            "total_checks": int,
            "passed": int,
            "failed": int,
        }
    """
    results = []

    # Schema consistency check
    results.append(_check_schema_consistency(df, entity_type))

    # Column-level validation rules from config
    rules = get_validation_rules(entity_type)
    for col_name, checks in rules.items():
        for check_def in checks:
            result = _run_check(df, col_name, check_def, entity_type)
            results.append(result)

    # Compute summary
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    overall = "PASS" if failed == 0 else "FAIL"

    report = {
        "overall_status": overall,
        "checks": results,
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
    }

    # Persist to database if a session is provided
    if db is not None:
        _persist_report(db, pipeline_run_id, entity_type, results)

    status_emoji = "✓" if overall == "PASS" else "✗"
    logger.info(
        f"[{pipeline_run_id}] Validation {status_emoji} {overall}: "
        f"{passed}/{len(results)} checks passed"
    )
    return report


def _check_schema_consistency(df: pd.DataFrame, entity_type: str) -> dict:
    """Verify that all required target columns are present in the DataFrame."""
    target_schema = get_target_schema(entity_type)
    expected = {col["name"] for col in target_schema}
    actual = set(df.columns)
    missing = expected - actual

    if missing:
        return {
            "check_name": f"schema_consistency_{entity_type}",
            "status": "FAIL",
            "details": f"Missing target columns: {sorted(missing)}",
            "affected_rows": len(df),
        }
    return {
        "check_name": f"schema_consistency_{entity_type}",
        "status": "PASS",
        "details": "All target columns present",
        "affected_rows": 0,
    }


def _run_check(
    df: pd.DataFrame, col_name: str, check_def: dict, entity_type: str
) -> dict:
    """Dispatch a single validation check."""
    check_type = check_def["check"]
    check_name = f"{col_name}_{check_type}"

    if col_name not in df.columns:
        return {
            "check_name": check_name,
            "status": "FAIL",
            "details": f"Column '{col_name}' not found in data",
            "affected_rows": len(df),
        }

    dispatch = {
        "not_null": _check_not_null,
        "unique": _check_unique,
        "range": _check_range,
        "date_range": _check_date_range,
        "pattern": _check_pattern,
    }

    handler = dispatch.get(check_type)
    if handler is None:
        return {
            "check_name": check_name,
            "status": "FAIL",
            "details": f"Unknown check type: {check_type}",
            "affected_rows": 0,
        }

    return handler(df, col_name, check_def, check_name)


# --- Individual validation check handlers ---

def _check_not_null(
    df: pd.DataFrame, col: str, check_def: dict, check_name: str
) -> dict:
    null_count = int(df[col].isna().sum())
    if null_count > 0:
        return {
            "check_name": check_name,
            "status": "FAIL",
            "details": f"{null_count} null values found in '{col}'",
            "affected_rows": null_count,
        }
    return {
        "check_name": check_name,
        "status": "PASS",
        "details": f"No nulls in '{col}'",
        "affected_rows": 0,
    }


def _check_unique(
    df: pd.DataFrame, col: str, check_def: dict, check_name: str
) -> dict:
    # Only check non-null values for uniqueness
    non_null = df[col].dropna()
    dup_count = int(non_null.duplicated().sum())
    if dup_count > 0:
        return {
            "check_name": check_name,
            "status": "FAIL",
            "details": f"{dup_count} duplicate values found in '{col}'",
            "affected_rows": dup_count,
        }
    return {
        "check_name": check_name,
        "status": "PASS",
        "details": f"All values unique in '{col}'",
        "affected_rows": 0,
    }


def _check_range(
    df: pd.DataFrame, col: str, check_def: dict, check_name: str
) -> dict:
    min_val = check_def.get("min")
    max_val = check_def.get("max")
    series = pd.to_numeric(df[col], errors="coerce")

    violations = pd.Series([False] * len(series))
    if min_val is not None:
        violations = violations | (series < min_val)
    if max_val is not None:
        violations = violations | (series > max_val)

    # Exclude NaN from violation counts (nulls caught by not_null check)
    violations = violations & series.notna()
    violation_count = int(violations.sum())

    if violation_count > 0:
        return {
            "check_name": check_name,
            "status": "FAIL",
            "details": (
                f"{violation_count} values in '{col}' outside range "
                f"[{min_val}, {max_val}]"
            ),
            "affected_rows": violation_count,
        }
    return {
        "check_name": check_name,
        "status": "PASS",
        "details": f"All values in '{col}' within range [{min_val}, {max_val}]",
        "affected_rows": 0,
    }


def _check_date_range(
    df: pd.DataFrame, col: str, check_def: dict, check_name: str
) -> dict:
    min_date = pd.to_datetime(check_def.get("min"), errors="coerce")
    max_date = pd.to_datetime(check_def.get("max"), errors="coerce")
    dates = pd.to_datetime(df[col], errors="coerce")

    violations = pd.Series([False] * len(dates))
    if min_date is not None:
        violations = violations | (dates < min_date)
    if max_date is not None:
        violations = violations | (dates > max_date)

    violations = violations & dates.notna()
    violation_count = int(violations.sum())

    if violation_count > 0:
        return {
            "check_name": check_name,
            "status": "FAIL",
            "details": (
                f"{violation_count} dates in '{col}' outside range "
                f"[{check_def.get('min')}, {check_def.get('max')}]"
            ),
            "affected_rows": violation_count,
        }
    return {
        "check_name": check_name,
        "status": "PASS",
        "details": f"All dates in '{col}' within valid range",
        "affected_rows": 0,
    }


def _check_pattern(
    df: pd.DataFrame, col: str, check_def: dict, check_name: str
) -> dict:
    pattern = check_def.get("regex", "")
    # Only check non-null string values
    non_null = df[col].dropna().astype(str)
    mismatches = non_null[~non_null.str.match(pattern, na=False)]
    mismatch_count = len(mismatches)

    if mismatch_count > 0:
        sample = list(mismatches.head(3))
        return {
            "check_name": check_name,
            "status": "FAIL",
            "details": (
                f"{mismatch_count} values in '{col}' don't match pattern. "
                f"Samples: {sample}"
            ),
            "affected_rows": mismatch_count,
        }
    return {
        "check_name": check_name,
        "status": "PASS",
        "details": f"All values in '{col}' match pattern",
        "affected_rows": 0,
    }


def _persist_report(
    db: Session,
    pipeline_run_id: str,
    entity_type: str,
    results: list[dict],
):
    """Store validation results in the database."""
    for r in results:
        entry = ValidationReport(
            pipeline_run_id=pipeline_run_id,
            entity_type=entity_type,
            check_name=r["check_name"],
            status=r["status"],
            details=r.get("details", ""),
            affected_rows=r.get("affected_rows", 0),
        )
        db.add(entry)
    db.commit()
