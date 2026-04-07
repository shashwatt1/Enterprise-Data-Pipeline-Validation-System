from __future__ import annotations

"""
Pipeline Orchestrator
Coordinates the end-to-end data processing flow:
  1. Load raw data
  2. Validate source schema
  3. Map columns (source → target)
  4. Apply transformations
  5. Run validation checks
  6. Store cleaned data to normalized tables
Each step is logged to the pipeline_logs table.
"""

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.models.database import Customer, Transaction
from app.services.ingestion import load_raw_data_as_dataframe
from app.services.schema_mapper import validate_source_schema, map_columns
from app.services.transformer import transform
from app.services.validator import validate
from app.utils.logger import log_pipeline_step, logger


def run_pipeline(
    db: Session, pipeline_run_id: str, entity_type: str
) -> dict:
    """
    Execute the full pipeline for a given entity type and run ID.

    Returns a summary dict with status, stats, and validation report.
    """
    summary = {
        "pipeline_run_id": pipeline_run_id,
        "entity_type": entity_type,
        "status": "STARTED",
        "steps": {},
    }

    try:
        # --- Step 1: Load raw data ---
        log_pipeline_step(
            db, pipeline_run_id, "LOAD_RAW", "STARTED",
            message="Loading raw data from staging table"
        )
        df = load_raw_data_as_dataframe(db, pipeline_run_id, entity_type)
        log_pipeline_step(
            db, pipeline_run_id, "LOAD_RAW", "COMPLETED",
            records_processed=len(df)
        )
        summary["steps"]["load_raw"] = {"rows": len(df)}

        # --- Step 2: Validate source schema ---
        log_pipeline_step(
            db, pipeline_run_id, "SCHEMA_VALIDATION", "STARTED"
        )
        schema_result = validate_source_schema(df, entity_type)
        if not schema_result["valid"]:
            log_pipeline_step(
                db, pipeline_run_id, "SCHEMA_VALIDATION", "FAILED",
                message=f"Missing columns: {schema_result['missing_columns']}"
            )
            summary["status"] = "FAILED"
            summary["steps"]["schema_validation"] = schema_result
            return summary

        log_pipeline_step(
            db, pipeline_run_id, "SCHEMA_VALIDATION", "COMPLETED"
        )
        summary["steps"]["schema_validation"] = schema_result

        # --- Step 3: Map columns ---
        log_pipeline_step(
            db, pipeline_run_id, "COLUMN_MAPPING", "STARTED"
        )
        df_mapped = map_columns(df, entity_type)
        log_pipeline_step(
            db, pipeline_run_id, "COLUMN_MAPPING", "COMPLETED",
            records_processed=len(df_mapped)
        )
        summary["steps"]["column_mapping"] = {"columns": list(df_mapped.columns)}

        # --- Step 4: Transform ---
        log_pipeline_step(
            db, pipeline_run_id, "TRANSFORMATION", "STARTED"
        )
        df_clean, transform_stats = transform(df_mapped, entity_type, pipeline_run_id)
        log_pipeline_step(
            db, pipeline_run_id, "TRANSFORMATION", "COMPLETED",
            records_processed=transform_stats["rows_after_cleaning"],
            records_failed=transform_stats["duplicates_removed"],
            message=f"Stats: {transform_stats}",
        )
        summary["steps"]["transformation"] = transform_stats

        # --- Step 5: Validate ---
        log_pipeline_step(
            db, pipeline_run_id, "VALIDATION", "STARTED"
        )
        validation_report = validate(df_clean, entity_type, pipeline_run_id, db)
        log_pipeline_step(
            db, pipeline_run_id, "VALIDATION", "COMPLETED",
            message=f"Overall: {validation_report['overall_status']} "
                    f"({validation_report['passed']}/{validation_report['total_checks']})"
        )
        summary["steps"]["validation"] = validation_report

        # --- Step 6: Store to normalized tables ---
        log_pipeline_step(
            db, pipeline_run_id, "STORAGE", "STARTED",
            message="Writing to normalized tables"
        )
        stored_count = _store_normalized(db, df_clean, entity_type, pipeline_run_id)
        log_pipeline_step(
            db, pipeline_run_id, "STORAGE", "COMPLETED",
            records_processed=stored_count,
        )
        summary["steps"]["storage"] = {"rows_stored": stored_count}

        summary["status"] = "COMPLETED"
        logger.info(
            f"[{pipeline_run_id}] Pipeline completed successfully. "
            f"Stored {stored_count} records."
        )

    except Exception as exc:
        log_pipeline_step(
            db, pipeline_run_id, "PIPELINE_ERROR", "FAILED",
            message=str(exc)
        )
        summary["status"] = "FAILED"
        summary["error"] = str(exc)
        logger.error(f"[{pipeline_run_id}] Pipeline failed: {exc}")

    return summary


def _store_normalized(
    db: Session,
    df: pd.DataFrame,
    entity_type: str,
    pipeline_run_id: str,
) -> int:
    """Insert cleaned records into the appropriate normalized table."""
    if entity_type == "customers":
        return _store_customers(db, df, pipeline_run_id)
    elif entity_type == "transactions":
        return _store_transactions(db, df, pipeline_run_id)
    else:
        raise ValueError(f"Unknown entity type for storage: {entity_type}")


def _store_customers(
    db: Session, df: pd.DataFrame, pipeline_run_id: str
) -> int:
    """Insert customer records, skipping rows with duplicate customer_id."""
    stored = 0
    for _, row in df.iterrows():
        try:
            cid = int(row["customer_id"]) if pd.notna(row.get("customer_id")) else None
            if cid is None:
                continue

            # Skip if this customer_id already exists
            existing = db.query(Customer).filter(
                Customer.customer_id == cid
            ).first()
            if existing:
                continue

            customer = Customer(
                pipeline_run_id=pipeline_run_id,
                customer_id=cid,
                name=_safe_str(row.get("name")),
                email=_safe_str(row.get("email")),
                phone=_safe_str(row.get("phone")),
                city=_safe_str(row.get("city")),
                state=_safe_str(row.get("state")),
                registration_date=_safe_datetime(row.get("registration_date")),
            )
            db.add(customer)
            stored += 1
        except Exception as exc:
            logger.warning(f"Skipping customer row: {exc}")
            continue

    db.commit()
    return stored


def _store_transactions(
    db: Session, df: pd.DataFrame, pipeline_run_id: str
) -> int:
    """Insert transaction records, skipping duplicates."""
    stored = 0
    for _, row in df.iterrows():
        try:
            tid = (
                int(row["transaction_id"])
                if pd.notna(row.get("transaction_id"))
                else None
            )
            if tid is None:
                continue

            existing = db.query(Transaction).filter(
                Transaction.transaction_id == tid
            ).first()
            if existing:
                continue

            txn = Transaction(
                pipeline_run_id=pipeline_run_id,
                transaction_id=tid,
                customer_id=int(row["customer_id"]),
                amount=float(row["amount"]) if pd.notna(row.get("amount")) else 0.0,
                currency=_safe_str(row.get("currency")),
                transaction_date=_safe_datetime(row.get("transaction_date")),
                category=_safe_str(row.get("category")),
                status=_safe_str(row.get("status")),
            )
            db.add(txn)
            stored += 1
        except Exception as exc:
            logger.warning(f"Skipping transaction row: {exc}")
            continue

    db.commit()
    return stored


def _safe_str(val) -> Optional[str]:
    """Convert a value to string, returning None for NaN/empty."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() != "nan" else None


def _safe_datetime(val) -> Optional[datetime]:
    """Convert a value to datetime, returning None if unparseable."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return pd.to_datetime(val)
    except Exception:
        return None
