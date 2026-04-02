from __future__ import annotations

"""
Data Ingestion Service
Handles CSV file uploads and JSON data ingestion, storing raw records
in the staging table for downstream processing.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.models.database import RawData
from app.utils.logger import logger


def generate_run_id() -> str:
    """Generate a unique pipeline run identifier."""
    return str(uuid.uuid4())[:12]


def ingest_csv(
    db: Session,
    file_path: str,
    entity_type: str,
    pipeline_run_id: Optional[str] = None,
) -> dict:
    """
    Read a CSV file and store each row as a raw JSON record.

    Returns:
        dict with pipeline_run_id, entity_type, and row count.
    """
    if pipeline_run_id is None:
        pipeline_run_id = generate_run_id()

    logger.info(f"[{pipeline_run_id}] Ingesting CSV: {file_path} as '{entity_type}'")

    try:
        df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    except Exception as exc:
        logger.error(f"[{pipeline_run_id}] CSV read failed: {exc}")
        raise ValueError(f"Failed to read CSV: {exc}")

    row_count = _store_raw_records(db, df, entity_type, pipeline_run_id, file_path)

    logger.info(f"[{pipeline_run_id}] Ingested {row_count} rows from {file_path}")
    return {
        "pipeline_run_id": pipeline_run_id,
        "entity_type": entity_type,
        "rows_ingested": row_count,
        "source": file_path,
    }


def ingest_json(
    db: Session,
    records: list[dict],
    entity_type: str,
    pipeline_run_id: Optional[str] = None,
) -> dict:
    """
    Accept a list of JSON records and store them as raw data.

    Returns:
        dict with pipeline_run_id, entity_type, and row count.
    """
    if pipeline_run_id is None:
        pipeline_run_id = generate_run_id()

    logger.info(
        f"[{pipeline_run_id}] Ingesting {len(records)} JSON records as '{entity_type}'"
    )

    df = pd.DataFrame(records).astype(str)
    row_count = _store_raw_records(db, df, entity_type, pipeline_run_id, source="api")

    logger.info(f"[{pipeline_run_id}] Ingested {row_count} JSON records")
    return {
        "pipeline_run_id": pipeline_run_id,
        "entity_type": entity_type,
        "rows_ingested": row_count,
        "source": "api",
    }


def _store_raw_records(
    db: Session,
    df: pd.DataFrame,
    entity_type: str,
    pipeline_run_id: str,
    source: str,
) -> int:
    """Persist each DataFrame row as a RawData record."""
    raw_records = []
    for idx, row in df.iterrows():
        raw_records.append(
            RawData(
                pipeline_run_id=pipeline_run_id,
                entity_type=entity_type,
                row_index=idx,
                data_json=json.dumps(row.to_dict()),
                source_file=source,
                created_at=datetime.now(timezone.utc),
            )
        )

    db.bulk_save_objects(raw_records)
    db.commit()
    return len(raw_records)


def load_raw_data_as_dataframe(
    db: Session, pipeline_run_id: str, entity_type: str
) -> pd.DataFrame:
    """Retrieve raw records for a pipeline run and reconstruct as a DataFrame."""
    rows = (
        db.query(RawData)
        .filter(
            RawData.pipeline_run_id == pipeline_run_id,
            RawData.entity_type == entity_type,
        )
        .order_by(RawData.row_index)
        .all()
    )

    if not rows:
        raise ValueError(
            f"No raw data found for run_id={pipeline_run_id}, entity={entity_type}"
        )

    records = [json.loads(r.data_json) for r in rows]
    return pd.DataFrame(records)
