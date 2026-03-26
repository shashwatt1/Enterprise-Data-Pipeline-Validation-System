"""
Structured logging utility for pipeline operations.
Logs to both console and file, and persists step-level logs to the database.
"""

import logging
import sys
from datetime import datetime, timezone
from app.core.config import settings


def setup_logger(name: str = "pipeline") -> logging.Logger:
    """Create a logger with console + file handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))

    # Avoid adding duplicate handlers on re-import
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler
    file_handler = logging.FileHandler("pipeline.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()


def log_pipeline_step(
    db_session,
    pipeline_run_id: str,
    step: str,
    status: str,
    message: str = "",
    records_processed: int = 0,
    records_failed: int = 0,
):
    """Log a pipeline step to both the logger and the database."""
    from app.models.database import PipelineLog

    log_msg = (
        f"[{pipeline_run_id}] {step} — {status} | "
        f"processed={records_processed}, failed={records_failed}"
    )
    if message:
        log_msg += f" | {message}"

    if status == "FAILED":
        logger.error(log_msg)
    else:
        logger.info(log_msg)

    entry = PipelineLog(
        pipeline_run_id=pipeline_run_id,
        step=step,
        status=status,
        message=message,
        records_processed=records_processed,
        records_failed=records_failed,
        timestamp=datetime.now(timezone.utc),
    )
    db_session.add(entry)
    db_session.commit()
