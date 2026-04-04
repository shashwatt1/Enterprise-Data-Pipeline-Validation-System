"""
Data Transformation Service
Applies cleaning and standardization rules to mapped DataFrames:
  - Missing value handling
  - Duplicate removal
  - Format standardization (dates, phones, currency, casing)
  - Config-driven custom transformations
"""

import re
from datetime import datetime

import pandas as pd

from app.services.schema_mapper import (
    get_transformation_rules,
    get_deduplicate_keys,
)
from app.utils.logger import logger


def transform(
    df: pd.DataFrame, entity_type: str, pipeline_run_id: str = ""
) -> tuple[pd.DataFrame, dict]:
    """
    Run the full transformation pipeline on a mapped DataFrame.

    Returns:
        (cleaned_df, stats) where stats tracks rows dropped/modified at each step.
    """
    stats = {
        "initial_rows": len(df),
        "duplicates_removed": 0,
        "nulls_filled": 0,
        "rows_after_cleaning": 0,
    }

    prefix = f"[{pipeline_run_id}] " if pipeline_run_id else ""

    # Step 1: Remove duplicates
    dedup_keys = get_deduplicate_keys(entity_type)
    if dedup_keys:
        before = len(df)
        df = df.drop_duplicates(subset=dedup_keys, keep="first").reset_index(drop=True)
        stats["duplicates_removed"] = before - len(df)
        logger.info(
            f"{prefix}Removed {stats['duplicates_removed']} duplicate rows "
            f"on keys {dedup_keys}"
        )

    # Step 2: Apply column-level transformations
    rules = get_transformation_rules(entity_type)
    for col_name, rule in rules.items():
        if col_name not in df.columns:
            continue
        action = rule.get("action", "")
        df[col_name] = _apply_action(df[col_name], action, rule)
        logger.info(f"{prefix}Applied '{action}' to column '{col_name}'")

    # Step 3: Handle remaining missing values
    stats["nulls_filled"] = int(df.isna().sum().sum())
    df = _handle_missing_values(df, entity_type)

    stats["rows_after_cleaning"] = len(df)
    logger.info(
        f"{prefix}Transformation complete: "
        f"{stats['initial_rows']} → {stats['rows_after_cleaning']} rows"
    )
    return df, stats


def _apply_action(series: pd.Series, action: str, rule: dict) -> pd.Series:
    """Dispatch a transformation action to the appropriate handler."""
    handlers = {
        "to_integer": _to_integer,
        "strip_whitespace": _strip_whitespace,
        "lowercase": _lowercase,
        "uppercase": _uppercase,
        "title_case": _title_case,
        "standardize_phone": _standardize_phone,
        "parse_date": _parse_date,
        "parse_currency": _parse_currency,
    }
    handler = handlers.get(action)
    if handler is None:
        logger.warning(f"Unknown transformation action: {action}")
        return series
    return handler(series, rule)


# --- Individual transformation handlers ---

def _to_integer(series: pd.Series, rule: dict) -> pd.Series:
    """Convert to numeric, coercing errors to NaN."""
    return pd.to_numeric(series, errors="coerce")


def _strip_whitespace(series: pd.Series, rule: dict) -> pd.Series:
    """Strip leading/trailing whitespace from strings."""
    return series.astype(str).str.strip()


def _lowercase(series: pd.Series, rule: dict) -> pd.Series:
    """Convert strings to lowercase."""
    return series.astype(str).str.strip().str.lower()


def _uppercase(series: pd.Series, rule: dict) -> pd.Series:
    """Convert strings to UPPERCASE."""
    return series.astype(str).str.strip().str.upper()


def _title_case(series: pd.Series, rule: dict) -> pd.Series:
    """Convert strings to Title Case."""
    return series.astype(str).str.strip().str.title()


def _standardize_phone(series: pd.Series, rule: dict) -> pd.Series:
    """
    Normalize phone numbers to a consistent format.
    Strips non-digit characters, then formats as +1-XXX-XXXX.
    """
    def normalize(val):
        if pd.isna(val) or str(val).strip() == "":
            return None
        digits = re.sub(r"\D", "", str(val))
        # Handle US numbers: strip leading 1 if 11 digits
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == 10:
            return f"+1-{digits[:3]}-{digits[3:]}"
        # Return cleaned digits if we can't normalize the format
        return digits

    return series.apply(normalize)


def _parse_date(series: pd.Series, rule: dict) -> pd.Series:
    """
    Parse dates from multiple formats into a standardized datetime.
    Tries common formats then falls back to pandas inference.
    """
    common_formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d-%b-%Y",
        "%Y/%m/%d",
    ]

    def try_parse(val):
        if pd.isna(val) or str(val).strip() == "":
            return None
        val_str = str(val).strip()
        for fmt in common_formats:
            try:
                return datetime.strptime(val_str, fmt)
            except ValueError:
                continue
        # Fallback: let pandas try
        try:
            return pd.to_datetime(val_str)
        except Exception:
            return None

    return series.apply(try_parse)


def _parse_currency(series: pd.Series, rule: dict) -> pd.Series:
    """
    Strip currency symbols and thousand separators, then convert to float.
    Handles formats like: $1,200.50, $150.00, -$25.00
    """
    strip_chars = rule.get("strip_chars", "$,")

    def clean(val):
        if pd.isna(val) or str(val).strip() == "":
            return None
        val_str = str(val).strip()
        # Remove currency characters but keep minus sign and decimal point
        for ch in strip_chars:
            val_str = val_str.replace(ch, "")
        val_str = val_str.strip('"')
        try:
            return float(val_str)
        except ValueError:
            return None

    return series.apply(clean)


def _handle_missing_values(df: pd.DataFrame, entity_type: str) -> pd.DataFrame:
    """
    Fill or flag missing values based on column data types.
    - Strings: replace NaN / 'nan' / empty with None
    - Numerics: leave NaN for validation to catch
    """
    for col in df.columns:
        if df[col].dtype == object:
            # Clean up string representations of null
            df[col] = df[col].replace({"nan": None, "": None, "None": None})
    return df
