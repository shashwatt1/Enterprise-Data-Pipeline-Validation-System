from __future__ import annotations

"""
Schema Mapper Service
Loads mapping configuration from YAML and applies column renaming
from source schema to target schema.
"""

import os
from typing import Any

import pandas as pd
import yaml

from app.utils.logger import logger

# Resolve config path relative to project root
_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "config",
    "mapping_config.yaml",
)


def load_mapping_config(config_path: str = _CONFIG_PATH) -> dict:
    """Load and parse the YAML mapping configuration."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def get_entity_config(entity_type: str, config_path: str = _CONFIG_PATH) -> dict:
    """Get the mapping configuration for a specific entity type."""
    config = load_mapping_config(config_path)
    if entity_type not in config:
        raise ValueError(
            f"No mapping config found for entity '{entity_type}'. "
            f"Available: {list(config.keys())}"
        )
    return config[entity_type]


def validate_source_schema(df: pd.DataFrame, entity_type: str) -> dict:
    """
    Check whether the DataFrame columns match the expected source schema.

    Returns:
        dict with 'valid' (bool), 'missing' and 'extra' column lists.
    """
    entity_config = get_entity_config(entity_type)
    expected_cols = {col["name"] for col in entity_config["source_schema"]}
    actual_cols = set(df.columns)

    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    result = {
        "valid": len(missing) == 0,
        "missing_columns": sorted(missing),
        "extra_columns": sorted(extra),
    }

    if missing:
        logger.warning(
            f"Source schema validation failed for '{entity_type}': "
            f"missing columns {missing}"
        )
    else:
        logger.info(f"Source schema validated for '{entity_type}'")

    return result


def map_columns(df: pd.DataFrame, entity_type: str) -> pd.DataFrame:
    """
    Rename DataFrame columns from source names to target names
    using the mapping configuration.

    Returns:
        DataFrame with renamed columns (unmapped columns are dropped).
    """
    entity_config = get_entity_config(entity_type)
    column_mapping = entity_config["column_mapping"]

    # Keep only columns that appear in the mapping
    source_cols_present = [c for c in column_mapping.keys() if c in df.columns]
    df_mapped = df[source_cols_present].copy()

    # Rename from source → target
    rename_map = {src: tgt for src, tgt in column_mapping.items() if src in df.columns}
    df_mapped = df_mapped.rename(columns=rename_map)

    logger.info(
        f"Column mapping applied for '{entity_type}': "
        f"{len(rename_map)} columns mapped"
    )
    return df_mapped


def get_transformation_rules(entity_type: str) -> dict[str, dict[str, Any]]:
    """Get the transformation rules for a specific entity type."""
    entity_config = get_entity_config(entity_type)
    return entity_config.get("transformations", {})


def get_validation_rules(entity_type: str) -> dict[str, list[dict]]:
    """Get the validation rules for a specific entity type."""
    entity_config = get_entity_config(entity_type)
    return entity_config.get("validation_rules", {})


def get_target_schema(entity_type: str) -> list[dict]:
    """Get the target schema definition for a specific entity type."""
    entity_config = get_entity_config(entity_type)
    return entity_config.get("target_schema", [])


def get_deduplicate_keys(entity_type: str) -> list[str]:
    """Get the deduplication key columns for an entity type."""
    entity_config = get_entity_config(entity_type)
    return entity_config.get("deduplicate_on", [])
