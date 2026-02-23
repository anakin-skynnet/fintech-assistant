"""
Shared utilities for Databricks job notebooks.
Use for consistent run IDs, safe identifiers, and config loading.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Optional


# Default catalog/schema for closure (align with bundle variables)
DEFAULT_CATALOG = "getnet_closure_dev"
DEFAULT_SCHEMA = "financial_closure"
DEFAULT_VOLUME_RAW = "raw_closure_files"

# SQL-safe: alphanumeric and underscore only
_IDENT_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")


def get_run_id() -> str:
    """Return current job run ID or a timestamp-based fallback."""
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get())
    except Exception:
        return f"run-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"


def safe_identifier(value: str, default: str) -> str:
    """Return value if it matches safe SQL identifier pattern, else default."""
    if not value or not isinstance(value, str):
        return default
    v = value.strip()
    return v if _IDENT_PATTERN.match(v) else default


def safe_catalog(catalog: str) -> str:
    return safe_identifier(catalog, DEFAULT_CATALOG)


def safe_schema(schema: str) -> str:
    return safe_identifier(schema, DEFAULT_SCHEMA)


def log(step: str, message: str, *args: Any) -> None:
    """Print a consistent log line for job steps."""
    prefix = f"[{step}]"
    if args:
        print(prefix, message, *args)
    else:
        print(prefix, message)
