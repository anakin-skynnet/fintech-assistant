"""
Config-driven validation for closure Excel files.
Returns list of errors: {row, field, value, invalid_cause}; if any error, file is rejected.
"""
import json
from typing import Any, Dict, List, Optional
from datetime import datetime

import yaml


def load_schema(config_path: str) -> Dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _coerce_date(val: Any, fmt: str) -> Optional[datetime]:
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return None
    if hasattr(val, "date"):
        return val
    try:
        return datetime.strptime(str(val).strip()[:10], fmt[:10] if len(fmt) >= 10 else fmt)
    except Exception:
        return None


def validate_row(
    row_index: int,
    row: Dict[str, Any],
    columns_config: List[Dict],
    max_errors: int = 100,
) -> List[Dict[str, Any]]:
    """
    Validate one row against closure_schema columns. Returns list of errors.
    Each error: {row, field, value, invalid_cause}.
    """
    errors = []
    for col_def in columns_config:
        name = col_def.get("name")
        validations = col_def.get("validations", [])
        val = row.get(name)
        for v in validations:
            if v == "not_null":
                if val is None or (isinstance(val, str) and str(val).strip() == ""):
                    errors.append({
                        "row": row_index,
                        "field": name,
                        "value": str(val) if val is not None else "",
                        "invalid_cause": "not_null",
                    })
                    if len(errors) >= max_errors:
                        return errors
                    break
            elif v == "greater_than_zero":
                try:
                    n = float(val) if val is not None and str(val).strip() != "" else None
                except (TypeError, ValueError):
                    n = None
                if n is None or n <= 0:
                    errors.append({
                        "row": row_index,
                        "field": name,
                        "value": str(val) if val is not None else "",
                        "invalid_cause": "must be greater than zero",
                    })
                    if len(errors) >= max_errors:
                        return errors
                    break
            elif v == "non_negative":
                try:
                    n = float(val) if val is not None and str(val).strip() != "" else None
                except (TypeError, ValueError):
                    n = None
                if n is not None and n < 0:
                    errors.append({
                        "row": row_index,
                        "field": name,
                        "value": str(val),
                        "invalid_cause": "must be non-negative",
                    })
                    if len(errors) >= max_errors:
                        return errors
                    break
            elif v == "date_format":
                fmt = col_def.get("date_format", "yyyy-MM-dd").replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d")
                if _coerce_date(val, fmt) is None and val is not None and str(val).strip() != "":
                    errors.append({
                        "row": row_index,
                        "field": name,
                        "value": str(val) if val is not None else "",
                        "invalid_cause": "invalid date format",
                    })
                    if len(errors) >= max_errors:
                        return errors
                    break
    return errors


def validate_dataframe(df, columns_config: List[Dict], max_errors: int = 100) -> List[Dict[str, Any]]:
    """
    Validate a DataFrame (from Excel). Returns all errors up to max_errors.
    """
    all_errors = []
    for idx, row in df.iterrows():
        row_index = int(idx) + 2  # 1-based Excel row (2 = first data row if header is 1)
        row_dict = row.to_dict()
        errs = validate_row(row_index, row_dict, columns_config, max_errors - len(all_errors))
        all_errors.extend(errs)
        if len(all_errors) >= max_errors:
            break
    return all_errors
