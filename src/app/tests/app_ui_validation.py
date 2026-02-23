"""
Validation tests for app UI logic: dataframe building and formatting.
Run from repo root: python -m pytest src/app/tests/app_ui_validation.py -v
Or from src/app: pytest tests/app_ui_validation.py -v
No Streamlit server required; uses mock backend.
"""
import sys
from pathlib import Path

# Ensure app package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from backend import get_backend
from models import (
    AuditFileRow,
    AuditStatusByBU,
    ClosureByBU,
    ClosureKPIs,
    ClosureQualitySummary,
    ClosureSlaRow,
    DocumentFlowStage,
    ErrorAnalysisSummary,
    GlobalClosureSent,
    RejectedFile,
)


def _models_to_dataframe(items: list, columns: list, row_to_list) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([row_to_list(x) for x in items], columns=columns)


def test_kpis_render():
    """KPI values render without error (no HTML injection)."""
    b, _ = get_backend("getnet_closure_dev", "financial_closure", "2025-02")
    k = b.get_kpis()
    assert k.total_amount >= 0
    assert k.rows_count >= 0
    assert k.files_valid >= 0
    assert k.files_rejected >= 0
    assert k.periods_sent >= 0
    # Simulate app KPI card value
    _ = f"{k.total_amount:,.0f}"
    _ = f"{k.rows_count:,}"


def test_document_flow_stages():
    """Document flow stages have valid counts and labels."""
    b, _ = get_backend("getnet_closure_dev", "financial_closure", "2025-02")
    flow = b.get_document_flow()
    assert len(flow.stages) >= 1
    for s in flow.stages:
        assert s.stage
        assert s.label
        assert isinstance(s.count, int) and s.count >= 0


def test_audit_files_table_rows():
    """Audit files table: row builder runs without error and produces display strings."""
    b, _ = get_backend("getnet_closure_dev", "financial_closure", "2025-02")
    audit_files = b.get_all_audit_files()

    def row_list(a: AuditFileRow):
        reason = (a.rejection_explanation or a.rejection_reason or "")[:80]
        if (a.rejection_explanation or a.rejection_reason) and len((a.rejection_explanation or a.rejection_reason or "")) > 80:
            reason += "..."
        return [
            a.file_name,
            a.file_path_in_volume[:60] + "..." if len(a.file_path_in_volume) > 60 else a.file_path_in_volume,
            a.business_unit or "",
            a.validation_status,
            reason,
            a.processed_at.strftime("%Y-%m-%d %H:%M") if a.processed_at else "",
            a.moved_to_review_at.strftime("%Y-%m-%d %H:%M") if a.moved_to_review_at else "",
        ]

    df = _models_to_dataframe(
        audit_files,
        ["File", "Path", "BU", "Status", "Rejection reason", "Processed at", "Moved to review"],
        row_list,
    )
    assert list(df.columns) == ["File", "Path", "BU", "Status", "Rejection reason", "Processed at", "Moved to review"]


def test_closure_by_bu_dataframe():
    """Closure by BU table and chart data build correctly."""
    b, _ = get_backend("getnet_closure_dev", "financial_closure", "2025-02")
    closure_by_bu = b.get_closure_by_bu()
    df = _models_to_dataframe(
        closure_by_bu,
        ["business_unit", "closure_period", "row_count", "total_amount", "file_count"],
        lambda x: [x.business_unit or "", x.closure_period or "", x.row_count, x.total_amount, x.file_count],
    )
    assert "business_unit" in df.columns and "total_amount" in df.columns


def test_audit_status_by_bu_datetime_format():
    """Validation status by BU: last_processed formats without error."""
    b, _ = get_backend("getnet_closure_dev", "financial_closure", "2025-02")
    audit_list = b.get_audit_status_by_bu()
    df = _models_to_dataframe(
        audit_list,
        ["business_unit", "validation_status", "file_count", "last_processed"],
        lambda x: [
            x.business_unit or "",
            x.validation_status,
            x.file_count,
            x.last_processed.strftime("%Y-%m-%d %H:%M") if x.last_processed else "",
        ],
    )
    assert "last_processed" in df.columns


def test_global_sent_and_rejected_datetime_format():
    """Global sent and rejected tables: datetime columns format correctly."""
    b, _ = get_backend("getnet_closure_dev", "financial_closure", "2025-02")
    global_sent = b.get_global_closure_sent()
    rejected = b.get_rejected_files()
    for x in global_sent:
        _ = x.sent_at.strftime("%Y-%m-%d %H:%M") if x.sent_at else ""
    for x in rejected:
        _ = x.processed_at.strftime("%Y-%m-%d %H:%M") if x.processed_at else ""
        _ = x.moved_to_review_at.strftime("%Y-%m-%d %H:%M") if x.moved_to_review_at else ""


def test_sla_dataframe_format():
    """SLA table: all columns including optional datetimes format."""
    b, _ = get_backend("getnet_closure_dev", "financial_closure", "2025-02")
    sla_list = b.get_sla_metrics()

    def sla_row(x):
        return [
            x.period,
            x.business_unit or "",
            x.first_file_at.strftime("%Y-%m-%d %H:%M") if x.first_file_at else "",
            x.first_valid_at.strftime("%Y-%m-%d %H:%M") if x.first_valid_at else "",
            f"{x.hours_to_valid:.2f}" if x.hours_to_valid is not None else "",
            x.files_rejected,
            x.files_valid,
        ]

    df = _models_to_dataframe(
        sla_list,
        ["period", "business_unit", "first_file_at", "first_valid_at", "hours_to_valid", "files_rejected", "files_valid"],
        sla_row,
    )
    assert len(df.columns) == 7


def test_error_analysis_and_quality():
    """Error analysis and quality summary are consumable."""
    b, _ = get_backend("getnet_closure_dev", "financial_closure", "2025-02")
    err = b.get_error_analysis()
    quality_list = b.get_quality_summary()
    assert err.total_errors >= 0
    assert err.files_with_errors >= 0
    if quality_list:
        q = quality_list[0]
        _ = f"{q.pct_valid:.1f}"
        _ = f"{q.pct_rejected:.1f}"


if __name__ == "__main__":
    import sys
    tests = [
        test_kpis_render,
        test_document_flow_stages,
        test_audit_files_table_rows,
        test_closure_by_bu_dataframe,
        test_audit_status_by_bu_datetime_format,
        test_global_sent_and_rejected_datetime_format,
        test_sla_dataframe_format,
        test_error_analysis_and_quality,
    ]
    for t in tests:
        t()
        print(t.__name__, "OK")
    print("All UI validation checks passed.")
    sys.exit(0)
