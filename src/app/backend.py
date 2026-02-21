"""
Backend service for Getnet Financial Closure.
Fetches data from Databricks (Spark SQL) or returns mock data when backend is unavailable.
All responses use Pydantic models from models.py.
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from models import (
    AuditStatusByBU,
    ClosureByBU,
    ClosureKPIs,
    ClosureQualitySummary,
    ClosureSlaRow,
    DocumentFlowStage,
    DocumentFlowSummary,
    ErrorAnalysisSummary,
    ErrorCauseCount,
    GlobalClosureSent,
    RejectedFile,
)


class ClosureBackend(ABC):
    """Abstract backend: Databricks or Mock."""

    def __init__(self, catalog: str, schema: str, period: Optional[str] = None):
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"
        self.period = period or ""
        self.period_clause = f"AND closure_period = '{self.period}'" if self.period else ""
        self.audit_period_clause = (
            f"AND date_format(processed_at, 'yyyy-MM') = '{self.period}'" if self.period else ""
        )

    @abstractmethod
    def get_kpis(self) -> ClosureKPIs:
        ...

    @abstractmethod
    def get_closure_by_bu(self) -> list[ClosureByBU]:
        ...

    @abstractmethod
    def get_audit_status_by_bu(self) -> list[AuditStatusByBU]:
        ...

    @abstractmethod
    def get_global_closure_sent(self) -> list[GlobalClosureSent]:
        ...

    @abstractmethod
    def get_rejected_files(self) -> list[RejectedFile]:
        ...

    @abstractmethod
    def get_document_flow(self) -> DocumentFlowSummary:
        ...

    @abstractmethod
    def get_error_analysis(self) -> ErrorAnalysisSummary:
        ...

    @abstractmethod
    def get_sla_metrics(self) -> list[ClosureSlaRow]:
        ...

    @abstractmethod
    def get_quality_summary(self) -> list[ClosureQualitySummary]:
        ...


def _safe_float(v: Any) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _safe_int(v: Any) -> int:
    try:
        return int(v) if v is not None else 0
    except (TypeError, ValueError):
        return 0


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _safe_identifier(value: str, default: str, pattern: str = r"^[a-zA-Z0-9_]+$") -> str:
    """Return value if it matches pattern (safe for SQL identifiers), else default."""
    if not value or not value.strip():
        return default
    import re
    return value.strip() if re.match(pattern, value.strip()) else default


def _safe_period(value: Optional[str]) -> str:
    """Return period if it matches yyyy-MM, else empty string."""
    if not value or not value.strip():
        return ""
    import re
    return value.strip() if re.match(r"^\d{4}-\d{2}$", value.strip()) else ""


def _safe_ts(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return pd.Timestamp(v).to_pydatetime()
    except Exception:
        return None


class DatabricksBackend(ClosureBackend):
    """Backend that runs Spark SQL against Unity Catalog closure tables."""

    def __init__(self, catalog: str, schema: str, period: Optional[str] = None, spark: Any = None):
        super().__init__(catalog, schema, period)
        self.spark = spark

    def _run_sql(self, sql: str) -> pd.DataFrame:
        if self.spark is None:
            return pd.DataFrame()
        try:
            return self.spark.sql(sql).toPandas()
        except Exception:
            return pd.DataFrame()

    def get_kpis(self) -> ClosureKPIs:
        df_closure = self._run_sql(f"""
            SELECT COUNT(*) AS rows_count, SUM(COALESCE(amount, 0)) AS total_amount
            FROM {self.full_schema}.closure_data WHERE 1=1 {self.period_clause}
        """)
        df_audit = self._run_sql(f"""
            SELECT validation_status, COUNT(*) AS cnt
            FROM {self.full_schema}.closure_file_audit WHERE 1=1 {self.audit_period_clause}
            GROUP BY validation_status
        """)
        df_global = self._run_sql(f"SELECT COUNT(*) AS sent_count FROM {self.full_schema}.global_closure_sent")

        total_amount = 0.0
        rows_count = 0
        if not df_closure.empty:
            total_amount = _safe_float(df_closure["total_amount"].iloc[0])
            rows_count = _safe_int(df_closure["rows_count"].iloc[0])

        valid_count = 0
        rejected_count = 0
        if not df_audit.empty:
            for _, row in df_audit.iterrows():
                cnt = _safe_int(row.get("cnt"))
                if row.get("validation_status") == "valid":
                    valid_count = cnt
                else:
                    rejected_count += cnt

        sent_count = 0
        if not df_global.empty:
            sent_count = _safe_int(df_global["sent_count"].iloc[0])

        return ClosureKPIs(
            total_amount=total_amount,
            rows_count=rows_count,
            files_valid=valid_count,
            files_rejected=rejected_count,
            periods_sent=sent_count,
        )

    def get_closure_by_bu(self) -> list[ClosureByBU]:
        df = self._run_sql(f"""
            SELECT business_unit, closure_period, COUNT(*) AS row_count,
                   SUM(COALESCE(amount, 0)) AS total_amount,
                   COUNT(DISTINCT source_file_name) AS file_count
            FROM {self.full_schema}.closure_data WHERE 1=1 {self.period_clause}
            GROUP BY business_unit, closure_period
            ORDER BY closure_period DESC, total_amount DESC
        """)
        out = []
        for _, row in df.iterrows():
            out.append(ClosureByBU(
                business_unit=_safe_str(row.get("business_unit")),
                closure_period=_safe_str(row.get("closure_period")),
                row_count=_safe_int(row.get("row_count")),
                total_amount=_safe_float(row.get("total_amount")),
                file_count=_safe_int(row.get("file_count")),
            ))
        return out

    def get_audit_status_by_bu(self) -> list[AuditStatusByBU]:
        df = self._run_sql(f"""
            SELECT business_unit, validation_status, COUNT(*) AS file_count, MAX(processed_at) AS last_processed
            FROM {self.full_schema}.closure_file_audit WHERE 1=1 {self.audit_period_clause}
            GROUP BY business_unit, validation_status
            ORDER BY business_unit, validation_status
        """)
        out = []
        for _, row in df.iterrows():
            out.append(AuditStatusByBU(
                business_unit=_safe_str(row.get("business_unit")),
                validation_status=_safe_str(row.get("validation_status")) or "valid",
                file_count=_safe_int(row.get("file_count")),
                last_processed=_safe_ts(row.get("last_processed")),
            ))
        return out

    def get_global_closure_sent(self) -> list[GlobalClosureSent]:
        df = self._run_sql(f"""
            SELECT closure_period, sent_at, recipient_email, job_run_id
            FROM {self.full_schema}.global_closure_sent ORDER BY sent_at DESC LIMIT 24
        """)
        out = []
        for _, row in df.iterrows():
            out.append(GlobalClosureSent(
                closure_period=_safe_str(row.get("closure_period")),
                sent_at=_safe_ts(row.get("sent_at")),
                recipient_email=_safe_str(row.get("recipient_email")),
                job_run_id=_safe_str(row.get("job_run_id")),
            ))
        return out

    def get_rejected_files(self) -> list[RejectedFile]:
        df = self._run_sql(f"""
            SELECT file_name, business_unit, rejection_reason, processed_at, moved_to_review_at
            FROM {self.full_schema}.closure_file_audit
            WHERE validation_status = 'rejected'
            ORDER BY processed_at DESC LIMIT 50
        """)
        out = []
        for _, row in df.iterrows():
            out.append(RejectedFile(
                file_name=_safe_str(row.get("file_name")),
                business_unit=_safe_str(row.get("business_unit")),
                rejection_reason=_safe_str(row.get("rejection_reason")),
                processed_at=_safe_ts(row.get("processed_at")),
                moved_to_review_at=_safe_ts(row.get("moved_to_review_at")),
            ))
        return out

    def get_document_flow(self) -> DocumentFlowSummary:
        df_audit = self._run_sql(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN validation_status = 'valid' THEN 1 ELSE 0 END) AS valid_count,
                SUM(CASE WHEN validation_status = 'rejected' THEN 1 ELSE 0 END) AS rejected_count,
                SUM(CASE WHEN validation_status = 'rejected' AND moved_to_review_at IS NOT NULL THEN 1 ELSE 0 END) AS moved_count
            FROM {self.full_schema}.closure_file_audit WHERE 1=1 {self.audit_period_clause}
        """)
        total = 0
        valid_count = 0
        rejected_count = 0
        moved_count = 0
        if not df_audit.empty:
            total = _safe_int(df_audit["total"].iloc[0])
            valid_count = _safe_int(df_audit["valid_count"].iloc[0])
            rejected_count = _safe_int(df_audit["rejected_count"].iloc[0])
            moved_count = _safe_int(df_audit["moved_count"].iloc[0])
        stages = [
            DocumentFlowStage(stage="ingested", label="Files ingested", count=total),
            DocumentFlowStage(stage="valid", label="Valid", count=valid_count),
            DocumentFlowStage(stage="rejected", label="Rejected", count=rejected_count),
            DocumentFlowStage(stage="moved_to_review", label="Moved to review", count=moved_count),
        ]
        return DocumentFlowSummary(stages=stages, total_files=total)

    def get_error_analysis(self) -> ErrorAnalysisSummary:
        # View closure_audit_errors: one row per error (error_field, error_value, invalid_cause)
        df = self._run_sql(f"""
            SELECT error_field AS field_name, invalid_cause,
                   COUNT(*) AS error_count, MAX(error_value) AS example_value
            FROM {self.full_schema}.closure_audit_errors
            WHERE 1=1 {self.audit_period_clause}
            GROUP BY error_field, invalid_cause
            ORDER BY error_count DESC
        """)
        by_cause: list[ErrorCauseCount] = []
        total_errors = 0
        if not df.empty:
            for _, row in df.iterrows():
                cnt = _safe_int(row.get("error_count"))
                total_errors += cnt
                by_cause.append(ErrorCauseCount(
                    field=_safe_str(row.get("field_name")),
                    invalid_cause=_safe_str(row.get("invalid_cause")),
                    count=cnt,
                    example_value=_safe_str(row.get("example_value")),
                ))
        # Count distinct files with errors from audit
        df_files = self._run_sql(f"""
            SELECT COUNT(DISTINCT file_name) AS files_with_errors
            FROM {self.full_schema}.closure_file_audit
            WHERE validation_status = 'rejected' AND 1=1 {self.audit_period_clause}
        """)
        files_with_errors = _safe_int(df_files["files_with_errors"].iloc[0]) if not df_files.empty else 0
        return ErrorAnalysisSummary(
            total_errors=total_errors,
            by_field_and_cause=by_cause[:20],
            files_with_errors=files_with_errors,
        )

    def get_sla_metrics(self) -> list[ClosureSlaRow]:
        try:
            period_filter = f"AND period = '{self.period}'" if self.period else ""
            df = self._run_sql(f"""
                SELECT period, business_unit, first_file_at, first_valid_at, hours_to_valid, files_rejected, files_valid
                FROM {self.full_schema}.closure_sla_metrics
                WHERE 1=1 {period_filter}
                ORDER BY period DESC, business_unit LIMIT 50
            """)
            out = []
            for _, row in df.iterrows():
                out.append(ClosureSlaRow(
                    period=_safe_str(row.get("period")),
                    business_unit=_safe_str(row.get("business_unit")),
                    first_file_at=_safe_ts(row.get("first_file_at")),
                    first_valid_at=_safe_ts(row.get("first_valid_at")),
                    hours_to_valid=_safe_float(row.get("hours_to_valid")) if row.get("hours_to_valid") is not None else None,
                    files_rejected=_safe_int(row.get("files_rejected")),
                    files_valid=_safe_int(row.get("files_valid")),
                ))
            return out
        except Exception:
            return []

    def get_quality_summary(self) -> list[ClosureQualitySummary]:
        try:
            period_filter = f"AND period = '{self.period}'" if self.period else ""
            df = self._run_sql(f"""
                SELECT period, total_files, pct_valid, pct_rejected, most_common_error_types, updated_at
                FROM {self.full_schema}.closure_quality_summary
                WHERE 1=1 {period_filter}
                ORDER BY period DESC LIMIT 12
            """)
            out = []
            for _, row in df.iterrows():
                out.append(ClosureQualitySummary(
                    period=_safe_str(row.get("period")),
                    total_files=_safe_int(row.get("total_files")),
                    pct_valid=_safe_float(row.get("pct_valid")),
                    pct_rejected=_safe_float(row.get("pct_rejected")),
                    most_common_error_types=_safe_str(row.get("most_common_error_types")) or None,
                    updated_at=_safe_ts(row.get("updated_at")),
                ))
            return out
        except Exception:
            return []


def _mock_kpis() -> ClosureKPIs:
    return ClosureKPIs(
        total_amount=2_450_000.0,
        rows_count=1240,
        files_valid=8,
        files_rejected=2,
        periods_sent=3,
    )


def _mock_closure_by_bu() -> list[ClosureByBU]:
    return [
        ClosureByBU(business_unit="BU_A", closure_period="2025-02", row_count=420, total_amount=890_000.0, file_count=2),
        ClosureByBU(business_unit="BU_B", closure_period="2025-02", row_count=380, total_amount=760_000.0, file_count=2),
        ClosureByBU(business_unit="BU_C", closure_period="2025-02", row_count=440, total_amount=800_000.0, file_count=2),
    ]


def _mock_audit_status() -> list[AuditStatusByBU]:
    return [
        AuditStatusByBU(business_unit="BU_A", validation_status="valid", file_count=2, last_processed=datetime(2025, 2, 20, 10, 0)),
        AuditStatusByBU(business_unit="BU_B", validation_status="valid", file_count=2, last_processed=datetime(2025, 2, 20, 10, 5)),
        AuditStatusByBU(business_unit="BU_C", validation_status="rejected", file_count=1, last_processed=datetime(2025, 2, 20, 9, 55)),
    ]


def _mock_global_sent() -> list[GlobalClosureSent]:
    return [
        GlobalClosureSent(closure_period="2025-01", sent_at=datetime(2025, 1, 31, 14, 0), recipient_email="lead@getnet.com", job_run_id="123"),
    ]


def _mock_rejected() -> list[RejectedFile]:
    return [
        RejectedFile(file_name="closure_bu_c_202502.xlsx", business_unit="BU_C", rejection_reason="Invalid amount format", processed_at=datetime(2025, 2, 20, 9, 55), moved_to_review_at=datetime(2025, 2, 20, 10, 0)),
    ]


def _mock_document_flow() -> DocumentFlowSummary:
    return DocumentFlowSummary(
        stages=[
            DocumentFlowStage(stage="ingested", label="Files ingested", count=10),
            DocumentFlowStage(stage="valid", label="Valid", count=8),
            DocumentFlowStage(stage="rejected", label="Rejected", count=2),
            DocumentFlowStage(stage="moved_to_review", label="Moved to review", count=2),
        ],
        total_files=10,
    )


def _mock_error_analysis() -> ErrorAnalysisSummary:
    return ErrorAnalysisSummary(
        total_errors=12,
        by_field_and_cause=[
            ErrorCauseCount(field="amount", invalid_cause="invalid_format", count=5, example_value="1,234.56"),
            ErrorCauseCount(field="closure_period", invalid_cause="invalid_format", count=4, example_value="2025/02"),
            ErrorCauseCount(field="business_unit", invalid_cause="not_in_list", count=3, example_value="BU_X"),
        ],
        files_with_errors=2,
    )


def _mock_sla() -> list[ClosureSlaRow]:
    return [
        ClosureSlaRow(period="2025-02", business_unit="BU_A", first_file_at=datetime(2025, 2, 20, 8, 0), first_valid_at=datetime(2025, 2, 20, 8, 5), hours_to_valid=0.08, files_rejected=0, files_valid=2),
        ClosureSlaRow(period="2025-02", business_unit="BU_B", first_file_at=datetime(2025, 2, 20, 8, 10), first_valid_at=datetime(2025, 2, 20, 8, 12), hours_to_valid=0.06, files_rejected=0, files_valid=2),
        ClosureSlaRow(period="2025-02", business_unit="BU_C", first_file_at=datetime(2025, 2, 20, 7, 55), first_valid_at=None, hours_to_valid=None, files_rejected=1, files_valid=0),
    ]


def _mock_quality() -> list[ClosureQualitySummary]:
    return [
        ClosureQualitySummary(period="2025-02", total_files=10, pct_valid=80.0, pct_rejected=20.0, most_common_error_types="invalid date format(5), must be greater than zero(4)", updated_at=datetime(2025, 2, 20, 10, 0)),
    ]


class MockBackend(ClosureBackend):
    """Backend that returns mock data (Pydantic models) when Databricks is unavailable."""

    def get_kpis(self) -> ClosureKPIs:
        return _mock_kpis()

    def get_closure_by_bu(self) -> list[ClosureByBU]:
        return _mock_closure_by_bu()

    def get_audit_status_by_bu(self) -> list[AuditStatusByBU]:
        return _mock_audit_status()

    def get_global_closure_sent(self) -> list[GlobalClosureSent]:
        return _mock_global_sent()

    def get_rejected_files(self) -> list[RejectedFile]:
        return _mock_rejected()

    def get_document_flow(self) -> DocumentFlowSummary:
        return _mock_document_flow()

    def get_error_analysis(self) -> ErrorAnalysisSummary:
        return _mock_error_analysis()

    def get_sla_metrics(self) -> list[ClosureSlaRow]:
        return _mock_sla()

    def get_quality_summary(self) -> list[ClosureQualitySummary]:
        return _mock_quality()


def get_backend(catalog: str, schema: str, period: Optional[str] = None) -> tuple[ClosureBackend, bool]:
    """
    Return (backend, use_real_data).
    use_real_data is False when using mock (no Spark or backend unavailable).
    Sanitizes catalog, schema, and period for safe SQL use.
    """
    catalog = _safe_identifier(catalog, "getnet_closure_dev")
    schema = _safe_identifier(schema, "financial_closure")
    period = _safe_period(period)
    spark = None
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    except Exception:
        pass
    if spark is not None:
        return DatabricksBackend(catalog=catalog, schema=schema, period=period, spark=spark), True
    return MockBackend(catalog=catalog, schema=schema, period=period), False
