"""
Pydantic models for Getnet Financial Closure backend.
All data consumed by the app is typed via these models (real or mock).
"""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# -----------------------------------------------------------------------------
# KPIs
# -----------------------------------------------------------------------------
class ClosureKPIs(BaseModel):
    """Aggregate KPIs for closure data."""
    total_amount: float = Field(0.0, description="Sum of amount across closure rows")
    rows_count: int = Field(0, description="Number of closure data rows")
    files_valid: int = Field(0, description="Count of files with validation_status=valid")
    files_rejected: int = Field(0, description="Count of files with validation_status=rejected")
    periods_sent: int = Field(0, description="Count of global closure sends logged")


# -----------------------------------------------------------------------------
# Closure by business unit
# -----------------------------------------------------------------------------
class ClosureByBU(BaseModel):
    """One row: closure aggregate per business unit and period."""
    business_unit: Optional[str] = None
    closure_period: Optional[str] = None
    row_count: int = 0
    total_amount: float = 0.0
    file_count: int = 0


# -----------------------------------------------------------------------------
# Audit / validation status
# -----------------------------------------------------------------------------
class AuditStatusByBU(BaseModel):
    """One row: file count per business unit and validation status."""
    business_unit: Optional[str] = None
    validation_status: str = Field(..., description="valid | rejected")
    file_count: int = 0
    last_processed: Optional[datetime] = None


# -----------------------------------------------------------------------------
# Global closure sent
# -----------------------------------------------------------------------------
class GlobalClosureSent(BaseModel):
    """One row: global closure send log."""
    closure_period: str = ""
    sent_at: Optional[datetime] = None
    recipient_email: Optional[str] = None
    job_run_id: Optional[str] = None


# -----------------------------------------------------------------------------
# Rejected files
# -----------------------------------------------------------------------------
class RejectedFile(BaseModel):
    """One rejected file record."""
    file_name: str = ""
    business_unit: Optional[str] = None
    rejection_reason: Optional[str] = None
    processed_at: Optional[datetime] = None
    moved_to_review_at: Optional[datetime] = None


# -----------------------------------------------------------------------------
# All audit files (valid + invalid) for the audit table view
# -----------------------------------------------------------------------------
class AuditFileRow(BaseModel):
    """One row from closure_file_audit: all files with status, errors, dates."""
    file_name: str = ""
    file_path_in_volume: str = ""
    business_unit: Optional[str] = None
    validation_status: str = Field(..., description="valid | rejected")
    rejection_reason: Optional[str] = None
    rejection_explanation: Optional[str] = None
    processed_at: Optional[datetime] = None
    moved_to_review_at: Optional[datetime] = None


# -----------------------------------------------------------------------------
# Document flow (pipeline stages)
# -----------------------------------------------------------------------------
class DocumentFlowStage(BaseModel):
    """One stage in the document flow pipeline."""
    stage: str = Field(..., description="e.g. ingested, valid, rejected, moved_to_review")
    label: str = Field(..., description="Display label")
    count: int = 0


class DocumentFlowSummary(BaseModel):
    """Summary of document flow for the pipeline view."""
    stages: list[DocumentFlowStage] = Field(default_factory=list)
    total_files: int = 0


# -----------------------------------------------------------------------------
# Error analysis (from validation_errors_summary)
# -----------------------------------------------------------------------------
class ErrorCauseCount(BaseModel):
    """Aggregate: how often a field/cause appears in rejections."""
    field: Optional[str] = None
    invalid_cause: Optional[str] = None
    count: int = 0
    example_value: Optional[str] = None


class ErrorAnalysisSummary(BaseModel):
    """Summary of validation errors for insight."""
    total_errors: int = 0
    by_field_and_cause: list[ErrorCauseCount] = Field(default_factory=list)
    files_with_errors: int = 0


# -----------------------------------------------------------------------------
# Closure health (SLA + quality)
# -----------------------------------------------------------------------------
class ClosureSlaRow(BaseModel):
    """One row: SLA metrics per period and BU."""
    period: str = ""
    business_unit: Optional[str] = None
    first_file_at: Optional[datetime] = None
    first_valid_at: Optional[datetime] = None
    hours_to_valid: Optional[float] = None
    files_rejected: int = 0
    files_valid: int = 0


class ClosureQualitySummary(BaseModel):
    """Quality summary per period."""
    period: str = ""
    total_files: int = 0
    pct_valid: float = 0.0
    pct_rejected: float = 0.0
    most_common_error_types: Optional[str] = None
    updated_at: Optional[datetime] = None


# -----------------------------------------------------------------------------
# Pipeline / automation status (BU → Global flow)
# -----------------------------------------------------------------------------
class PipelineRunStatus(BaseModel):
    """Last closure pipeline run status for automation health."""
    job_name: Optional[str] = None
    run_id: Optional[str] = None
    state: str = Field("UNKNOWN", description="RUNNING | TERMINATED | SKIPPED | INTERNAL_ERROR | etc.")
    result_state: Optional[str] = Field(None, description="SUCCESS | FAILED | TIMEDOUT | CANCELED")
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    message: Optional[str] = None
