"""
Getnet Financial Closure — Databricks App
Presents closure analytics by business unit and global financial closure to end-users.
Data is fetched from backend (Databricks/Spark or mock); all DTOs use Pydantic models.
"""
import os
import streamlit as st
import pandas as pd

# Optional: plotly for charts (graceful fallback if not installed)
try:
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

from backend import get_backend
from models import (
    AuditFileRow,
    AuditStatusByBU,
    ClosureByBU,
    ClosureKPIs,
    DocumentFlowSummary,
    ErrorAnalysisSummary,
    GlobalClosureSent,
    RejectedFile,
)


@st.cache_data(ttl=60)
def _load_closure_data(catalog: str, schema: str, period: str):
    """
    Load all app data from backend. Cached 60s per (catalog, schema, period)
    to avoid refetching on every Streamlit rerun.
    Returns (use_real_data, kpis, closure_by_bu, flow, err_summary, audit_list,
             global_sent, rejected, audit_files, sla_list, quality_list).
    """
    backend, use_real_data = get_backend(catalog, schema, period or None)
    kpis = backend.get_kpis()
    closure_by_bu = backend.get_closure_by_bu()
    flow = backend.get_document_flow()
    err_summary = backend.get_error_analysis()
    audit_list = backend.get_audit_status_by_bu()
    global_sent = backend.get_global_closure_sent()
    rejected = backend.get_rejected_files()
    audit_files = backend.get_all_audit_files()
    sla_list = backend.get_sla_metrics()
    quality_list = backend.get_quality_summary()
    return (
        use_real_data,
        kpis,
        closure_by_bu,
        flow,
        err_summary,
        audit_list,
        global_sent,
        rejected,
        audit_files,
        sla_list,
        quality_list,
    )


# -----------------------------------------------------------------------------
# Page config and custom CSS
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Getnet Financial Closure",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* Fintech-inspired palette: deep navy, warm cream, teal accent */
    :root {
        --bg-dark: #0f172a;
        --bg-card: #1e293b;
        --text-primary: #f8fafc;
        --text-muted: #94a3b8;
        --accent: #14b8a6;
        --accent-soft: rgba(20, 184, 166, 0.15);
        --border: #334155;
        --valid: #22c55e;
        --rejected: #ef4444;
    }
    .stApp { background: linear-gradient(180deg, #0f172a 0%, #1e293b 50%, #0f172a 100%); }
    header[data-testid="stHeader"] { background: rgba(15, 23, 42, 0.9); }
    .main .block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 1400px; }
    
    /* Title block */
    .main-title {
        font-family: 'Georgia', 'Cambria', serif;
        font-size: 2.25rem;
        font-weight: 600;
        color: var(--text-primary);
        letter-spacing: -0.02em;
        margin-bottom: 0.25rem;
    }
    .main-subtitle {
        font-size: 1rem;
        color: var(--text-muted);
        margin-bottom: 2rem;
    }
    
    /* KPI cards */
    .kpi-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1.25rem 1.5rem;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.2);
    }
    .kpi-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: var(--accent);
    }
    .kpi-label {
        font-size: 0.8rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-top: 0.25rem;
    }
    
    /* Section headers */
    .section-title {
        font-size: 1.25rem;
        font-weight: 600;
        color: var(--text-primary);
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid var(--border);
    }
    
    /* Document flow pipeline */
    .flow-pipeline {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        flex-wrap: wrap;
        margin: 1rem 0;
    }
    .flow-stage {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 1rem 1.25rem;
        min-width: 140px;
        text-align: center;
    }
    .flow-stage .flow-count { font-size: 1.5rem; font-weight: 700; color: var(--accent); }
    .flow-stage .flow-label { font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; margin-top: 0.25rem; }
    .flow-arrow { color: var(--text-muted); font-size: 1.25rem; }
    
    /* DataFrames: subtle dark table styling */
    div[data-testid="stDataFrame"] {
        border-radius: 8px;
        overflow: hidden;
        border: 1px solid var(--border);
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
    }
    [data-testid="stSidebar"] .stSelectbox label { color: var(--text-muted); }
    
    /* Mock data banner */
    .mock-banner {
        background: rgba(245, 158, 11, 0.15);
        border: 1px solid rgba(245, 158, 11, 0.5);
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin-bottom: 1.5rem;
        font-size: 0.9rem;
        color: #fcd34d;
    }
</style>
""", unsafe_allow_html=True)


def _models_to_dataframe(items: list, columns: list[str], row_to_list):
    """Build a pandas DataFrame from a list of Pydantic models."""
    if not items:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([row_to_list(x) for x in items], columns=columns)


def main():
    # Sidebar: filters
    with st.sidebar:
        st.markdown("### **Filters**")
        catalog = st.text_input(
            "Catalog",
            value=os.environ.get("CLOSURE_CATALOG", "getnet_closure_dev"),
            key="catalog",
        )
        schema = st.text_input("Schema", value="financial_closure", key="schema")
        period = st.text_input(
            "Closure period (e.g. 2025-02)",
            value="",
            key="period",
            placeholder="Optional",
        )
        volume_raw = st.text_input(
            "Raw volume name",
            value=os.environ.get("CLOSURE_VOLUME_RAW", "raw_closure_files"),
            key="volume_raw",
            placeholder="raw_closure_files",
        )
        st.markdown("---")
        st.caption("Getnet Financial Closure · Data from backend (UC or mock)")
        st.caption("Pipeline automates: Ingest → Validate → Reject to review → Global send.")

    with st.spinner("Loading closure data..."):
        (
            use_real_data,
            kpis,
            closure_by_bu,
            flow,
            err_summary,
            audit_list,
            global_sent,
            rejected,
            audit_files,
            sla_list,
            quality_list,
        ) = _load_closure_data(catalog.strip(), schema.strip(), period.strip())

    # Title
    st.markdown('<p class="main-title">Getnet Financial Closure</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="main-subtitle">Automate financial closure: accelerate approval, reduce manual steps — document flow, validation, and global send</p>',
        unsafe_allow_html=True,
    )

    if not use_real_data:
        st.markdown(
            '<div class="mock-banner">📌 Using mock data — run this app on Databricks with Spark to load from Unity Catalog.</div>',
            unsafe_allow_html=True,
        )

    # Upload Excel to raw volume (same as SharePoint ingest) — only when connected to Databricks
    if use_real_data:
        with st.expander("📤 Upload Excel to raw volume", expanded=False):
            st.caption("Files are stored in the same volume as SharePoint ingest. **Validation runs automatically** (no separate job): audit table is updated with date, file name, status (valid/invalid), and errors. Reduces manual re-runs.")
            uploaded = st.file_uploader(
                "Choose Excel file(s)",
                type=["xlsx", "xls"],
                accept_multiple_files=True,
                key="upload_excel",
            )
            if uploaded:
                if st.button("Upload to volume", key="upload_btn"):
                    backend, _ = get_backend(catalog.strip(), schema.strip(), period.strip())
                    vol = (volume_raw or "raw_closure_files").strip()
                    for f in uploaded:
                        ok, msg = backend.upload_file_to_volume(vol, f.getvalue(), f.name)
                        if ok:
                            if "Validated: valid" in msg:
                                st.success(f"**{f.name}**: Saved. Validated: **valid** — audit and closure data updated.")
                            elif "Validated: invalid" in msg:
                                st.warning(f"**{f.name}**: Saved. Validated: **invalid** — audit updated with errors. {msg.split('Validated: invalid')[-1].strip()}")
                            else:
                                st.success(f"**{f.name}**: {msg}")
                        else:
                            st.error(f"**{f.name}**: {msg}")

    # KPIs from backend (Pydantic) — key metrics for closure and automation health
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value">{kpis.total_amount:,.0f}</div><div class="kpi-label">Total amount</div></div>',
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value">{kpis.rows_count:,}</div><div class="kpi-label">Closure rows</div></div>',
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value" style="color: var(--valid);">{kpis.files_valid}</div><div class="kpi-label">Files valid</div></div>',
            unsafe_allow_html=True,
        )
    with col4:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value" style="color: var(--rejected);">{kpis.files_rejected}</div><div class="kpi-label">Files rejected</div></div>',
            unsafe_allow_html=True,
        )
    with col5:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-value">{kpis.periods_sent}</div><div class="kpi-label">Periods sent</div></div>',
            unsafe_allow_html=True,
        )

    # Document flow — automated pipeline visibility
    st.markdown('<p class="section-title">Document flow (automated pipeline)</p>', unsafe_allow_html=True)
    if flow.stages:
        stages_html = []
        for i, s in enumerate(flow.stages):
            color = "var(--valid)" if s.stage == "valid" else ("var(--rejected)" if s.stage in ("rejected", "moved_to_review") else "var(--accent)")
            stages_html.append(
                f'<div class="flow-stage"><div class="flow-count" style="color: {color};">{s.count}</div><div class="flow-label">{s.label}</div></div>'
            )
            if i < len(flow.stages) - 1:
                stages_html.append('<span class="flow-arrow">→</span>')
        st.markdown(
            f'<div class="flow-pipeline">{"".join(stages_html)}</div>',
            unsafe_allow_html=True,
        )
        st.caption("Automated: ingested → valid / rejected → moved to review. Reduce intervention by fixing invalid files (download or send to review).")
    else:
        st.info("No document flow data yet. Run the pipeline job (Ingest → Validate and load) or upload files to populate.")

    # All files (audit) — one table valid + invalid, download, fix options, Send to review
    st.markdown('<p class="section-title">All files (audit)</p>', unsafe_allow_html=True)
    if audit_files:
        def _row_list(a: AuditFileRow):
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
        df_audit = _models_to_dataframe(
            audit_files,
            ["File", "Path", "BU", "Status", "Rejection reason", "Processed at", "Moved to review"],
            _row_list,
        )
        st.dataframe(df_audit, use_container_width=True, hide_index=True)
        # Download per row (only when real backend and path available). Limit to 25 files to avoid slow loads.
        if use_real_data:
            st.caption("Download a file to fix locally, then re-upload using **Upload Excel to raw volume** above.")
            backend, _ = get_backend(catalog.strip(), schema.strip(), period.strip())
            max_downloads = 25
            for i, a in enumerate(audit_files):
                if i >= max_downloads:
                    break
                if not a.file_path_in_volume:
                    continue
                ok, data, msg = backend.get_file_bytes_from_volume(a.file_path_in_volume)
                if ok and data:
                    st.download_button(
                        label=f"Download {a.file_name}",
                        data=data,
                        file_name=a.file_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_{a.file_path_in_volume.replace('/', '_')}",
                    )
            if len(audit_files) > max_downloads:
                st.caption(f"Download buttons shown for the most recent {max_downloads} files. Use volume path to access others.")
        st.markdown("**How to fix invalid files:**")
        st.markdown("- **Option 1 (recommended):** Download the file using the button above → fix it locally → re-upload in **Upload Excel to raw volume**.")
        st.markdown("- **Option 2:** Run the job **Reject to SharePoint** (or use **Send to review** below) to move invalid files to the review folder; fix and re-submit from there.")
    else:
        st.caption("No audit rows for the selected period. Upload files above or run the pipeline job (Ingest → Validate and load) to automate.")

    # Send to review button (invalid files → SharePoint) — reduces manual copy/move
    if use_real_data:
        backend, _ = get_backend(catalog.strip(), schema.strip(), period.strip())
        if st.button("Send invalid files to SharePoint review", type="primary", key="send_to_review"):
            count, message = backend.move_rejected_to_sharepoint("getnet-sharepoint")
            if count > 0:
                st.success(message)
            else:
                st.info(message)
        st.caption("One click moves all pending invalid files to the review folder — no manual upload to SharePoint.")

    # Closure by business unit
    st.markdown('<p class="section-title">Closure by business unit</p>', unsafe_allow_html=True)
    if closure_by_bu:
        df_bu = _models_to_dataframe(
            closure_by_bu,
            ["business_unit", "closure_period", "row_count", "total_amount", "file_count"],
            lambda x: [x.business_unit or "", x.closure_period or "", x.row_count, x.total_amount, x.file_count],
        )
        c1, c2 = st.columns([1, 1])
        with c1:
            if HAS_PLOTLY:
                df_plot = df_bu.groupby("business_unit", as_index=False)["total_amount"].sum().sort_values("total_amount", ascending=True)
                fig = px.bar(
                    df_plot,
                    x="total_amount",
                    y="business_unit",
                    orientation="h",
                    labels={"total_amount": "Total amount", "business_unit": "Business unit"},
                    color="total_amount",
                    color_continuous_scale="Teal",
                )
                fig.update_layout(
                    margin=dict(l=20, r=20, t=30, b=20),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#f8fafc"),
                    showlegend=False,
                    xaxis=dict(gridcolor="rgba(148,163,184,0.2)"),
                    yaxis=dict(gridcolor="rgba(148,163,184,0.2)"),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.dataframe(df_bu, use_container_width=True, hide_index=True)
        with c2:
            st.dataframe(df_bu, use_container_width=True, hide_index=True)
    else:
        st.info("No closure data for the selected filters.")

    # Error analysis — high value for fixing rejections and accelerating approval
    st.markdown('<p class="section-title">Error analysis (validation failures)</p>', unsafe_allow_html=True)
    if err_summary.by_field_and_cause:
        df_err = _models_to_dataframe(
            err_summary.by_field_and_cause,
            ["field", "invalid_cause", "count", "example_value"],
            lambda x: [x.field or "", x.invalid_cause or "", x.count, x.example_value or ""],
        )
        st.caption(f"Total validation errors: {err_summary.total_errors} across {err_summary.files_with_errors} file(s). Fix these patterns to improve approval rates.")
        st.dataframe(df_err, use_container_width=True, hide_index=True)
    else:
        st.success("No validation errors in the selected period. Pipeline can proceed with minimal intervention.")

    # Closure health (SLA + quality)
    st.markdown('<p class="section-title">Closure health</p>', unsafe_allow_html=True)
    if sla_list:
        def _sla_row(x):
            return [
                x.period,
                x.business_unit or "",
                x.first_file_at.strftime("%Y-%m-%d %H:%M") if x.first_file_at else "",
                x.first_valid_at.strftime("%Y-%m-%d %H:%M") if x.first_valid_at else "",
                f"{x.hours_to_valid:.2f}" if x.hours_to_valid is not None else "",
                x.files_rejected,
                x.files_valid,
            ]
        df_sla = _models_to_dataframe(
            sla_list,
            ["period", "business_unit", "first_file_at", "first_valid_at", "hours_to_valid", "files_rejected", "files_valid"],
            _sla_row,
        )
        st.caption("SLA metrics (first file, first valid, hours to valid).")
        st.dataframe(df_sla, use_container_width=True, hide_index=True)
    if quality_list:
        q = quality_list[0]
        st.caption(f"Quality: {q.total_files} files — {q.pct_valid:.1f}% valid, {q.pct_rejected:.1f}% rejected. Most common errors: {q.most_common_error_types or '—'}.")
    if not sla_list and not quality_list:
        st.info("Run the **closure_sla_quality** job to populate closure health automatically. No data yet.")

    # Validation status by business unit
    st.markdown('<p class="section-title">Validation status by business unit</p>', unsafe_allow_html=True)
    if audit_list:
        df_audit = _models_to_dataframe(
            audit_list,
            ["business_unit", "validation_status", "file_count", "last_processed"],
            lambda x: [
                x.business_unit or "",
                x.validation_status,
                x.file_count,
                x.last_processed.strftime("%Y-%m-%d %H:%M") if x.last_processed else "",
            ],
        )
        st.dataframe(df_audit, use_container_width=True, hide_index=True)
    else:
        st.info("No audit data for the selected filters. Upload files or run the pipeline to see validation status by BU.")

    # Global financial closure sent
    st.markdown('<p class="section-title">Global financial closure sent</p>', unsafe_allow_html=True)
    if global_sent:
        df_global = _models_to_dataframe(
            global_sent,
            ["closure_period", "sent_at", "recipient_email", "job_run_id"],
            lambda x: [
                x.closure_period,
                x.sent_at.strftime("%Y-%m-%d %H:%M") if x.sent_at else "",
                x.recipient_email or "",
                x.job_run_id or "",
            ],
        )
        st.dataframe(df_global, use_container_width=True, hide_index=True)
    else:
        st.info("No global closure send log yet. Run the **global_closure_send** job (or full pipeline) to automate the send.")

    # Rejected files — fix or send to review to unblock approval
    st.markdown('<p class="section-title">Rejected files (need review)</p>', unsafe_allow_html=True)
    if rejected:
        df_rej = _models_to_dataframe(
            rejected,
            ["file_name", "business_unit", "rejection_reason", "processed_at", "moved_to_review_at"],
            lambda x: [
                x.file_name,
                x.business_unit or "",
                x.rejection_reason or "",
                x.processed_at.strftime("%Y-%m-%d %H:%M") if x.processed_at else "",
                x.moved_to_review_at.strftime("%Y-%m-%d %H:%M") if x.moved_to_review_at else "",
            ],
        )
        st.dataframe(df_rej, use_container_width=True, hide_index=True)
    else:
        st.success("No rejected files. All files in scope are valid — approval pipeline unblocked.")

    st.sidebar.markdown("---")
    st.sidebar.caption("Built for Getnet · Databricks · Backend + Pydantic")


if __name__ == "__main__":
    main()
