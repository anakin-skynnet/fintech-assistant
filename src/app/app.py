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
    AuditStatusByBU,
    ClosureByBU,
    GlobalClosureSent,
    RejectedFile,
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
        st.markdown("---")
        st.caption("Getnet Financial Closure · Data from backend (UC or mock)")

    backend, use_real_data = get_backend(catalog, schema, period or None)

    # Title
    st.markdown('<p class="main-title">Getnet Financial Closure</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="main-subtitle">Analytics by business unit, document flow, and global closure status</p>',
        unsafe_allow_html=True,
    )

    if not use_real_data:
        st.markdown(
            '<div class="mock-banner">📌 Using mock data — run this app on Databricks with Spark to load from Unity Catalog.</div>',
            unsafe_allow_html=True,
        )

    # KPIs from backend (Pydantic)
    kpis = backend.get_kpis()
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

    # Document flow (new component)
    st.markdown('<p class="section-title">Document flow</p>', unsafe_allow_html=True)
    flow = backend.get_document_flow()
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
    st.caption("Pipeline: ingested → valid / rejected → moved to review.")

    # Closure by business unit
    st.markdown('<p class="section-title">Closure by business unit</p>', unsafe_allow_html=True)
    closure_by_bu = backend.get_closure_by_bu()
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

    # Error analysis (new component)
    st.markdown('<p class="section-title">Error analysis (validation failures)</p>', unsafe_allow_html=True)
    err_summary = backend.get_error_analysis()
    if err_summary.by_field_and_cause:
        df_err = _models_to_dataframe(
            err_summary.by_field_and_cause,
            ["field", "invalid_cause", "count", "example_value"],
            lambda x: [x.field or "", x.invalid_cause or "", x.count, x.example_value or ""],
        )
        st.caption(f"Total validation errors: {err_summary.total_errors} across {err_summary.files_with_errors} file(s).")
        st.dataframe(df_err, use_container_width=True, hide_index=True)
    else:
        st.success("No validation errors in the selected period.")

    # Closure health (SLA + quality)
    st.markdown('<p class="section-title">Closure health</p>', unsafe_allow_html=True)
    sla_list = backend.get_sla_metrics()
    quality_list = backend.get_quality_summary()
    if sla_list:
        df_sla = _models_to_dataframe(
            sla_list,
            ["period", "business_unit", "first_file_at", "first_valid_at", "hours_to_valid", "files_rejected", "files_valid"],
            lambda x: [x.period, x.business_unit or "", x.first_file_at, x.first_valid_at, x.hours_to_valid, x.files_rejected, x.files_valid],
        )
        st.caption("SLA metrics (first file, first valid, hours to valid).")
        st.dataframe(df_sla, use_container_width=True, hide_index=True)
    if quality_list:
        q = quality_list[0]
        st.caption(f"Quality: {q.total_files} files — {q.pct_valid:.1f}% valid, {q.pct_rejected:.1f}% rejected. Most common errors: {q.most_common_error_types or '—'}.")
    if not sla_list and not quality_list:
        st.info("Run the SLA/quality job to populate closure health. No data yet.")

    # Validation status by business unit
    st.markdown('<p class="section-title">Validation status by business unit</p>', unsafe_allow_html=True)
    audit_list = backend.get_audit_status_by_bu()
    if audit_list:
        df_audit = _models_to_dataframe(
            audit_list,
            ["business_unit", "validation_status", "file_count", "last_processed"],
            lambda x: [x.business_unit or "", x.validation_status, x.file_count, x.last_processed],
        )
        st.dataframe(df_audit, use_container_width=True, hide_index=True)
    else:
        st.info("No audit data for the selected filters.")

    # Global financial closure sent
    st.markdown('<p class="section-title">Global financial closure sent</p>', unsafe_allow_html=True)
    global_sent = backend.get_global_closure_sent()
    if global_sent:
        df_global = _models_to_dataframe(
            global_sent,
            ["closure_period", "sent_at", "recipient_email", "job_run_id"],
            lambda x: [x.closure_period, x.sent_at, x.recipient_email or "", x.job_run_id or ""],
        )
        st.dataframe(df_global, use_container_width=True, hide_index=True)
    else:
        st.info("No global closure send log yet.")

    # Rejected files
    st.markdown('<p class="section-title">Rejected files (need review)</p>', unsafe_allow_html=True)
    rejected = backend.get_rejected_files()
    if rejected:
        df_rej = _models_to_dataframe(
            rejected,
            ["file_name", "business_unit", "rejection_reason", "processed_at", "moved_to_review_at"],
            lambda x: [x.file_name, x.business_unit or "", x.rejection_reason or "", x.processed_at, x.moved_to_review_at],
        )
        st.dataframe(df_rej, use_container_width=True, hide_index=True)
    else:
        st.success("No rejected files.")

    st.sidebar.markdown("---")
    st.sidebar.caption("Built for Getnet · Databricks · Backend + Pydantic")


if __name__ == "__main__":
    main()
