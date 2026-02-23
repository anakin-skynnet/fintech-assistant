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
    PipelineRunStatus,
    RejectedFile,
)


@st.cache_data(ttl=60)
def _load_closure_data(catalog: str, schema: str, period: str, use_mock: bool = False):
    """
    Load all app data from backend. Cached 60s per (catalog, schema, period, use_mock).
    Always returns Pydantic-backed data: on backend error, falls back to mock so UI never breaks.
    use_mock: if True, force mock backend; if False, use real (Spark) when available.
    Returns (use_real_data, fallback_used, kpis, closure_by_bu, flow, err_summary, audit_list,
             global_sent, rejected, audit_files, sla_list, quality_list, pipeline_status).
    """
    fallback_used = False
    try:
        backend, use_real_data = get_backend(catalog, schema, period or None, force_mock=use_mock)
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
        pipeline_status = backend.get_pipeline_status()
    except Exception:
        fallback_used = True
        backend, _ = get_backend(catalog, schema, period or None, force_mock=True)
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
        pipeline_status = backend.get_pipeline_status()
        use_real_data = False
    return (
        use_real_data,
        fallback_used,
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
        pipeline_status,
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

# Google Fonts for polished typography (Databricks Apps / Streamlit)
st.markdown(
    '<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&family=DM+Serif+Display&display=swap" rel="stylesheet">',
    unsafe_allow_html=True,
)

st.markdown("""
<style>
    /* ========== Design system: Financial Closure App ========== */
    :root {
        --bg-dark: #0a0e1a;
        --bg-card: #111827;
        --bg-card-hover: #1a2332;
        --surface: #0f172a;
        --text-primary: #f8fafc;
        --text-secondary: #cbd5e1;
        --text-muted: #94a3b8;
        --accent: #06b6d4;
        --accent-hover: #22d3ee;
        --accent-soft: rgba(6, 182, 212, 0.18);
        --border: #1e293b;
        --border-light: #334155;
        --valid: #34d399;
        --valid-soft: rgba(52, 211, 153, 0.15);
        --rejected: #f87171;
        --rejected-soft: rgba(248, 113, 113, 0.12);
        --warning: #fbbf24;
        --warning-soft: rgba(251, 191, 36, 0.12);
        --info: #38bdf8;
        --radius: 14px;
        --radius-sm: 10px;
        --radius-pill: 9999px;
        --shadow: 0 4px 20px rgba(0,0,0,0.25);
        --shadow-hover: 0 12px 32px rgba(0,0,0,0.35);
        --font-sans: 'Plus Jakarta Sans', 'Inter', system-ui, sans-serif;
        --font-display: 'DM Serif Display', Georgia, serif;
    }

    .stApp {
        background: linear-gradient(160deg, var(--bg-dark) 0%, var(--surface) 45%, #0c1222 100%);
        font-family: var(--font-sans);
    }
    header[data-testid="stHeader"] {
        background: rgba(10, 14, 26, 0.95);
        border-bottom: 1px solid var(--border);
    }
    .main .block-container {
        padding-top: 1.75rem;
        padding-bottom: 3rem;
        max-width: 1520px;
    }

    /* Hero */
    .hero-title {
        font-family: var(--font-display);
        font-size: 2.75rem;
        font-weight: 400;
        color: var(--text-primary);
        letter-spacing: -0.04em;
        margin-bottom: 0.25rem;
        line-height: 1.15;
    }
    .hero-subtitle {
        font-size: 1rem;
        color: var(--text-muted);
        margin-bottom: 0.5rem;
        line-height: 1.5;
        max-width: 640px;
    }
    .hero-meta {
        font-size: 0.8rem;
        color: var(--text-muted);
        opacity: 0.9;
    }

    /* KPI cards */
    .kpi-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 1.4rem 1.5rem;
        text-align: center;
        box-shadow: var(--shadow);
        transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-3px);
        box-shadow: var(--shadow-hover);
        border-color: var(--border-light);
    }
    .kpi-value {
        font-size: 1.9rem;
        font-weight: 700;
        color: var(--accent);
        letter-spacing: -0.03em;
        font-family: var(--font-sans);
    }
    .kpi-label {
        font-size: 0.7rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-top: 0.4rem;
        font-weight: 500;
    }

    /* Section titles */
    .section-title {
        font-size: 1.15rem;
        font-weight: 600;
        color: var(--text-primary);
        margin: 1.75rem 0 0.85rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid var(--border);
        letter-spacing: -0.02em;
        font-family: var(--font-sans);
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
        border-radius: var(--radius-sm);
        padding: 1rem 1.25rem;
        min-width: 120px;
        text-align: center;
        transition: background 0.2s ease, border-color 0.2s ease, transform 0.2s ease;
    }
    .flow-stage:hover {
        background: var(--bg-card-hover);
        border-color: var(--accent);
        transform: scale(1.02);
    }
    .flow-stage .flow-count { font-size: 1.5rem; font-weight: 700; letter-spacing: -0.02em; }
    .flow-stage .flow-label { font-size: 0.68rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.06em; margin-top: 0.25rem; }
    .flow-arrow { color: var(--text-muted); font-size: 1rem; opacity: 0.7; }

    /* Status pills/badges */
    .badge {
        display: inline-block;
        padding: 0.25rem 0.65rem;
        border-radius: var(--radius-pill);
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.03em;
    }
    .badge-valid { background: var(--valid-soft); color: var(--valid); border: 1px solid rgba(52,211,153,0.4); }
    .badge-rejected { background: var(--rejected-soft); color: var(--rejected); border: 1px solid rgba(248,113,113,0.4); }
    .badge-neutral { background: var(--accent-soft); color: var(--accent); border: 1px solid rgba(6,182,212,0.4); }

    /* Tables */
    div[data-testid="stDataFrame"] {
        border-radius: var(--radius-sm);
        overflow: hidden;
        border: 1px solid var(--border);
        box-shadow: 0 2px 12px rgba(0,0,0,0.2);
    }
    div[data-testid="stDataFrame"] th {
        background: var(--bg-card) !important;
        color: var(--text-primary) !important;
        font-weight: 600 !important;
        font-size: 0.8rem !important;
    }
    div[data-testid="stDataFrame"] td { border-color: var(--border) !important; font-size: 0.85rem !important; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #0a0e1a 100%);
        border-right: 1px solid var(--border);
    }
    [data-testid="stSidebar"] .stMarkdown { font-family: var(--font-sans); }
    [data-testid="stSidebar"] .stTextInput label,
    [data-testid="stSidebar"] .stRadio label { color: var(--text-secondary) !important; }
    [data-testid="stSidebar"] .stTextInput input {
        background: var(--bg-card) !important;
        border: 1px solid var(--border) !important;
        color: var(--text-primary) !important;
        border-radius: var(--radius-sm) !important;
    }

    /* Banners */
    .mock-banner {
        background: var(--warning-soft);
        border: 1px solid rgba(251, 191, 36, 0.45);
        border-radius: var(--radius-sm);
        padding: 0.95rem 1.25rem;
        margin-bottom: 1.25rem;
        font-size: 0.9rem;
        color: #fde047;
        font-family: var(--font-sans);
    }
    .banner-error {
        background: var(--rejected-soft);
        border: 1px solid rgba(248, 113, 113, 0.4);
        border-radius: var(--radius-sm);
        padding: 0.95rem 1.25rem;
        margin-bottom: 1.25rem;
        font-size: 0.9rem;
        color: var(--rejected);
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.25rem;
        background: var(--bg-card);
        padding: 0.35rem;
        border-radius: var(--radius-sm);
        border: 1px solid var(--border);
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 0.6rem 1.2rem;
        font-weight: 500;
        font-family: var(--font-sans);
    }
    .stTabs [aria-selected="true"] {
        background: var(--accent-soft) !important;
        color: var(--accent) !important;
    }

    /* Buttons */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, var(--accent) 0%, #0891b2 100%) !important;
        border: none !important;
        font-weight: 600 !important;
        border-radius: var(--radius-sm) !important;
        font-family: var(--font-sans) !important;
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 4px 20px rgba(6, 182, 212, 0.4) !important;
        filter: brightness(1.08);
    }

    /* Expander & alerts */
    .streamlit-expanderHeader { background: var(--bg-card) !important; border-radius: var(--radius-sm) !important; }
    [data-testid="stAlert"] { border-radius: var(--radius-sm) !important; border: 1px solid var(--border) !important; }

    /* Footer */
    .app-footer {
        margin-top: 2.5rem;
        padding-top: 1.25rem;
        border-top: 1px solid var(--border);
        font-size: 0.78rem;
        color: var(--text-muted);
        font-family: var(--font-sans);
    }
</style>
""", unsafe_allow_html=True)


def _models_to_dataframe(items: list, columns: list[str], row_to_list):
    """Build a pandas DataFrame from a list of Pydantic models."""
    if not items:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([row_to_list(x) for x in items], columns=columns)


def main():
    # Sidebar: data source, filters, branding
    with st.sidebar:
        st.markdown("**Getnet** · Financial Closure")
        st.caption("BU → Global automation")
        st.markdown("---")
        st.markdown("**Data source**")
        data_source = st.radio(
            "Source",
            options=["real", "mock"],
            format_func=lambda x: "Real (Unity Catalog)" if x == "real" else "Mock (sample data)",
            index=0,
            key="data_source",
            help="Real: live data from Databricks. Mock: sample data for demos.",
            label_visibility="collapsed",
        )
        use_mock = data_source == "mock"
        st.markdown("---")
        st.markdown("**Filters**")
        catalog = st.text_input(
            "Catalog",
            value=os.environ.get("CLOSURE_CATALOG", "getnet_closure_dev"),
            key="catalog",
            placeholder="getnet_closure_dev",
        )
        schema = st.text_input("Schema", value="financial_closure", key="schema")
        period = st.text_input(
            "Closure period",
            value="",
            key="period",
            placeholder="e.g. 2025-02",
        )
        volume_raw = st.text_input(
            "Raw volume",
            value=os.environ.get("CLOSURE_VOLUME_RAW", "raw_closure_files"),
            key="volume_raw",
            placeholder="raw_closure_files",
        )
        st.markdown("---")
        st.caption("Pipeline: Ingest → Validate → Reject → Notify BU → Global send")

    with st.spinner("Loading closure data..."):
        (
            use_real_data,
            fallback_used,
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
            pipeline_status,
        ) = _load_closure_data(catalog.strip(), schema.strip(), period.strip(), use_mock=use_mock)

    # Hero
    st.markdown('<p class="hero-title">Getnet Financial Closure</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="hero-subtitle">Monitor BU files, validation, and global closure — fix invalid files and automate send.</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<p class="hero-meta">Data as of last load · Refresh the app to fetch latest from backend</p>',
        unsafe_allow_html=True,
    )

    if fallback_used:
        st.markdown(
            '<div class="banner-error">⚠️ Backend error: showing sample data so you can continue. Check catalog/schema and Databricks connection.</div>',
            unsafe_allow_html=True,
        )
    elif not use_real_data:
        if use_mock:
            st.markdown(
                '<div class="mock-banner">📌 Showing sample (mock) data for demo. Switch to <strong>Real (Unity Catalog)</strong> in the sidebar to load live data on Databricks.</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="mock-banner">📌 No mock data in production. On Databricks, this app reads files from the volume and updates the audit and closure tables. Run this app on Databricks to load from Unity Catalog.</div>',
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

    # Tabbed layout for clear navigation (Overview | Files & audit | Analytics | Health)
    tab_overview, tab_files, tab_analytics, tab_health = st.tabs([
        "📊 Overview",
        "📁 Files & audit",
        "📈 Analytics",
        "❤️ Health & status",
    ])

    with tab_overview:
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
        st.markdown('<p class="section-title">BU → Global: document flow & pipeline</p>', unsafe_allow_html=True)
        flow_col, pipe_col = st.columns([3, 1])
        with flow_col:
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
                st.caption("Ingested → valid / rejected → moved to review → notify BU → global send.")
            else:
                st.info("No document flow data yet. Run the pipeline job or upload files to populate.")
        with pipe_col:
            ps = pipeline_status
            state_color = "var(--valid)" if (ps.result_state == "SUCCESS") else ("var(--rejected)" if ps.result_state == "FAILED" else "var(--accent)")
            st.markdown(
                f'<div class="kpi-card"><div class="kpi-value" style="font-size:1rem;color:{state_color};">{ps.state}</div>'
                f'<div class="kpi-label">Pipeline</div><div style="font-size:0.75rem;color:var(--text-muted);margin-top:4px;">{ps.message or ""}</div></div>',
                unsafe_allow_html=True,
            )

    with tab_files:
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
            if use_real_data:
                st.caption("Download a file to fix locally, then re-upload via **Upload Excel to raw volume** above.")
                backend, _ = get_backend(catalog.strip(), schema.strip(), period.strip())
                max_downloads = 25
                download_items = []
                for i, a in enumerate(audit_files):
                    if i >= max_downloads:
                        break
                    if not a.file_path_in_volume:
                        continue
                    ok, data, msg = backend.get_file_bytes_from_volume(a.file_path_in_volume)
                    if ok and data:
                        download_items.append((a.file_name, data, a.file_path_in_volume))
                if download_items:
                    cols = st.columns(min(4, len(download_items)))
                    for idx, (fname, data, path) in enumerate(download_items):
                        with cols[idx % len(cols)]:
                            st.download_button(
                                label=f"⬇ {fname[:24]}{'…' if len(fname) > 24 else ''}",
                                data=data,
                                file_name=fname,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"dl_{path.replace('/', '_')}",
                            )
                if len(audit_files) > max_downloads:
                    st.caption(f"Showing download for the most recent {max_downloads} files.")
            with st.expander("ℹ️ How to fix invalid files", expanded=False):
                st.markdown("**Option 1 (recommended):** Download the file above → fix locally using **Error analysis** (Analytics tab) → re-upload in **Upload Excel to raw volume**. Same file re-upload updates the audit row; if valid, data is loaded.")
                st.markdown("**Option 2:** Use **Send to review** below to move invalid files to the review folder; fix and re-submit, then run **Validate and load** or re-upload in the app.")
        else:
            st.caption("No audit rows for the selected period. Upload files above or run the pipeline job (Ingest → Validate and load) to automate.")
        if use_real_data:
            backend, _ = get_backend(catalog.strip(), schema.strip(), period.strip())
            if st.button("Send invalid files to SharePoint review", type="primary", key="send_to_review"):
                count, message = backend.move_rejected_to_sharepoint("getnet-sharepoint")
                if count > 0:
                    st.success(message)
                else:
                    st.info(message)
            st.caption("One click moves all pending invalid files to the review folder.")

    with tab_analytics:
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
                        margin=dict(l=20, r=20, t=28, b=20),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(17,24,39,0.5)",
                        font=dict(color="#f8fafc", family="Plus Jakarta Sans, system-ui, sans-serif"),
                        showlegend=False,
                        xaxis=dict(gridcolor="rgba(148,163,184,0.12)", zeroline=False),
                        yaxis=dict(gridcolor="rgba(148,163,184,0.12)", zeroline=False),
                        bargap=0.35,
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.dataframe(df_bu, use_container_width=True, hide_index=True)
            with c2:
                st.dataframe(df_bu, use_container_width=True, hide_index=True)
        else:
            st.info("No closure data for the selected filters.")
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

    with tab_health:
        st.markdown('<p class="section-title">Closure health (SLA & quality)</p>', unsafe_allow_html=True)
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
            st.info("Run the **closure_sla_quality** job to populate closure health. No data yet.")
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

    st.markdown(
        '<div class="app-footer">Getnet Financial Closure · Databricks · Backend + Pydantic</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
