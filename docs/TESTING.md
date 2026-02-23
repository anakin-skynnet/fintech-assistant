# Testing the Getnet Financial Closure App

This document describes how to test and validate the Databricks app and end-to-end flow for financial closure automation.

---

## 1. Local validation (no Databricks)

### 1.1 Backend and models

```bash
cd src/app
python -c "
from backend import get_backend
from models import ClosureKPIs
b, real = get_backend('getnet_closure_dev', 'financial_closure', '2025-02')
k = b.get_kpis()
assert isinstance(k, ClosureKPIs)
assert not real  # mock when no Spark
print('Backend + models OK')
"
```

### 1.2 Run Streamlit locally (mock data)

```bash
cd src/app
pip install -q streamlit pandas plotly pydantic
streamlit run app.py --server.port 8501
```

Then open http://localhost:8501 and verify:

- **Sidebar**: Catalog, Schema, Period; caption "Data from backend (UC or mock)".
- **Banner**: "Using mock data" when Spark is not available.
- **KPIs**: Five cards (Total amount, Closure rows, Files valid, Files rejected, Periods sent) with numeric values.
- **Document flow**: Pipeline with four stages (Files ingested → Valid → Rejected → Moved to review) and counts.
- **Closure by business unit**: Bar chart (left) and table (right); no errors.
- **Error analysis**: Table of field, invalid_cause, count, example_value; or "No validation errors".
- **Closure health**: SLA table and quality summary caption; or "No data yet".
- **Validation status by BU**: Table with business_unit, validation_status, file_count, last_processed.
- **Global financial closure sent**: Table or "No global closure send log yet".
- **Rejected files**: Table or "No rejected files".
- **All files (audit)**: Single table (valid + invalid) with File, Path, BU, Status, Rejection reason, Processed at, Moved to review; download buttons (when on Databricks) and "How to fix invalid files" copy; **Send invalid files to SharePoint review** button when using real backend.

The app uses a cached data loader (TTL 60s) and a loading spinner; changing filters refetches data after cache expiry.

### 1.3 Input validation (SQL safety)

- In the app sidebar, try Catalog = `getnet_closure_dev; DROP TABLE x;` — the value should be sanitized (only alphanumeric and underscore allowed), so the app should still load with default or safe catalog.
- Period = `2025-02` → accepted; `2025/02` or `invalid` → period filter empty (all periods).

---

## 2. On Databricks (deployed app)

### 2.1 Open the app (deployed)

- From the workspace: **Apps** → **getnet-financial-closure** (or the name you deployed).
- Or use the URL: `https://<workspace-host>/apps/<app-name>`.

### 2.2 Test with real data

1. Set **Catalog** and **Schema** to your closure catalog/schema (e.g. `getnet_closure_dev`, `financial_closure`).
2. Optionally set **Closure period** (e.g. `2025-02`).
3. Confirm the mock banner does **not** appear (Spark is available).
4. Verify all sections show data from your tables (or empty states if no data).
5. Change period and confirm KPIs and tables update (cache TTL 60s).

### 2.3 End-to-end flow (with jobs)

1. **Setup**: Run `setup_uc` once; optionally run `setup_uc_workflow_extension` for reviewer/global_team columns.
2. **Sample data**: Upload the sample Excel files from `samples/closure_excel/` to the UC volume `{catalog}.{schema}.raw_closure_files/<date>/`.
3. **Validate**: Run job `validate_and_load`; check that audit and `closure_data` are populated.
4. **Pipeline**: Run job `closure_pipeline` (ingest → validate → reject → global send) or run individual jobs.
5. **App**: Open the app, select the same catalog/schema/period; confirm KPIs, document flow, closure by BU, and (if jobs ran) global closure sent and rejected files match expectations.

---

## 3. Checklist: value and insights

Use this to validate that the app delivers high value for Getnet financial closure:

| Area | What to check |
|------|----------------|
| **KPIs** | Total amount, closure rows, files valid/rejected, periods sent are correct and help prioritize action. |
| **Document flow** | Ingested → valid / rejected → moved to review is clear; bottlenecks (e.g. many rejected) are visible. |
| **Closure by BU** | Chart and table support comparison and allocation review. |
| **Error analysis** | Most common validation failures (field + cause) help BUs fix files and reduce rework. |
| **Closure health** | SLA (time to valid) and quality (% valid/rejected, common errors) support process improvement. |
| **Validation status** | Per-BU valid/rejected counts and last processed support reviewer and orchestrator. |
| **Global closure sent** | Log of when and to whom the global report was sent supports audit and global team. |
| **Rejected files** | List of files needing review with reason and dates supports closed-loop correction. |
| **All files (audit)** | One table for valid + invalid files; download and fix options; "Send to review" moves invalid files to SharePoint — supports accelerating approval. |
| **Automation & reduced intervention** | Subtitle and captions emphasize automating closure, reducing manual steps; empty states point to pipeline jobs; "Send to review" one-click; upload runs validation automatically. |

### Expert UI testing (deployed app)

When testing the deployed Databricks app as an expert tester:

1. **Sidebar**: Change Catalog, Schema, Period, Raw volume; confirm caption and no script injection.
2. **KPIs**: All five cards render; numbers align with document flow (e.g. files valid + rejected ≈ total).
3. **Document flow**: Four stages with counts; caption mentions accelerating approval.
4. **All files (audit)**: Table shows all audit rows; rejection reason truncated; download (real backend) and "Send to review" work; fix options copy is clear.
5. **Closure by BU**: Chart and table match; no empty chart when data exists.
6. **Error analysis**: Caption suggests fixing patterns to improve approval rates; table or success message.
7. **Closure health**: SLA and quality sections; datetimes formatted consistently.
8. **Validation status by BU**, **Global closure sent**, **Rejected files**: Tables with formatted datetimes; empty states are friendly (e.g. "approval pipeline unblocked" when no rejected).
9. **No console errors or Python tracebacks** in app logs.

---

## 4. Common issues

- **Mock banner when on Databricks**: Spark session not available in the app context; check app compute and that the app runs on a cluster with Spark.
- **Empty tables**: Run `setup_uc` and, for SLA/quality, the `closure_sla_quality` job; for workflow columns, run `setup_uc_workflow_extension`.
- **Cache not updating**: Data is cached 60 seconds; change catalog/schema/period or wait 60s to see new data.
- **Permission denied on tables**: Ensure the app’s compute identity has SELECT on the closure schema and its tables/views.
