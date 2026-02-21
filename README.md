# Getnet Financial Closure Automation

Databricks Asset Bundle that automates the Getnet financial closure process: ingest Excel files from SharePoint, validate (all-or-nothing) with detailed error summaries, load valid data to Delta, move rejected files to a SharePoint review folder, and generate and email the global closure to the Financial Lead.

**Business context**: [Santander Knowledge Base](https://docs.google.com/document/d/1V0OhY8N9vRto4uPoROhOnAElZTRcfGkxKFy6dAkUkhY/edit?usp=sharing) — use for field names, validation rules, and closure logic.

---

## Architecture (summary)

1. **Job 1 (Ingest)** — Lists the SharePoint BU folder, downloads new Excel (and attachments) to a UC volume.
2. **Job 2 (Validate and load)** — **File-level audit**: each Excel is a unit; if any row or value is wrong or missing, the **whole file** is flagged invalid (date + reason persisted), and only **perfect** files are loaded. One audit row per file with `validation_status` (valid/rejected), `rejection_reason`, `validation_errors_summary`, `processed_at`. See **[docs/FILE_LEVEL_AUDIT.md](docs/FILE_LEVEL_AUDIT.md)**.
3. **Job 3 (Reject to SharePoint)** — Moves rejected files (and attachments) to the SharePoint “review” folder and sets `moved_to_review_at`.
4. **Job 4 (Global closure)** — When all expected BUs have valid (and optionally reviewer-approved) files for the period, aggregates closure data, writes the global file, sends it via Outlook to the **Financial Lead** and **global team** (from `config/closure_roles.yaml` or secret), and logs in `global_closure_sent` and `global_closure_recipients`.

**Genie** — Two spaces: (1) Financial/global view over audit and closure tables; (2) **BU-facing Genie** where business units can talk with the data, check rejected files, and see the summary of wrong fields (use view `closure_audit_errors`).

**Dashboard** — A **Financial Closure Analytics** dashboard (Lakeview) and a **dashboard notebook** provide analytics by business unit and global financial closure: closure by BU, validation status, global closure sent log, and rejected files.

**Databricks App** — A **Streamlit app** (**getnet-financial-closure**) presents the same information to end-users in a polished UI.

**Enrichment & automation** — **[docs/REDESIGN_AND_ENRICHMENT.md](docs/REDESIGN_AND_ENRICHMENT.md)** summarizes the **full workflow** (BU drops in SharePoint → reviewer reviews → wrong files to BU → correction → approval → orchestrator creates global report → notify global team), **what was redone** (approval gate, global-team notification, recipients log), **how to enrich** in Databricks (insights, intelligence), and **how to automate** to avoid human intervention. See also **[docs/WORKFLOW_ENRICHMENT_AND_AUTOMATION.md](docs/WORKFLOW_ENRICHMENT_AND_AUTOMATION.md)** and **[docs/ENRICHMENT_AND_AUTOMATION_ROADMAP.md](docs/ENRICHMENT_AND_AUTOMATION_ROADMAP.md)** for detailed roadmaps.

**Testing** — See **[docs/TESTING.md](docs/TESTING.md)** for local validation, deployed app checks, end-to-end flow, and a value/insights checklist.

**Get started** — See **[docs/GET_STARTED_RUNBOOK.md](docs/GET_STARTED_RUNBOOK.md)** for the **order of jobs to execute**, step-by-step setup (deploy → UC setup → upload test files → run jobs), and a minimal test without SharePoint/email.

---

## Prerequisites

- Databricks workspace with Unity Catalog.
- **Secrets** (create scopes and keys as below).
- **SharePoint**: App registration with **Sites.Read.All** (or **Sites.Selected**) and **Files.Read.All**; same app can be used for **Mail.Send** if sending as a user.

---

## Secrets

Create secret scopes and store the following (do not commit secrets).

### Scope: `getnet-sharepoint`

| Key | Description |
|-----|-------------|
| `tenant_id` | Azure AD tenant ID |
| `client_id` | App (client) ID |
| `client_secret` | Client secret |
| `sharepoint_site_id` | SharePoint site ID |
| `sharepoint_drive_id` | Drive (document library) ID |
| `sharepoint_folder_path` | Folder path where BUs drop files (e.g. `BU Closure`) |
| `sharepoint_review_folder_path` | Folder path for rejected files (e.g. `Review`) |

### Scope: `getnet-outlook` (or reuse `getnet-sharepoint`)

| Key | Description |
|-----|-------------|
| `financial_lead_email` | Email address of the Financial Lead |
| `global_team_emails` | (Optional) Comma-separated emails for global team notification when global closure is sent (overrides `config/closure_roles.yaml` when set) |
| (Optional) Same `tenant_id`, `client_id`, `client_secret` if using same app for send |

For **sending email**, the app needs **Mail.Send** (application permission) or the job must use a user context with send-as. If using a shared mailbox, configure the app to send as that user as per your tenant setup.

---

## Configuration

- **`config/closure_schema.yaml`** — Column names, types, and validations (not_null, greater_than_zero, date_format, etc.). Align with the Santander doc.
- **`config/validation_rules.yaml`** — Rule labels and `max_errors_per_file`.
- **`config/business_units.yaml`** — `expected_bus` list for “all BUs valid” and closure period type.

**Role and contacts config** (for notifications and role-based views): **`config/bu_contacts.yaml`** (BU email per business unit), **`config/closure_roles.yaml`** (reviewers, orchestrator, financial_lead, global_team). Optional workflow extension: run **`src/notebooks/setup_uc_workflow_extension.py`** once to add reviewer/approval columns to `closure_file_audit`, orchestrator/global-team columns to `global_closure_sent`, and table `global_closure_recipients`.

Bundle variables (e.g. in `databricks.yml` targets): `catalog`, `schema`, `volume_raw`, `warehouse_id`, `sharepoint_site_id`, `sharepoint_drive_id`, `sharepoint_folder_path`, `sharepoint_review_folder_path`, `financial_lead_email`, `closure_period_type`.

---

## Setup

1. **Clone and deploy the bundle**
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   ```
   For **Azure Databricks**, use a CLI profile that points to your workspace (`https://adb-<id>.<n>.azuredatabricks.net`) and the `azure_dev` or `azure_prod` target:
   ```bash
   databricks bundle validate -t azure_dev --profile <your-azure-profile>
   databricks bundle deploy -t azure_dev --profile <your-azure-profile>
   ```
   Or run the script: `./scripts/deploy_azure.sh <your-azure-profile> azure_dev`. See **[docs/DEPLOY_AZURE.md](docs/DEPLOY_AZURE.md)** for full steps.

2. **Run UC setup once per environment**
   - Run the `setup_uc` job (or the `setup_uc` notebook) with parameters: `catalog`, `schema`, `volume_raw`.
   - This creates the schema, volumes (`raw_closure_files`, `global_closure_output`), tables (`closure_file_audit`, `closure_data`, `global_closure_sent`, `closure_anomalies`, `closure_sla_metrics`, `closure_quality_summary`), and the view `closure_audit_errors`. If you already ran setup earlier, run it again once to add the new tables.

3. **Python dependencies**: Job definitions include PyPI libraries (msal, requests, pandas, openpyxl, pyyaml) on the cluster; no extra install needed if you use the bundle as-is.

---

## Jobs (runbook)

| Job | Schedule (default) | Description |
|-----|--------------------|-------------|
| `setup_uc` | Manual | Create schema, volumes, audit/closure tables, audit_errors view, closure_anomalies, closure_sla_metrics, closure_quality_summary. |
| **`closure_pipeline`** | Daily 07:00 UTC | **Single pipeline**: Ingest → Validate and load → Reject to SharePoint → Global closure send (with retries). Use this for one-shot e2e. |
| `ingest_sharepoint` | Hourly | Download new files from SharePoint BU folder to volume. |
| `validate_and_load` | After ingest (chain or schedule) | Validate files (including **re-ingestion** of rejected files: re-validate and update audit / load if now valid); write audit; load valid rows to `closure_data`. |
| `reject_to_sharepoint` | Daily 06:00 UTC | Upload rejected files to SharePoint review folder; set `moved_to_review_at`. |
| `global_closure_send` | Daily 08:00 UTC | If all BUs valid for the period, aggregate, write global file (with **execution number prefix**, e.g. `003_global_closure_2025-02.csv`), send via Outlook (with optional **LLM executive summary** in body), log in `global_closure_sent`. |
| `closure_anomaly_detection` | Daily 09:00 UTC | Compare current vs prior period; write `closure_anomalies` for variance above threshold. |
| `closure_sla_quality` | Daily 07:30 UTC | Compute `closure_sla_metrics` and `closure_quality_summary` for closure health. |

**Re-ingestion**: When BUs fix and re-upload files, the same file path (rejected before) is re-validated; if valid, the audit row is updated and data is loaded to `closure_data` (closed loop).

**Chaining**: Use **`closure_pipeline`** for a single schedule, or trigger `validate_and_load` after `ingest_sharepoint` (job dependency or schedule offset).

**Duplicate jobs**: If the workspace shows duplicate job names (e.g. from multiple deploys), run **`python scripts/deduplicate_bundle_jobs.py`** to remove duplicates and keep the latest version of each job. Use **`--dry-run`** to preview; use **`--profile <profile>`** if your CLI uses a non-default profile.

---

## Dashboard (analytics by BU and global closure)

- **Lakeview dashboard** — The dashboard definition is in **`src/dashboards/financial_closure_analytics.lvdash.json`**. To deploy it with the bundle: (1) set **`warehouse_id`** in your target to your SQL warehouse ID, (2) in `databricks.yml` add `resources/dashboards/*.yml` to the `include` list. It shows: total amount by business unit (bar chart), closure by BU and period (table), validation status by BU (table), and global closure sent log (table). The dashboard uses unqualified table names (`closure_data`, `closure_file_audit`, `global_closure_sent`); set your SQL warehouse’s default catalog/schema to the closure catalog/schema, or duplicate the dashboard in the UI and point the queries to your catalog.schema.
- **Dashboard notebook** — Run **`src/notebooks/dashboard_financial_closure.py`** for the same analytics with filters: use widgets to set catalog, schema, and optional closure period. Use this when you want to point at a specific catalog/schema or period without changing the Lakeview dashboard.

---

## Databricks App (end-user UI)

The **getnet-financial-closure** app is a Streamlit app that presents closure analytics in a clean, fintech-style interface:

- **Sidebar**: Catalog, schema, and optional closure period filter.
- **KPIs**: Total amount, closure rows, files valid, files rejected, periods sent (global).
- **Document flow**: Pipeline view (ingested → valid / rejected → moved to review) for closure file flow.
- **Closure by business unit**: Bar chart (total amount by BU) and table with row count and file count.
- **Error analysis**: Validation failure summary by field and cause (from `closure_audit_errors`).
- **Validation status by BU**: Table of valid vs rejected file counts per business unit.
- **Global financial closure sent**: Table of sent periods, recipient, and job run.
- **Rejected files**: Table of files needing review (name, BU, reason, processed/moved dates).
- **Closure health**: SLA metrics (first file, first valid, hours to valid per BU) and quality summary (% valid/rejected, most common error types).

**Data layer**: All data is fetched via a backend service (`src/app/backend.py`) that uses **Pydantic models** (`src/app/models.py`). On Databricks, the backend runs Spark SQL against your Unity Catalog closure tables; when Spark is unavailable (e.g. local run), it returns **mock data** so the UI and flows remain consistent. For a React/TypeScript UI you can use the [shadcn/ui MCP](https://ui.shadcn.com/docs/mcp) to add components; this app uses Streamlit and the same backend pattern for consistency.

After deploying the bundle, open the app from the workspace **Apps** page or run:

```bash
databricks bundle run financial_closure_app -t <target> --profile <profile>
```

The app runs on Databricks compute and reads from your closure catalog/schema; set Catalog and Schema in the sidebar if you use a non-default environment.

---

## Genie spaces

1. **Financial / global** — Create a Genie space with data sources: `{catalog}.{schema}.closure_file_audit`, `{catalog}.{schema}.closure_data`, `{catalog}.{schema}.global_closure_sent`. Use for “Which files were rejected?”, “Summarize closure by BU”, “Most common validation errors”, etc.

2. **Business units** — Create a Genie space for BUs with:
   - `closure_file_audit` — to **check rejected files** (filter by `validation_status = 'rejected'`, `business_unit`, `moved_to_review_at`).
   - `closure_audit_errors` — to see **summary of wrong fields** (row, field, value, invalid_cause) per file.

   **Example questions for BUs**: “Show my unit’s rejected files this month”, “What’s wrong with file X?”, “List all errors (row, field, value, cause) for my rejections”. Document this space and example questions in your internal runbook.

---

## Optional: agents (enabled by default)

- **Rejection explanation** — Implemented in `src/notebooks/agent_rejection_explanation.py`. Run as a task after `validate_and_load`: queries rejected rows where `rejection_explanation` is null, calls Databricks AI (or fallback template), updates `rejection_explanation`. Enabled in `config/agent_prompts.yaml` (`rejection_explanation.enabled: true`).
- **Global closure summary** — Implemented inline in Job 4 (`global_closure_send`): builds metrics from closure data, calls LLM for 3–5 bullet executive summary, uses it as the email body. Enabled in `config/agent_prompts.yaml` (`global_closure_summary.enabled: true`). Standalone notebook `src/notebooks/agent_global_summary.py` is available for custom runs.

## Notifications (rejected / global sent / blocked)

Use **`src/notebooks/notify_closure_status.py`** to send email via Graph API. Parameters: `notification_type` (rejected | global_sent | blocked), `message`, optional `link`, `secret_scope`, `to_email`. Options:
- **After validate_and_load**: Add a task that checks for new rejections and runs this notebook with `notification_type=rejected` and a message listing file names.
- **After global_closure_send**: Add a task that runs with `notification_type=global_sent` when the send succeeds; or from inside Job 4 when blocked (missing BUs), call with `notification_type=blocked` and the list of missing BUs.
- **Databricks SQL Alerts**: Create an alert on `closure_file_audit` (e.g. when `validation_status = 'rejected'` and `processed_at` is recent) and set the action to run the notify notebook or a webhook.

## Sample Excel and end-to-end testing

- **Sample files** — In **`samples/closure_excel/`** you find `closure_bu_a_202502.xlsx`, `closure_bu_b_202502.xlsx`, `closure_bu_c_202502.xlsx` (valid rows matching `config/closure_schema.yaml`). See **`samples/README.md`**.
- **Regenerate** — Run `python scripts/generate_sample_closure_excel.py` (requires `pandas`, `openpyxl`). Options: `--out-dir`, `--bus BU_A BU_B`, `--rows 10`, `--date 2025-02-15`.
- **E2E without SharePoint** — Upload the three sample Excel files to the UC volume `{catalog}.{schema}.raw_closure_files/YYYY-MM-DD/`, then run `validate_and_load`. If all three are valid, run `global_closure_send` (ensure `financial_lead_email` secret is set). Or run the **`closure_pipeline`** job after uploading to the volume (if ingest is skipped or you upload directly to the volume).

---

## Databricks and related repos

This solution runs on **Databricks** (Unity Catalog, Jobs, Apps, Volumes). Useful references from **Databricks Field Solutions**:

- **[databricks-solutions](https://github.com/databricks-solutions)** — Curated implementations, demos, and best practices from Databricks field engineering ([overview](https://github.com/databricks-solutions)).
- **[ai-dev-kit](https://github.com/databricks-solutions/ai-dev-kit)** — Toolkit for AI coding agents (Cursor, Claude, etc.): skills, MCP server, and tools for Databricks development.
- **[databricks-apps-cookbook](https://github.com/databricks-solutions/databricks-apps-cookbook)** — Ready-to-use code snippets for building interactive Databricks Apps.
- **[apx](https://github.com/databricks-solutions/apx)** — Toolkit for building Databricks Apps (Rust).

To give your AI assistant (e.g. **Cursor**) Databricks skills and MCP tools, install the **AI Dev Kit** in this project. It provides:

- **databricks-skills** — 19 markdown skills (Spark pipelines, Jobs, Unity Catalog, Genie, Apps, MLflow, etc.) so the assistant follows Databricks patterns.
- **databricks-mcp-server** — 50+ MCP tools (run SQL, manage jobs, query catalogs, etc.) for executable actions inside the IDE.
- **databricks-tools-core** — Python library for use in your own code (e.g. `execute_sql`, workspace APIs).

**Install for Cursor (project scope, from this repo root):**

```bash
bash <(curl -sL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh) --tools cursor
```

Then follow the prompts (e.g. choose DEFAULT or your Databricks CLI profile). After install, Cursor may need a reload or settings update so it picks up the new MCP server and skills. See the [AI Dev Kit README](https://github.com/databricks-solutions/ai-dev-kit) for global install, Visual Builder App, and options. Content under [databricks-solutions](https://github.com/databricks-solutions) is for reference and education; see each repo for license and support terms.

---

## License and support

Internal use. Align validation rules and columns with the Santander Knowledge Base and your governance.
