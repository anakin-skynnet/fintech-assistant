# Getnet Financial Closure — Get Started Runbook

Step-by-step order of jobs and how to get the solution up and running on Databricks.

---

## Part 1: Get the solution running on Databricks

### Step 1 — Prerequisites

- **Databricks workspace** with Unity Catalog enabled.
- **Catalog** created (e.g. `getnet_closure_dev` or `getnet_closure`). Your user needs **USE CATALOG**, **CREATE SCHEMA**, **CREATE VOLUME**, **CREATE TABLE** on it.
- **Secrets** (optional for first test): You can run without SharePoint/Outlook by uploading Excel files directly to the volume (see Step 5). For full flow, create:
  - Scope **`getnet-sharepoint`**: `tenant_id`, `client_id`, `client_secret`, `sharepoint_site_id`, `sharepoint_drive_id`, `sharepoint_folder_path`, `sharepoint_review_folder_path`
  - Scope **`getnet-outlook`**: `financial_lead_email` (and same Graph credentials if needed for Mail.Send)
- **Databricks CLI** installed and configured (profile pointing to your workspace).

### Step 2 — Deploy the bundle

From the project root:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

For **Azure Databricks**, use your CLI profile:

```bash
databricks bundle validate -t azure_dev --profile <your-azure-profile>
databricks bundle deploy -t azure_dev --profile <your-azure-profile>
```

This creates/updates **jobs**, **notebooks**, and the **app** in the workspace. If the app fails with "already exists", delete the existing app in the UI or change the app name in `resources/apps/financial-closure-app.yml`.

### Step 3 — Run UC setup (once per environment)

Run the **setup_uc** job **once** so the schema, volumes, and tables exist.

1. In the workspace: **Workflows** → **Jobs** → open **Getnet Closure - UC Setup**.
2. Click **Run now**.
3. Use default parameters (or set):
   - **catalog**: e.g. `getnet_closure_dev`
   - **schema**: `financial_closure`
   - **volume_raw**: `raw_closure_files`

This creates:

- Schema `{catalog}.{schema}`
- Volumes: `raw_closure_files`, `global_closure_output`
- Tables: `closure_file_audit`, `closure_data`, `global_closure_sent`, `closure_anomalies`, `closure_sla_metrics`, `closure_quality_summary`
- View: `closure_audit_errors`
- Table: `closure_run_counter` (execution count per job; used as numeric prefix in global closure output file names, e.g. `001_global_closure_2025-02.csv`)

**Optional (workflow extension):** Run the **setup_uc_workflow_extension** notebook once to add reviewer/approval columns and `global_closure_recipients` table. If you don’t have that job, run the notebook `src/notebooks/setup_uc_workflow_extension.py` manually with the same catalog/schema.

### Step 4 — Put test Excel files in the volume

You need at least one Excel file (matching `config/closure_schema.yaml`) in the raw volume so the validate job has something to process.

**Option A — Without SharePoint (direct upload)**

1. In the workspace: **Data** → **Volumes** → open your catalog → schema → **raw_closure_files**.
2. Create a folder with today’s date, e.g. **2025-02-21**.
3. Upload the sample files from **`samples/closure_excel/`**:
   - `getnet_closure_test_202502.xlsx` (single BU test), or
   - `closure_bu_a_202502.xlsx`, `closure_bu_b_202502.xlsx`, `closure_bu_c_202502.xlsx` (full e2e with 3 BUs).

**Option B — With SharePoint**

1. Configure secrets and SharePoint folder (see README).
2. Put the same Excel files in the SharePoint folder that the ingest job uses.
3. Run the **ingest** job so it downloads them into the volume (Step 5).

### Step 5 — Run jobs in order (to test the solution)

Execute jobs in this order. You can run them manually from **Workflows** → **Jobs** or use the single pipeline job.

---

## Part 2: Order of jobs to execute

### One-shot: use the pipeline job

Run **Getnet Closure - Full Pipeline (Ingest → Validate → Reject → Global Send)** once. It runs in order:

1. **Ingest from SharePoint** (if you use SharePoint; otherwise skip by having files already in the volume).
2. **Validate and load** — validates files in the volume, writes audit, loads valid rows into `closure_data`.
3. **Move Rejected to SharePoint Review** — moves rejected files to the review folder (and updates `moved_to_review_at`).
4. **Global Closure and Send** — if all expected BUs have valid files for the period, generates the global file and sends it to the Financial Lead (and logs in `global_closure_sent`).

If you **did not** use SharePoint, run the pipeline anyway: ingest may find nothing; then run **Validate and load** manually if needed so your uploaded files are processed.

---

### Manual order (run one by one)

Use this order when you want to run and test each step separately.

| Order | Job name in UI | Purpose |
|-------|----------------------------------|---------|
| **1** | **Getnet Closure - UC Setup** | Run **once** per environment. Creates schema, volumes, tables, view. |
| **2** | **Getnet Closure - Ingest from SharePoint** | Optional. Only if you use SharePoint; downloads Excel files into the volume. |
| **3** | **Getnet Closure - Validate and Load** | Validates all Excel files in the volume; writes `closure_file_audit`; loads valid rows into `closure_data`. **Run after** files are in the volume (upload or ingest). |
| **4** | **Getnet Closure - Move Rejected to SharePoint Review** | Moves rejected files to the review folder; sets `moved_to_review_at`. Run after validate. |
| **5** | **Getnet Closure - Global Closure and Send** | Runs only when all expected BUs have valid files for the period. Aggregates closure, writes global file, sends email, logs in `global_closure_sent`. Run after validate (and optionally after reject). |

**Supporting jobs (optional, can run anytime after data exists):**

| Job | When to run | Purpose |
|-----|-------------|---------|
| **Getnet Closure - SLA and Quality Metrics** | After validate (or on a schedule) | Fills `closure_sla_metrics` and `closure_quality_summary` for the app “Closure health” section. |
| **Getnet Closure - Anomaly Detection** | After you have at least two periods of data | Writes `closure_anomalies` (e.g. variance vs prior period). |

---

### Minimal test (no SharePoint, no email)

1. **Deploy** the bundle (Step 2).
2. **Run UC Setup** once (Step 3).
3. **Upload** `samples/closure_excel/getnet_closure_test_202502.xlsx` (or the three BU files) to **Volumes** → `raw_closure_files` → e.g. `2025-02-21/`.
4. **Run** **Getnet Closure - Validate and Load**.
5. **Open the app** (Apps → getnet-financial-closure): set catalog/schema (and period `2025-02` if you like). You should see KPIs, document flow, closure by BU, and the uploaded file as valid.
6. (Optional) **Run** **Getnet Closure - Global Closure and Send** — only succeeds if all expected BUs have valid files and `financial_lead_email` secret is set.

---

## Part 3: Verify the solution is running

- **App:** **Apps** → **getnet-financial-closure** → set catalog/schema (and period) → check KPIs, document flow, closure by BU, validation status, rejected files, global sent.
- **Data:** In **Data** → your catalog → schema, check tables **closure_file_audit**, **closure_data**, **global_closure_sent** (and **closure_audit_errors** view).
- **Jobs:** In **Workflows** → **Jobs**, confirm no duplicate job names; run **Validate and Load** again after uploading a new file to see re-ingestion if you use rejected files.

For more detail, see **README.md** (Secrets, Configuration, Jobs) and **docs/TESTING.md** (testing checklist).
