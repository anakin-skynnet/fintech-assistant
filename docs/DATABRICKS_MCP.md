# Using Databricks MCP Tools for Financial Closure

The **Databricks MCP** (Model Context Protocol) server exposes tools you can use from Cursor/IDE to query data, run jobs, and manage resources. Use these to complement the app and automate the full financial closure flow (BU → Global).

---

## 1. Fetch data from backend (SQL)

Use **`execute_sql`** to run queries against your closure catalog/schema. Data is the same source as the app (Unity Catalog tables).

**Tool**: `execute_sql`  
**Parameters**: `sql_query`, optional `warehouse_id`, `catalog`, `schema`, `timeout`

**Examples** (replace `getnet_closure_dev` and `financial_closure` with your catalog/schema):

- **Valid files by BU this period**
  ```sql
  SELECT business_unit, validation_status, COUNT(*) AS file_count
  FROM getnet_closure_dev.financial_closure.closure_file_audit
  WHERE date_format(processed_at, 'yyyy-MM') = '2025-02'
  GROUP BY business_unit, validation_status
  ORDER BY business_unit, validation_status
  ```

- **Closure totals by BU**
  ```sql
  SELECT business_unit, closure_period, COUNT(*) AS rows_count, SUM(amount) AS total_amount
  FROM getnet_closure_dev.financial_closure.closure_data
  WHERE closure_period = '2025-02'
  GROUP BY business_unit, closure_period
  ```

- **Global closure sent log**
  ```sql
  SELECT closure_period, sent_at, recipient_email, job_run_id
  FROM getnet_closure_dev.financial_closure.global_closure_sent
  ORDER BY sent_at DESC
  LIMIT 10
  ```

- **Rejected files with explanation**
  ```sql
  SELECT file_name, business_unit, validation_status, rejection_explanation, processed_at
  FROM getnet_closure_dev.financial_closure.closure_file_audit
  WHERE validation_status = 'rejected'
  ORDER BY processed_at DESC
  ```

Use **`catalog`** and **`schema`** parameters so unqualified table names resolve correctly.

---

## 2. Pipeline and job runs

Use **`manage_job_runs`** to trigger or monitor the closure pipeline (Ingest → Validate → Reject → Notify BU → Global send).

**Tool**: `manage_job_runs`  
**Actions**: `run_now`, `list`, `get`, `get_output`, `wait`, `cancel`

- **Trigger pipeline**: `action: "run_now"`, `job_id: <closure_pipeline_job_id>`
- **List recent runs**: `action: "list"`, `job_id: <job_id>`, `limit: 10`
- **Get run status**: `action: "get"`, `run_id: <run_id>`
- **Wait for completion**: `action: "wait"`, `run_id: <run_id>`, `timeout: 3600`

To get `job_id`, use **`manage_jobs`** (list jobs) and find the job named like "Getnet Closure - Full Pipeline (Ingest → Validate → Reject → Global Send)" (or with your bundle prefix).

---

## 3. Volume files (raw closure files)

Use **`list_volume_files`** and **`download_from_volume`** / **`upload_to_volume`** to inspect or manage files in the raw closure volume (same as SharePoint ingest and app upload).

**Tools**: `list_volume_files`, `download_from_volume`, `upload_to_volume`

Volume path format: `/Volumes/<catalog>/<schema>/<volume_name>/<folder>/<file>`.  
Example: `getnet_closure_dev.financial_closure.raw_closure_files` → list files under date folders.

---

## 4. App and Genie

- **`list_apps`** / **`get_app`**: See deployed apps (e.g. getnet-financial-closure).
- **`create_or_update_genie`**: Create or update a Genie space for "Valid files by BU & Global Closure" (see [genie_runbook.md](genie_runbook.md)).

---

## 5. Consistency with the app

- The **app always fetches from the backend** (Databricks Spark when running on Databricks; mock with Pydantic when offline or on error). All app DTOs are Pydantic models.
- MCP **`execute_sql`** runs against the same Unity Catalog tables the app uses. Use it to validate data, build ad-hoc reports, or debug.
- For a **single source of truth**: run the pipeline via MCP or Workflows; the app displays the same audit, closure_data, and global_closure_sent produced by that pipeline.

---

## 6. Quick reference

| Goal | MCP tool | Notes |
|------|----------|--------|
| Query closure tables | `execute_sql` | Set catalog/schema for context |
| Trigger full pipeline | `manage_job_runs` (run_now) | Use closure_pipeline job_id |
| List pipeline runs | `manage_job_runs` (list) | Filter by job_id |
| List raw Excel files | `list_volume_files` | Volume path = catalog.schema.raw_closure_files |
| Deploy or inspect app | `list_apps`, `get_app` | App name: getnet-financial-closure |
