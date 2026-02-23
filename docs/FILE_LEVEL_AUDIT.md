# File-Level Audit: Excel as a Unit

The financial closure process **tracks and audits each Excel file as a single unit**. There is no partial acceptance: either the entire file is **valid** (and its data is loaded) or the entire file is **invalid** (and it is sent back to the Business Unit with reasons).

---

## Rules

| Case | Result | What happens |
|------|--------|----------------|
| **Every row and value is correct** | File is **valid** | File is flagged as `validation_status = 'valid'`. Its data is **uploaded into the corresponding tables** (`closure_data`). One audit row is stored with `processed_at`, no `rejection_reason`, no `validation_errors_summary`. |
| **Any row or value is missing or wrong** | File is **invalid** | The **whole file** is flagged as `validation_status = 'rejected'`. **No rows** from that file are loaded. An audit row is stored with **date** (`processed_at`) and **reason** (errors detected): `rejection_reason` and `validation_errors_summary` (detailed list of row, field, value, invalid_cause). The file is **sent back to Business Units** (moved to the SharePoint review folder; reviewer can forward to BU). |

So: **one bad row or one bad value → whole file invalid**; only **perfect** files contribute data to the closure tables.

---

## What is persisted (audit)

For **every** ingested file, one row is written to **`closure_file_audit`**:

| Column | Purpose |
|--------|---------|
| `file_name` | Excel file name |
| `file_path_in_volume` | Path in the raw volume (unique key for re-ingestion) |
| `business_unit` | From file content (if present) |
| **`validation_status`** | `'valid'` or `'rejected'` |
| **`rejection_reason`** | Short reason when invalid (e.g. "5 validation error(s)" or "read_error") |
| **`validation_errors_summary`** | JSON array of `{row, field, value, invalid_cause}` for each error (when invalid) |
| `rejection_explanation` | Optional human-readable explanation (e.g. from agent or generated from errors) |
| **`processed_at`** | **Date/time** the file was validated |
| `processed_by_job_run_id` | Job run that processed it |
| `moved_to_review_at` | When the file was moved to the SharePoint review folder (rejected files only) |

Rejected files can be corrected and re-uploaded; the same path is re-validated and the audit row is **updated** (and data loaded if the file becomes valid).

---

## Optimal correction of invalid Excel files

| Step | What to do | Result |
|------|------------|--------|
| 1 | See **Error analysis** and **All files (audit)** in the app for rejection reasons (row, field, invalid_cause). | Clear what to fix. |
| 2 | **Download** the invalid file from the app (or get it from the SharePoint review folder after "Send to review"). | File available locally. |
| 3 | Fix the Excel (correct values, dates, required fields per `config/closure_schema.yaml`). | File ready to re-submit. |
| 4a | **App**: Re-upload via **Upload Excel to raw volume** (same filename, same day → same path). | **One audit row per path**: the existing row is updated (valid/rejected); no duplicate rows. Valid data is appended to `closure_data`. |
| 4b | **Pipeline**: Put corrected file in SharePoint (or same volume path); run **Validate and load** (or full pipeline). | Job **merges** the audit row for that path; valid data is loaded. |

**Implementation**: Both the app (`_upsert_audit_row` in `src/app/backend.py`) and the job (`validate_and_load.py` MERGE for re-ingested paths) use **one row per `file_path_in_volume`**. Re-uploading a corrected file updates that row instead of appending a duplicate, so the correction flow is optimal.

---

## Sending invalid files back to Business Units

1. **Job 3 (Reject to SharePoint)** moves every file with `validation_status = 'rejected'` and `moved_to_review_at IS NULL` to the **SharePoint review folder** and sets `moved_to_review_at`.
2. The **reviewer** (or an automated notification) can then send the file and the **reason** (from `rejection_reason` and `validation_errors_summary` or `rejection_explanation`) to the corresponding Business Unit.
3. The view **`closure_audit_errors`** flattens `validation_errors_summary` into rows (row, field, value, invalid_cause) so BUs and reviewers can query errors per file easily.

---

## When validation is triggered

- **App upload**: When a user uploads an Excel file in the Streamlit app, the file is written to the raw volume and **validation runs immediately** in the same request. The audit table is **upserted by path** (re-upload of a corrected file updates the same row); valid files are appended to `closure_data`.
- **SharePoint ingest**: The **closure_pipeline** job runs Ingest → Validate and Load in order. So whenever files are ingested from SharePoint into the volume, the next step in the pipeline runs validation and updates the audit table. Running the **Validate and Load** job (or the full pipeline) processes all Excel files in the volume and updates the audit accordingly.

## Where it is implemented

- **Validation (all-or-nothing)**: `src/python/validator.py` — `validate_dataframe()` returns a list of errors; if the list is non-empty, the file is treated as invalid.
- **Audit write**: `src/notebooks/validate_and_load.py` — One audit row per file (new rows appended; re-ingested rejected paths **merged**). App backend (`_validate_and_audit` + `_upsert_audit_row` in `src/app/backend.py`) **upserts** by `file_path_in_volume` so re-upload of a corrected file updates the same row. Valid files only are appended to `closure_data`.
- **Send back to BUs**: `src/notebooks/reject_to_sharepoint.py` — Moves rejected files to the review folder; reviewer/BU notification can use `closure_file_audit` and `closure_audit_errors` for the reason (errors detected).

Validation rules (required fields, formats, greater_than_zero, etc.) are defined in **`config/closure_schema.yaml`**.
