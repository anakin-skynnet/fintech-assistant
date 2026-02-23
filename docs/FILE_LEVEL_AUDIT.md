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

Rejected files can be corrected and re-uploaded; the same path is re-validated and the audit row is updated (and data loaded if the file becomes valid).

---

## Sending invalid files back to Business Units

1. **Job 3 (Reject to SharePoint)** moves every file with `validation_status = 'rejected'` and `moved_to_review_at IS NULL` to the **SharePoint review folder** and sets `moved_to_review_at`.
2. The **reviewer** (or an automated notification) can then send the file and the **reason** (from `rejection_reason` and `validation_errors_summary` or `rejection_explanation`) to the corresponding Business Unit.
3. The view **`closure_audit_errors`** flattens `validation_errors_summary` into rows (row, field, value, invalid_cause) so BUs and reviewers can query errors per file easily.

---

## When validation is triggered

- **App upload**: When a user uploads an Excel file in the Streamlit app, the file is written to the raw volume and **validation runs immediately** in the same request. The audit table is updated with date, file name, location, status (valid/invalid), and wrong values (if any); valid files are appended to `closure_data`.
- **SharePoint ingest**: The **closure_pipeline** job runs Ingest → Validate and Load in order. So whenever files are ingested from SharePoint into the volume, the next step in the pipeline runs validation and updates the audit table. Running the **Validate and Load** job (or the full pipeline) processes all Excel files in the volume and updates the audit accordingly.

## Where it is implemented

- **Validation (all-or-nothing)**: `src/python/validator.py` — `validate_dataframe()` returns a list of errors; if the list is non-empty, the file is treated as invalid.
- **Audit write**: `src/notebooks/validate_and_load.py` — One audit row per file with `validation_status`, `rejection_reason`, `validation_errors_summary`, `processed_at`; valid files only are appended to `closure_data`. The same logic runs in the app backend after upload (`_validate_and_audit` in `src/app/backend.py`).
- **Send back to BUs**: `src/notebooks/reject_to_sharepoint.py` — Moves rejected files to the review folder; reviewer/BU notification can use `closure_file_audit` and `closure_audit_errors` for the reason (errors detected).

Validation rules (required fields, formats, greater_than_zero, etc.) are defined in **`config/closure_schema.yaml`**.
