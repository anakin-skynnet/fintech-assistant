# Genie Runbook — Getnet Financial Closure

## 1. Financial / global Genie space

**Purpose**: High-level view of closure status, rejections, and closure data for finance and global teams.

**Data sources** (add in Genie space configuration):

- `{catalog}.{schema}.closure_file_audit`
- `{catalog}.{schema}.closure_data`
- `{catalog}.{schema}.global_closure_sent`

**Example questions**:

- Which files were rejected this month?
- Summarize closure by business unit.
- What were the most common validation errors?
- Has the global closure been sent for period 2025-02?

**Setup**: Create a Genie space in the Databricks workspace, attach the SQL warehouse used for closure, add the three tables above as data sources. Grant access to financial and global closure teams.

---

## 2. Genie for business units (rejected files and wrong-field summary)

**Purpose**: Let business units **talk with the data**, **check rejected files**, and see the **summary of wrong fields** (row, field, value, invalid cause) without writing SQL.

**Data sources**:

- `{catalog}.{schema}.closure_file_audit` — one row per file; use for “my rejected files”.
- `{catalog}.{schema}.closure_audit_errors` — one row per validation error; use for “what’s wrong with this file” and “summary of wrong fields”.

The view `closure_audit_errors` parses `validation_errors_summary` JSON into columns: `file_name`, `file_path_in_volume`, `business_unit`, `validation_status`, `processed_at`, `error_row`, `error_field`, `error_value`, `invalid_cause`.

**Example questions for BUs**:

- Show my unit’s rejected files this month.
- List rejected files not yet moved to review.
- Which files from BU_X are still invalid?
- What’s wrong with file `closure_bu_a_2025-02.xlsx`?
- Which fields failed most often in my rejected files?
- List all errors (row, field, value, cause) for my unit’s rejections.

**Filtering**: Filter by `business_unit` and by `processed_at` (or closure period) as needed. Document for BUs how to choose their BU (e.g. from a dropdown or by naming convention).

**Setup**: Create a second Genie space, add `closure_file_audit` and `closure_audit_errors` as data sources. Grant access to BU-relevant groups. Add this runbook (or a short “how to ask Genie” guide) to your internal docs.
