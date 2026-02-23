# Genie Runbook — Getnet Financial Closure

## 1. Valid files by BU & Global Financial Closure (recommended first space)

**Purpose**: Explore **valid financial files** from business units and **generate or inspect the Global Financial Closure**. One space for both BU-level valid data and global send status.

**Data sources** (replace `{catalog}` and `{schema}` with yours, e.g. `getnet_closure_dev.financial_closure`):

- `{catalog}.{schema}.closure_file_audit` — one row per file; filter `validation_status = 'valid'` for valid files by BU.
- `{catalog}.{schema}.closure_data` — valid closure rows with `business_unit`, `source_file_name`, `amount`, `closure_period`; use to explore valid financial data and aggregate by BU.
- `{catalog}.{schema}.global_closure_sent` — when and to whom the global closure was sent (generate/report side).
- `{catalog}.{schema}.closure_audit_errors` — validation error details for rejected files (optional; for “why invalid”).

**Example questions**:

- Which files are valid this month?
- Summarize closure by business unit from valid files.
- Show valid files per BU for period 2025-02.
- What is the total amount by business unit from valid files?
- Has the global financial closure been sent for 2025-02?
- List valid source files and total amount by business unit.
- What were the most common validation errors in rejected files?

**Setup**: Create a Genie space in the workspace, attach your SQL warehouse, add the four tables above as data sources. Grant access to financial and BU/reviewer teams. You can use the script below after running `setup_uc` and setting `CATALOG`, `SCHEMA`, and (optionally) `WAREHOUSE_ID`.

---

## 2. Financial / global Genie space (alternative)

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

## 3. Genie for business units (rejected files and wrong-field summary)

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

**Setup**: Create a second Genie space, add `closure_file_audit` and `closure_audit_errors` as data sources. Grant access to BU-relevant groups. Add this runbook (or a short how-to-ask-Genie guide) to your internal docs.

---

## Creating the "Valid files by BU & Global Closure" space (after setup_uc)

1. Run **setup_uc** so that `closure_file_audit`, `closure_data`, `global_closure_sent`, and `closure_audit_errors` exist in your catalog and schema.
2. In the Databricks workspace: **Genie** → **Create space** (or **New space**).
3. **Name**: e.g. `Financial Closure — Valid files by BU & Global Closure`.
4. **Warehouse**: Select the SQL warehouse used for closure queries.
5. **Data sources**: Add the four tables (e.g. `getnet_closure_dev.financial_closure.closure_file_audit`, same for `closure_data`, `global_closure_sent`, `closure_audit_errors`).
6. **Permissions**: Grant **Can run** (or **Can edit**) to financial and BU/reviewer groups.
7. Pin or document the example questions from section 1 so users can start with "Which files are valid?" and "Summarize closure by business unit."
