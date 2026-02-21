# Sample closure files for end-to-end testing

## Contents

- **closure_excel/** — Sample Excel files that match `config/closure_schema.yaml`:
  - **`getnet_closure_test_202502.xlsx`** — Single file for a quick test (BU_A, 5 rows). **Download this file** to test the solution.
  - `closure_bu_a_202502.xlsx`, `closure_bu_b_202502.xlsx`, `closure_bu_c_202502.xlsx` — One per BU for full e2e (all 3 BUs).
  - Columns: amount, currency, account_code, description, value_date, business_unit
  - All rows are valid (amount > 0, date format yyyy-MM-dd, required fields set).

## How to use for e2e testing

1. **Upload to UC volume** (when SharePoint is not configured):
   - Run the `setup_uc` job once to create the schema and volume.
   - From the Databricks UI: Data → Volumes → your catalog → schema → `raw_closure_files`.
   - Create a folder (e.g. `2025-02-20`) and upload the three Excel files.
   - Run the `validate_and_load` job; all three should validate and load into `closure_data`.
   - Run `global_closure_send`; if all BUs have valid files for the current period, the global closure is sent.

2. **Regenerate samples** (optional):
   ```bash
   pip install pandas openpyxl
   python scripts/generate_sample_closure_excel.py --out-dir samples/closure_excel --rows 10
   ```

3. **Invalid sample** (to test rejection): Edit one Excel and set a row's `amount` to 0 or `value_date` to an invalid format, then run validate_and_load to see the file rejected and errors in `closure_audit_errors`.
