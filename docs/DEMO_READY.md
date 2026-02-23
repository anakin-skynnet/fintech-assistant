# Demo-ready setup — Getnet Financial Closure

Use this to get the solution ready for a demo in the minimum number of steps.

---

## Option A: One script (CLI)

From the project root, with **Databricks CLI** configured and **jq** installed:

```bash
./scripts/prepare_demo.sh [profile] [target]
# Examples:
./scripts/prepare_demo.sh              # default profile, target dev
./scripts/prepare_demo.sh myprofile dev
./scripts/prepare_demo.sh azure_getnet azure_dev
```

The script will:

1. Validate and deploy the bundle.
2. Run **Getnet Closure - UC Setup** (once).
3. Run **Getnet Closure - Workflow Extension** (once).
4. Run **Getnet Closure - Demo Bootstrap** (seeds sample Excel in the volume).
5. Run **Getnet Closure - Full Pipeline** (ingest → validate → agent → reject → notify BU → global send).

If your workspace uses bundle prefixes (e.g. `[dev user]` in job names), the script matches jobs by name substring. If a job is not found, run it manually from **Workflows → Jobs** (see Option B).

---

## Option B: Manual steps (UI)

Do these in order in your Databricks workspace.

### 1. Deploy the bundle (once)

From the repo root:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

(Use `--profile <profile>` and `-t azure_dev` if you use Azure and a named profile.)

### 2. Run jobs once in this order

In **Workflows** → **Jobs**:

| Order | Job name (substring to find it) | Action |
|-------|----------------------------------|--------|
| 1 | **Getnet Closure - UC Setup** | **Run now** (creates schema, volumes, tables). |
| 2 | **Getnet Closure - Workflow Extension** | **Run now** (adds reviewer/recipients/notified_bu_at). |
| 3 | **Getnet Closure - Demo Bootstrap** | **Run now** (creates sample Excel files in the raw volume). |
| 4 | **Getnet Closure - Full Pipeline** | **Run now** (ingest → validate → rejection agent → reject to SharePoint → notify BU → global closure send). |

### 3. After the pipeline run

- **App**: Open the **getnet-financial-closure** app to see document flow, validation by BU, global closure sent, and rejected files.
- **Genie**: Use the Genie space (see [genie_runbook.md](genie_runbook.md)) to explore valid files by BU and global closure.
- **Workflows**: Check the last run of the Full Pipeline; all tasks should be green. If **global_closure_send** was skipped (e.g. “not all BUs ready” or “already sent”), run **Validate and load** then **Global closure send** again, or run the Full Pipeline again.

---

## Configuration for a full demo

- **Secrets** (optional for a minimal demo without email/SharePoint):
  - **getnet-outlook**: `financial_lead_email` (and `tenant_id`, `client_id`, `client_secret` if sending email).
  - **getnet-sharepoint**: only if you use SharePoint ingest; otherwise rely on **Demo Bootstrap** to put files in the volume.
- **config/bu_contacts.yaml**: Used when **Notify BU on rejection** runs; ensure each BU has an `email` so rejection emails are sent.
- **Teams**: To post to Teams when global closure is sent or when a file is rejected, set the **teams_webhook_url** widget (or secret) in the pipeline/job.

---

## Sample files

- **In repo**: `samples/closure_excel/` contains `closure_bu_a_202502.xlsx`, `closure_bu_b_202502.xlsx`, `closure_bu_c_202502.xlsx` (valid, 5 rows each). You can upload these to the volume manually if you prefer not to run **Demo Bootstrap**.
- **In volume** (after Demo Bootstrap): Same files are created under `raw_closure_files/<date>/` in your catalog/schema.

---

## Troubleshooting

- **“Not all BUs ready”**: Ensure **Demo Bootstrap** ran and that the current month matches the sample files’ period (e.g. 2025-02). Re-run **Validate and load** then **Global closure send**, or run the Full Pipeline again.
- **“Job not found” in script**: Run the corresponding job from the UI (Option B). In dev, job names may include a prefix like `[dev user]`.
- **Catalog/schema not found**: Run **Getnet Closure - UC Setup** first and ensure the cluster is Unity Catalog–enabled.
