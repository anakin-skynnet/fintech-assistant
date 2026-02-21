# Deploy Getnet Financial Closure to Azure Databricks

This guide deploys the Getnet Financial Closure bundle to **Azure Databricks** using the Databricks CLI and Asset Bundles.

## Prerequisites

1. **Azure Databricks workspace** with:
   - Unity Catalog enabled
   - A catalog (create one if needed, e.g. `getnet_closure` or `getnet_closure_dev`)
   - Network access to SharePoint/Outlook (or use Azure integration)

2. **Databricks CLI** (>= 0.250.0) installed and configured for your Azure workspace.

3. **Authentication** to Azure Databricks (Azure AD or PAT):
   - [Configure the Databricks CLI for Azure](https://docs.databricks.com/dev-tools/cli/auth.html)

---

## 1. Configure CLI profile for Azure Databricks

Your Azure Databricks workspace URL has the form:

`https://adb-<workspace-id>.<number>.azuredatabricks.net`

Find it in the Azure portal (Databricks resource → Overview → Workspace URL) or in the browser when you open the workspace.

Create or edit `~/.databrickscfg`:

```ini
[azure_getnet]
host = https://adb-xxxxxxxx.xx.azuredatabricks.net
auth_type = azure-cli
# Or use PAT:
# auth_type = pat
# token = <your-personal-access-token>
```

For Azure CLI auth (recommended):

```bash
az login
databricks auth login --host https://adb-xxxxxxxx.xx.azuredatabricks.net
```

---

## 2. Override workspace host (optional)

If you want the bundle to always target Azure, set the host in `databricks.yml` under `workspace` or in the target. By default, the CLI uses the **profile** host when you run deploy. So as long as your profile points to your Azure workspace, you don't need to change the bundle.

---

## 3. Validate the bundle

From the project root:

```bash
# Validate default (dev) target
databricks bundle validate -t dev --profile azure_getnet

# Validate Azure-specific targets
databricks bundle validate -t azure_dev --profile azure_getnet
databricks bundle validate -t azure_prod --profile azure_getnet
```

Fix any reported errors before deploying.

---

## 4. Deploy to Azure Databricks

**Development (azure_dev):**

```bash
databricks bundle deploy -t azure_dev --profile azure_getnet
```

Deploys to:
- Workspace path: `/Users/<your-user>/.bundle/getnet-financial-closure/azure_dev`
- Jobs and notebooks are created/updated in the workspace.

**Production (azure_prod):**

```bash
databricks bundle deploy -t azure_prod --profile azure_getnet
```

Deploys to:
- Workspace path: `/Shared/.bundle/getnet-financial-closure/prod`

---

## 5. After deploy: one-time setup

1. **Create secret scopes** in the Azure Databricks workspace:
   - `getnet-sharepoint` (tenant_id, client_id, client_secret, sharepoint_site_id, sharepoint_drive_id, sharepoint_folder_path, sharepoint_review_folder_path)
   - `getnet-outlook` (financial_lead_email; optionally same Graph credentials)

2. **Run the UC setup job once** (creates schema, volumes, tables, view):
   - In the workspace: Workflows → Getnet Closure - UC Setup → Run now
   - Or: `databricks bundle run setup_uc -t azure_dev --profile azure_getnet`
   - Ensure the job parameters (catalog, schema, volume_raw) match your target (e.g. `getnet_closure_dev` for azure_dev).

3. **Grant permissions** on the catalog/schema to job clusters and users (Unity Catalog).

4. **Create Genie spaces** (optional) as in [genie_runbook.md](genie_runbook.md), using your catalog and schema.

5. **Dashboard**: To deploy the **Getnet Financial Closure Analytics** Lakeview dashboard: add `resources/dashboards/*.yml` to the bundle `include` in `databricks.yml` and set **`warehouse_id`** in your target to your SQL warehouse ID; then redeploy. Alternatively run the **dashboard notebook** (`dashboard_financial_closure`) from the workspace for analytics by business unit and global closure with catalog/schema/period filters (no warehouse required).

6. **App**: The **getnet-financial-closure** Streamlit app is deployed with the bundle. Open it from the workspace **Apps** page or run `databricks bundle run financial_closure_app -t azure_dev --profile <profile>`. Use the sidebar to set Catalog and Schema; the app shows KPIs, closure by BU, validation status, global sent log, and rejected files.

---

## 6. Job schedules and dependencies

| Job                     | Default schedule   | Note                          |
|-------------------------|--------------------|-------------------------------|
| Ingest from SharePoint  | Hourly             | Adjust folder path in secrets |
| Validate and Load       | None (trigger after ingest) | Chain or schedule after ingest |
| Move Rejected to SharePoint | Daily 06:00 UTC |                               |
| Global Closure and Send | Daily 08:00 UTC   |                               |

You can chain **Validate and Load** to run after **Ingest** in the UI (Job → Add dependency) or with a separate schedule.

---

## 7. Azure-specific notes

- **Node type**: Jobs use `Standard_DS3_v2` (Azure VM). Change in `resources/jobs/*.yml` if your subscription uses different SKUs.
- **Spark version**: `13.3.x-scala2.12` (LTS). Adjust in job YAML if your workspace uses another runtime.
- **Libraries**: Each job declares PyPI libraries (msal, requests, pandas, openpyxl, pyyaml); they are installed on the job cluster at runtime.
- **Unity Catalog**: Ensure the workspace is attached to a metastore and you have CREATE privileges on the catalog you use for `getnet_closure_dev` / `getnet_closure`.

---

## 8. Troubleshooting

- **Bundle validate fails**: Ensure the CLI profile host is correct and you are logged in (`databricks auth login --host <your-azure-workspace-url>`).
- **Deploy fails (permission)**: Your user needs permission to create/update workspace files and jobs in the target path.
- **Job run fails (secrets)**: Create the secret scopes and keys; reference them exactly as in the notebooks (e.g. `getnet-sharepoint` / `tenant_id`).
- **Job run fails (import)**: Ensure the job cluster has the required libraries (they are in the bundle YAML; if you changed the job, re-deploy).

For more details, see the main [README.md](../README.md) and [Santander Knowledge Base](https://docs.google.com/document/d/1V0OhY8N9vRto4uPoROhOnAElZTRcfGkxKFy6dAkUkhY/edit?usp=sharing).
