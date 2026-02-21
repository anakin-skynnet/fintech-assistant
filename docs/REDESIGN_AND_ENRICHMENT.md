# Financial Closure — Redesign, Enrichment & Automation

This document aligns the solution with your **full workflow** (BU → SharePoint → Reviewer → Correction → Approval → Orchestrator → Global Report → Global Team), summarizes **what was redone and optimized**, and gives concrete ways to **enrich with insights/intelligence** in Databricks and **automate** to minimize human intervention.

---

## 1. Your workflow (canonical)

| Step | Actor | Action |
|------|--------|--------|
| 1 | **BU** | Drops Excel files in SharePoint |
| 2 | **Reviewer** | Reviews those files |
| 3 | **Reviewer** | Sends wrong files back to BU for correction |
| 4 | **BU** | Corrects and re-uploads to SharePoint |
| 5 | **Reviewer** | Double-checks local financial closures (per BU) |
| 6 | **Reviewer** | Approves documents |
| 7 | **Supervisor / Orchestrator** | Creates global financial report from each BU’s information |
| 8 | **Orchestrator** | Creates a new financial closure from a global perspective |
| 9 | **System / Orchestrator** | Notifies the global team with the final report |

---

## 2. What was redone and optimized

### 2.1 Pipeline and data flow (unchanged, clarified)

- **Ingest** (Job 1): Lists SharePoint BU folder, downloads Excel (and attachments) into UC volume `raw_closure_files` by date.
- **Validate and load** (Job 2): Validates each file (schema + rules), writes one row per file to `closure_file_audit` (valid/rejected + `validation_errors_summary`). Valid files are appended to `closure_data`. Rejected files can be re-ingested after correction (same path re-validated).
- **Reject to SharePoint** (Job 3): Moves rejected files to the SharePoint **review** folder and sets `moved_to_review_at` in the audit table so the reviewer can send them to BU.
- **Global closure send** (Job 4): Aggregates `closure_data` for the period, writes the global file to `global_closure_output`, sends email to **Financial Lead** and **global team**, logs in `global_closure_sent` and `global_closure_recipients`.

### 2.2 Changes implemented (align with workflow)

| Change | Description |
|--------|-------------|
| **Optional reviewer-approval gate** | Job 4 (`global_closure_send`) has a widget **`require_reviewer_approval`** (default `false`). When `true`, the job only runs when every expected BU has at least one **valid** file with **`approval_status = 'approved'`** in `closure_file_audit`. Requires running `setup_uc_workflow_extension` once so the audit table has `approval_status`. |
| **Notify global team** | After sending to the Financial Lead, Job 4 sends the same report (attachment + body) to every address in **global_team** (from `config/closure_roles.yaml`, or secret `global_team_emails`, or widget `global_team_emails`). |
| **Recipients log** | Each send (Financial Lead + each global_team member) is logged in **`global_closure_recipients`** (closure_period, sent_at, recipient_email, recipient_role, job_run_id). Table is created by `setup_uc_workflow_extension`. |
| **Global-team-notified timestamp** | When global_team is notified, the row written to `global_closure_sent` sets **`global_team_notified_at`** (column added by the workflow extension). |

### 2.3 Config and setup

- **Roles**: `config/closure_roles.yaml` — `reviewers`, `orchestrator`, `financial_lead`, `global_team` (list of emails).
- **BU contacts**: `config/bu_contacts.yaml` (optional) — for “send wrong files to BU” and BU nudges.
- **Workflow extension**: Run **`setup_uc_workflow_extension`** once per environment to add reviewer/approval columns to `closure_file_audit`, orchestrator/global-team columns to `global_closure_sent`, and table `global_closure_recipients`.

---

## 3. How to enrich the solution in Databricks (insights & intelligence)

These additions give the **best insights and intelligence** for the financial closure process.

### 3.1 Already in place

- **Document flow**: Ingested → valid → rejected → moved to review (app / dashboard).
- **Validation errors**: `validation_errors_summary` per file; error analysis views.
- **Anomaly detection** (optional job): `closure_anomalies` — variance vs prior period; severity.
- **SLA / quality** (optional job): `closure_sla_metrics`, `closure_quality_summary`; “Closure health” in app.
- **LLM summary**: Optional executive summary in the global closure email (Databricks AI).
- **Genie**: Spaces for financial/BU-facing questions.

### 3.2 Enrichments to add (prioritized)

| Enrichment | Where in Databricks | Value |
|------------|---------------------|--------|
| **Document flow including human steps** | Views / app: “pending review”, “sent to BU”, “approved”. Filter by `approval_status`. | Reviewer and orchestrator see bottlenecks (e.g. many stuck in “sent to BU”). |
| **SLA by role** | New view/table: time from “file in SharePoint” to “reviewer approved” per BU/period; time from “all approved” to “global sent”. | Identifies slow BUs or slow reviewer/orchestrator steps. |
| **Rejection → correction cycle** | Metric: count of files that were rejected then became valid (re-ingestion). Dashboard: “Correction rate” and “Avg cycles to valid” per BU. | Shows which BUs need better templates or guidance. |
| **Anomaly gate before global send** | In Job 4 (or a gate notebook): run anomaly check; if high severity, **do not send** and notify orchestrator. | Prevents signing off with unexplained variances. |
| **Trends and forecast** | Aggregate by period (and BU); MoM/QoQ; optional forecast (e.g. next period total). Lakeview + app section “Trends & forecast”. | “Are we on track?” for orchestrator and global team. |
| **Genie by role** | **BU space**: my rejected files, my closure totals. **Reviewer space**: files pending approval, sent to BU, approved; error summary. **Orchestrator space**: all BUs status, anomalies, global sent log, recipients. **Global team space**: read-only global closure sent and summary. | Self-serve answers; fewer “where are we?” emails. |
| **Recurring reports** | Lakeview dashboard subscription (e.g. “Weekly closure status” by email) or a weekly job that builds an HTML/PDF snapshot and sends via Outlook. | Consistent, timely reporting without opening the app. |
| **Business glossary / semantic layer** | Unity Catalog table/column comments; Genie documentation so terms (“local closure”, “global closure”, “BU”) are consistent. | Better Genie answers and onboarding. |

### 3.3 Dashboards and app by role

| Role | What to show |
|------|----------------|
| **BU** | My files (valid/rejected/sent to BU); my closure totals; error breakdown; link to SharePoint/upload. |
| **Reviewer** | Files pending review; files sent to BU (awaiting re-upload); approved count; document flow; validation error summary; action to “mark approved” / “sent to BU” (app or Genie). |
| **Orchestrator** | All BUs status (valid/approved); anomalies; “Ready for global?”; global closure sent log; “Create global & notify”; list of global team recipients. |
| **Global team** | Global closure sent (period, date, link to report); executive summary; optional trends. |

Implement via: **Lakeview dashboards** (one per role or one dashboard with role-based filters) and **Streamlit app** (tabs or “View as: BU / Reviewer / Orchestrator / Global team”).

---

## 4. How to automate most of the process (minimize human intervention)

### 4.1 Already automated

- **Ingest**: Scheduled job reads SharePoint and copies new files to the volume.
- **Validation**: All-or-nothing per file; audit and load without human action.
- **Reject to SharePoint**: Rejected files moved to review folder automatically.
- **Global send**: When all BUs valid (and optionally all approved), global file is created and sent to Financial Lead and global team; recipients logged.

### 4.2 Automation to add (prioritized)

| Automation | What | How |
|------------|------|-----|
| **Auto-send wrong files to BU** | When a file is rejected, notify the corresponding BU with rejection reason and link to review folder. | After Job 2 or 3: for each new rejected file, email `bu_contacts[business_unit]` (from `config/bu_contacts.yaml`) with `rejection_explanation` and link. |
| **Auto-notify global team** | ✅ **Done**: Job 4 sends to global_team and logs in `global_closure_recipients`. | Configure `global_team` in `closure_roles.yaml` or secret `global_team_emails`. |
| **Anomaly check before global send** | If high-severity anomaly exists, block send and notify orchestrator. | Gate notebook or step in Job 4: run anomaly job/query; if any high severity, exit and send “Global closure blocked: anomalies detected” to orchestrator. |
| **Proactive BU nudges** | Remind BUs to submit or correct. | Daily (or 2x/week) job: check expected BUs vs audit; for missing or only-rejected, email BU from `bu_contacts`. |
| **Proactive reviewer nudges** | Remind reviewer when many files are pending review. | Alert or job: if “pending_review” count > N or age > 24h, notify reviewers (requires `approval_status` / “pending_review” state). |
| **Re-ingestion of corrected files** | When BU re-uploads to SharePoint, re-validate without manual “run again”. | Job 1 already picks up new/changed files. Job 2 already allows re-validation for paths that are in audit with status “rejected”; re-run validates and updates audit and loads if valid. |
| **Single pipeline** | One job runs: Ingest → Validate → Reject to SharePoint → (optional anomaly) → Global closure send (with optional approval gate). | Use **closure_pipeline**; add optional “gate” task that checks approval and/or anomalies before global send. |

### 4.3 Where humans still add value

- **Reviewer**: Decide “sent to BU” vs “approved” (and optionally mark in app/Genie so `approval_status` is set).
- **Orchestrator**: Trigger or approve global send when using `require_reviewer_approval`; resolve high-severity anomalies before re-running.
- **BU**: Correct data and re-upload to SharePoint; no automation can replace correct numbers.

---

## 5. Quick reference: implementation checklist

| Done | Item |
|------|------|
| ✅ | Optional reviewer-approval gate in Job 4 (`require_reviewer_approval` widget) |
| ✅ | Notify global team from Job 4 (closure_roles / secret / widget) |
| ✅ | Log recipients in `global_closure_recipients`; set `global_team_notified_at` in `global_closure_sent` |
| ✅ | Workflow extension: `setup_uc_workflow_extension` (approval columns, recipients table) |
| ☐ | App / dashboard: document flow with “sent to BU” and “approved”; role-based views |
| ☐ | Auto-send to BU on rejection (using `bu_contacts`) |
| ☐ | Anomaly gate before global send (block or warn) |
| ☐ | BU nudge job (missing or only-rejected) |
| ☐ | Reviewer nudge (pending review backlog) |
| ☐ | Genie spaces and example questions by role |
| ☐ | Recurring report subscription or weekly snapshot email |

---

## 6. Summary

- **Workflow**: The solution now supports the full path — SharePoint → review → send to BU → correct → approve → orchestrator creates global report → notify global team — via optional **reviewer-approval gate**, **global-team notification**, and **recipients log**.
- **Enrichment**: Add **document flow with human steps**, **SLA by role**, **correction-cycle metrics**, **anomaly gate**, **trends/forecast**, **Genie by role**, and **recurring reports** to provide the best insights and intelligence in Databricks.
- **Automation**: **Auto-notify global team** is in place; add **auto-send to BU on rejection**, **anomaly gate**, **BU and reviewer nudges**, and a **single pipeline** with optional gates to automate most of the process and keep human intervention only where judgment is needed (approval, anomaly override, corrections).

For more detail, see **WORKFLOW_ENRICHMENT_AND_AUTOMATION.md** and **ENRICHMENT_AND_AUTOMATION_ROADMAP.md**.
