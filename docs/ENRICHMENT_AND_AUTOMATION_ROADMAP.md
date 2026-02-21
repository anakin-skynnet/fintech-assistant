# Getnet Financial Closure — Enrichment & Automation Roadmap

This document outlines how to **enrich** the solution with better insights and intelligence, and how to **automate** more of the process to minimize human intervention.

---

## Part 1: Insights and intelligence (Databricks)

### 1.1 Anomaly detection and variance analysis

**What**: Automatically flag closure data that deviates from expected patterns (e.g. BU total drops >15% vs prior period, new account codes, duplicate entries).

**How in Databricks**:
- **Delta Live Tables (DLT)** or a **scheduled job** that:
  - Computes period-over-period metrics (total by BU this month vs last month, % change).
  - Compares to thresholds (from config or historical std dev).
- **Table**: `closure_anomalies` — one row per anomaly (period, BU, metric, expected vs actual, severity).
- **Consumption**: Dashboard, app, and Genie show “Anomalies this period”; optional **alert** (email/Slack) when severity is high.
- **Optional ML**: Use **Databricks ML** (e.g. Isolation Forest or statistical bounds) on historical closure_data to learn “normal” ranges and flag outliers.

**Value**: Financial Lead and BUs see issues before sign-off; fewer manual checks.

---

### 1.2 Forecasting and trend analytics

**What**: Simple forecasts (e.g. next period total by BU) and trend views (month-over-month, quarter-over-quarter).

**How in Databricks**:
- **SQL or Spark**: Aggregate `closure_data` by period and BU; compute rolling averages, YoY/MoM growth.
- **Optional**: **Prophet** or **ARIMA** in a notebook for next-period forecast; write results to a table `closure_forecasts`.
- **Dashboard / App**: Add a section “Trends & forecast” with line charts and a “Expected next period” KPI.

**Value**: Better planning and expectation setting; quick view of “are we on track?”

---

### 1.3 Genie + semantic layer (business terms)

**What**: Let users ask questions in natural language and get answers from closure data using consistent business definitions.

**How**:
- **Genie spaces** (already planned): Add a **semantic model** or **documentation** so Genie understands terms (e.g. “local closure”, “global closure”, “BU”, “period”).
- **Unity Catalog**: Use **table comments** and **column comments** (and optionally a small “business glossary” table) so Genie can explain metrics.
- **Genie suggested questions**: Document 10–15 example questions in the runbook and pin them in the Genie space.

**Value**: Self-serve analytics; less dependency on SQL or dashboards for ad-hoc questions.

---

### 1.4 LLM-powered summaries and explanations (agents)

**What**: Human-readable summaries and suggested fixes so BUs and the Financial Lead spend less time interpreting raw data.

**How** (you already have stubs and config):
- **Rejection explanation**: Enable `config/agent_prompts.yaml` → `rejection_explanation`; run an agent task after Job 2 that generates `rejection_explanation` (and optionally a `.txt` uploaded with the file to the review folder). BUs see “Row 3: amount must be > 0” instead of raw JSON.
- **Global closure summary**: Enable `global_closure_summary`; run an agent task in Job 4 that generates a 3–5 bullet executive summary and attaches it to the Outlook email (or as second attachment).
- **Optional**: An agent that reads **plain-text attachments** (e.g. “correction notes”) and suggests mapping to rows/fields for auditors.

**Value**: Faster correction cycles; clearer communication with the Financial Lead.

---

### 1.5 Data quality metrics and SLA monitoring

**What**: Track “closure health” over time: % files valid, time to first valid file per BU, time to global send.

**How in Databricks**:
- **Tables**: 
  - `closure_sla_metrics`: e.g. period, BU, first_file_at, first_valid_at, hours_to_valid, files_rejected, files_valid.
  - `closure_quality_summary`: period, total_files, pct_valid, pct_rejected, most_common_error_types (from `validation_errors_summary`).
- **Job**: Run after Job 2 (or daily) to compute and upsert these metrics from `closure_file_audit` and `closure_data`.
- **Dashboard / App**: “Closure health” section with SLA and quality trends; **alerts** when e.g. % valid drops below 80% or “time to global send” exceeds N days.

**Value**: Operational visibility; early detection of process or data issues.

---

### 1.6 Recurring reports and subscriptions

**What**: Automatically send key stakeholders a snapshot (e.g. “Closure status as of Friday”) without them opening the app.

**How in Databricks**:
- **Lakeview dashboard**: Create a “Weekly closure status” dashboard; use **dashboard subscriptions** (schedule + email) to send a snapshot to a distribution list.
- **Alternative**: A small **job** that runs weekly, queries audit + closure_data, builds an HTML or PDF report (e.g. with a notebook + library), and sends it via **Outlook/Graph** (reuse your send logic).

**Value**: Reduced “where are we?” emails; consistent, timely reporting.

---

## Part 2: Automation to avoid human intervention

### 2.1 End-to-end pipeline trigger (one workflow)

**What**: A single job that runs in order: Ingest → Validate → Move rejected → (if all BUs valid) Global closure and send. No need to “remember” to run the next step.

**How**:
- **Multi-task job** in the bundle: Task 1 = ingest_sharepoint, Task 2 = validate_and_load (depends on 1), Task 3 = reject_to_sharepoint (depends on 2), Task 4 = global_closure_send (depends on 3). Optionally add **run-if** or a small “gate” notebook before Task 4 that only continues if all BUs are valid (to avoid unnecessary runs).
- **Schedule**: Run this job e.g. every 6 hours or daily; idempotency (already in place) ensures only new/changed files are processed.

**Value**: One schedule, one place to monitor; no manual handoffs.

---

### 2.2 Auto-retry and self-healing for transient failures

**What**: If ingest or validation fails (e.g. SharePoint timeout, temporary API error), retry automatically before notifying humans.

**How**:
- **Job settings**: In the job definition, set **retry policy** (e.g. max 2 retries, exponential backoff). Databricks Jobs support this natively.
- **Notebook logic**: For critical steps (e.g. “all BUs valid?”), use **try/except** and return exit codes; optional **retry loop** inside the notebook for idempotent operations (e.g. list SharePoint again after 5 minutes).

**Value**: Fewer false alarms; many failures resolve without human action.

---

### 2.3 Notifications only when human action is needed

**What**: Notify BUs only when *their* files are rejected; notify the Financial Lead when global closure is sent or when it’s blocked (e.g. missing BU).

**How**:
- **Alerts**: Use **Databricks Alerts** (SQL alert on `closure_file_audit`): e.g. “When new row has validation_status = 'rejected'”, trigger a notification. Route to a **notification channel** (email or webhook to Teams/Slack) with a message that includes file name and link to Genie or the app.
- **Job 4**: After sending global closure, you already log to `global_closure_sent`. Add an **alert** or a **follow-up email** (optional) to the Financial Lead: “Global closure for 2025-02 has been sent.”
- **“Blocked” notification**: In Job 4, when “not all BUs valid”, call a **webhook or send email** (e.g. to a distribution list) with the list of missing BUs and a link to the app, so someone can chase BUs instead of discovering it later.

**Value**: Right people get the right signal at the right time; no need to poll the app.

---

### 2.4 Re-ingestion of corrected files (closed loop)

**What**: When BUs fix files and re-upload to SharePoint (e.g. in the same folder or a “corrected” subfolder), the pipeline automatically picks them up and re-validates without a human “re-running” the job.

**How**:
- **Job 1 (Ingest)**: Already scans the SharePoint folder on a schedule. Ensure BUs re-upload to a path that Job 1 reads (e.g. same “BU Closure” folder or a “Corrected” subfolder that you add to the list of scanned paths). Use **file naming** (e.g. same name overwrites, or v2 suffix) so the new file is copied to the volume; optionally **track last-modified** and overwrite in the volume so the new version replaces the old.
- **Job 2 (Validate)**: Today it processes files that are *not* in the audit table. To allow **re-processing** of the same file name after fix: either (a) allow re-running validation for files that are currently “rejected” and then **update** the audit row and optionally re-load to Delta if now valid, or (b) use a “batch id” or “upload timestamp” in the path so the new file has a new path and gets a new audit row. Option (a) requires a small change: “for this file path, if audit row exists and status is rejected, re-validate and update row (and load to Delta if valid).”

**Value**: True closed loop: fix in SharePoint → automatic re-check → no manual “please run the job again.”

---

### 2.5 Auto-approval or auto-send with guardrails

**What**: When “all BUs valid” and no anomalies (or anomalies below threshold), automatically send the global closure to the Financial Lead without a manual “approve” click.

**How**:
- **Today**: Job 4 already sends when all BUs valid. Add **guardrails** so auto-send is acceptable:
  - **Anomaly check**: Before send, run the anomaly job (or a light version); if any “high” severity anomaly, **do not send** and instead notify “Global closure blocked: anomalies detected” with a link to the app/dashboard.
  - **Optional**: Require “N consecutive runs with all BUs valid” before the first send of the period to avoid sending on a partial state.
- **Audit**: You already log in `global_closure_sent`; ensure alerts/subscriptions notify the Financial Lead when a send happens so they’re aware.

**Value**: Most periods close without human approval; humans only step in when guardrails fire.

---

### 2.6 Proactive BU nudges (optional)

**What**: Remind BUs to submit or correct files when the period is open and they haven’t submitted yet (or have only rejected files).

**How**:
- **Job**: Daily (or twice weekly) job that: (1) lists expected BUs from config, (2) checks audit table for current period, (3) for BUs with no valid file (missing or only rejected), triggers an **email or Teams message** to the BU contact (from a config table or Azure AD group). Message: “Getnet closure: your unit has no valid file for 2025-02. Please submit or fix and re-upload. See [link to app].”
- **Config**: `config/bu_contacts.yaml` or a table `closure_bu_contacts` (BU, email, optional Slack/Teams id).

**Value**: Less chasing; BUs get a clear, timely nudge.

---

## Part 3: Suggested implementation order

| Priority | Item | Effort | Impact |
|----------|------|--------|--------|
| 1 | Single multi-task job (ingest → validate → reject → global send) | Low | High — one schedule, no handoffs |
| 2 | Alerts when files rejected + notification when global sent / blocked | Low | High — right people notified |
| 3 | Re-ingestion of corrected files (re-validate rejected file path) | Medium | High — closed loop |
| 4 | Enable rejection explanation agent (plain-language errors for BUs) | Low | Medium |
| 5 | Enable global closure summary agent (executive summary in email) | Low | Medium |
| 6 | Anomaly detection (variance vs prior period + anomalies table) | Medium | High — better insights |
| 7 | SLA / quality metrics table + “Closure health” in app/dashboard | Medium | Medium |
| 8 | Weekly (or daily) report subscription or email snapshot | Low–Medium | Medium |
| 9 | BU nudge job (email when no valid file for period) | Medium | Medium |
| 10 | Forecasting + trends section in app/dashboard | Medium | Lower (nice-to-have) |

---

## Part 4: What you have today vs. what this adds

| Area | Today | After enrichment / automation |
|------|--------|-------------------------------|
| **Insights** | Dashboard, app, Genie, validation_errors_summary | + Anomalies, trends, forecasts, SLA/quality metrics, LLM summaries |
| **Automation** | 4 separate jobs on schedules | + One pipeline job, retries, re-ingestion of corrected files, auto-send with guardrails |
| **Human touchpoints** | BUs check Genie/app; someone runs/supervises jobs; Financial Lead receives email | + Alerts only when action needed; BUs nudged; re-upload triggers re-check; optional auto-send with anomaly guardrails |

Implementing **1, 2, 3, 4, 5** gives the largest reduction in human intervention and clearest added intelligence with modest effort; **6 and 7** deepen insights for the Financial Lead and operations.
