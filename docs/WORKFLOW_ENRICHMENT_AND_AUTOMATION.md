# Financial Closure Workflow — Enrichment & Automation Guide

This document maps your **end-to-end workflow** (BU → Reviewer → Correction → Approval → Orchestrator → Global Report → Global Team) to the current solution, then proposes **enrichments** for insights and intelligence in Databricks, and **automation** to minimize human intervention.

---

## 1. Your workflow (as described)

| Step | Actor | Action |
|------|--------|--------|
| 1 | **BU** | Drops Excel files in SharePoint |
| 2 | **Reviewer** | Reviews those files |
| 3 | **Reviewer** | Sends wrong files back to BU for correction |
| 4 | **BU** | Corrects and re-uploads |
| 5 | **Reviewer** | Double-checks local financial closures (per BU) |
| 6 | **Reviewer** | Approves documents |
| 7 | **Supervisor / Orchestrator** | Creates global financial report from each BU’s information |
| 8 | **Orchestrator** | Creates a new financial closure from a global perspective |
| 9 | **System / Orchestrator** | Notifies the global team with the final report |

---

## 2. Mapping to the current solution

| Workflow step | Current implementation | Gap (if any) |
|---------------|------------------------|---------------|
| 1 – BU drops in SharePoint | Job 1 (Ingest): lists SharePoint folder, downloads to UC volume | ✓ Covered |
| 2 – Reviewer reviews | Job 2 (Validate): auto-validates; rejected files get `validation_errors_summary` and `rejection_explanation`; app/Genie show status | **Gap**: No explicit “reviewer reviewed” state; validation is system-only |
| 3 – Send wrong files to BU | Job 3 (Reject to SharePoint): moves rejected files to “review” folder; reviewer can forward to BU (manual) or you notify BU (notify job) | **Gap**: No explicit “sent to BU for correction” state; no BU contact/notification config per BU |
| 4 – BU corrects and re-uploads | Re-ingestion: same file path (rejected) is re-validated; if valid, audit updated and loaded | ✓ Covered |
| 5 – Reviewer double-checks local closures | App / Genie: closure by BU, validation status, document flow, error analysis | **Gap**: No “reviewer checked” or “local closure OK” flag per BU/period |
| 6 – Reviewer approves | Not modeled | **Gap**: No “approved by reviewer” state; global send today depends only on “all BUs valid” (system validation), not human approval |
| 7–8 – Orchestrator creates global report | Job 4 (Global closure): aggregates `closure_data`, writes global file, sends to Financial Lead | **Gap**: Single recipient (Financial Lead); no explicit “orchestrator approved” or “global closure created” audit for global team |
| 9 – Notify global team | Only Financial Lead gets the email | **Gap**: No “global team” distribution list or log of who was notified |

---

## 3. Enrichments to add in Databricks

These additions give **better insights and intelligence** for the financial closure process and align the data model with the human workflow.

### 3.1 Data model: reviewer and approval states

**Goal**: Track “reviewer reviewed”, “sent to BU for correction”, “reviewer approved” so dashboards and automation can reflect the real process.

| Addition | Description |
|----------|-------------|
| **`closure_file_audit` (new columns)** | `reviewed_at` (TIMESTAMP), `reviewed_by` (STRING, e.g. reviewer email/id), `approval_status` (STRING: `pending_review` \| `sent_to_bu` \| `approved` \| null), `approved_at` (TIMESTAMP), `approved_by` (STRING). For “valid” files, reviewer can mark approved; for “rejected”, system or reviewer can set `sent_to_bu` when file is sent back to BU. |
| **Or new table: `closure_reviewer_actions`** | One row per action: `file_path_in_volume`, `period`, `action` (`reviewed` \| `sent_to_bu` \| `approved`), `acted_at`, `acted_by`. Keeps audit table smaller and gives a full history. |

**Use in Databricks**:
- **Views**: “Files pending reviewer approval”, “Files sent to BU (awaiting re-upload)”, “Approved per BU/period”.
- **App / Lakeview**: Filters by `approval_status`; “Approved by reviewer” KPI; list of files awaiting approval or sent to BU.
- **Genie**: “Which files did the reviewer approve this month?”, “List files sent to BU for correction.”

### 3.2 Data model: orchestrator and global team

**Goal**: Record who created/approved the global closure and who was notified (global team).

| Addition | Description |
|----------|-------------|
| **`global_closure_sent` (new columns)** | `created_by` (STRING, orchestrator id/email), `approved_by` (STRING, optional), `global_team_notified_at` (TIMESTAMP). |
| **New table: `global_closure_recipients`** | `closure_period`, `sent_at`, `recipient_email`, `recipient_role` (e.g. `financial_lead`, `global_team`), `job_run_id`. One row per recipient per send. |

**Use in Databricks**:
- **App / Dashboard**: “Global closure sent” section shows creator and “Global team notified at”; optional table of recipients by role.
- **Genie**: “When was the global team last notified for 2025-02?”

### 3.3 Config: roles and contacts

**Goal**: Drive notifications and role-based views from one place.

| Addition | Description |
|----------|-------------|
| **`config/bu_contacts.yaml`** | `bu_contacts`: list of `{ business_unit, email, name }` for “send wrong files to BU” and BU nudges. |
| **`config/closure_roles.yaml`** | `reviewers`: list of emails/ids; `orchestrator`: email/id; `financial_lead`: email; `global_team`: list of emails (or a distribution list name). |

Use these in jobs (notify_closure_status, BU nudge, global send) and in the app to show “who does what”.

### 3.4 Insights and intelligence in Databricks

**Goal**: Best possible insights for each actor (BU, Reviewer, Orchestrator, Global team).

| Insight | Where in Databricks | Value |
|--------|---------------------|--------|
| **Document flow funnel** | Already in app: ingested → valid → rejected → moved to review. **Enrich**: Add “sent to BU” and “approved” so the funnel matches the workflow. | Reviewer and orchestrator see bottlenecks (e.g. many stuck in “sent to BU”). |
| **SLA by role** | **New view or table**: Time from “file in SharePoint” to “reviewer approved” per BU/period; time from “all approved” to “global sent”. | Identifies slow BUs or slow reviewer/orchestrator steps. |
| **Rejection and correction cycle** | **Metric**: Count of files that were rejected then became valid (re-ingestion). **Dashboard**: “Correction rate” and “Avg cycles to valid” per BU. | Shows which BUs need more guidance or better templates. |
| **Anomaly detection** | Already: `closure_anomalies` (variance vs prior period). **Enrich**: Run before global send; block or warn if high severity. | Orchestrator and Financial Lead see anomalies before sign-off. |
| **Quality and SLA summary** | Already: `closure_quality_summary`, `closure_sla_metrics`; “Closure health” in app. **Enrich**: Add “% approved by reviewer” and “Time to approval” when approval is modeled. | Single pane for “closure health” including human steps. |
| **Trends and forecast** | **New**: Aggregate by period (and BU); MoM/QoQ; optional forecast (e.g. next period total). Lakeview + app section “Trends & forecast”. | Planning and “are we on track?” for orchestrator and global team. |
| **Genie by role** | **Spaces**: (1) **BU space**: my rejected files, my closure totals, correction history. (2) **Reviewer space**: files pending approval, sent to BU, approved; error summary. (3) **Orchestrator space**: all BUs status, anomalies, global sent log, recipients. (4) **Global team space**: read-only global closure sent and summary. | Self-serve answers and less “where are we?” emails. |
| **LLM summaries** | Already: rejection explanation, global closure summary in email. **Optional**: Weekly digest for orchestrator (“This week: X files approved, Y pending, Z sent to BU”). | Less time reading raw tables. |

### 3.5 Dashboards and app by role

| Role | What to show |
|------|----------------|
| **BU** | My files (valid/rejected/sent to BU); my closure totals; error breakdown; link to upload/SharePoint. |
| **Reviewer** | Files pending review; files sent to BU (awaiting re-upload); approved count; document flow; validation error summary; action buttons or link to “mark approved” / “sent to BU” (if you add an action API or app). |
| **Orchestrator** | All BUs status (valid/approved); anomalies; “Ready for global?”; global closure sent log; “Create global & notify” trigger or confirmation; list of global team recipients. |
| **Global team** | Global closure sent (period, date, link to report); executive summary; optional trends. |

Implement via: **Lakeview dashboards** (one per role or one dashboard with role-based filters) and **Streamlit app** (tabs or sidebar “View as: BU / Reviewer / Orchestrator / Global team” using the same backend).

---

## 4. Automation to reduce human intervention

**Goal**: Automate as much as possible so humans only step in where judgment is needed (e.g. approval, anomaly override).

| Automation | What | How |
|------------|------|-----|
| **Auto-send wrong files to BU** | When a file is rejected, notify the corresponding BU (and optionally attach the file or link to review folder). | Use `config/bu_contacts.yaml`; after Job 2 or 3, for each new rejected file, send email to `bu_contacts[business_unit]` with `rejection_explanation` and link. |
| **Auto double-check on re-upload** | When a previously rejected file is re-ingested and becomes valid, no need for reviewer to “remember” to re-check. | Already covered: re-ingestion updates audit and loads data. **Add**: Optional notification to reviewer “BU X re-uploaded; file now valid — please review.” |
| **Auto-approval with guardrails** | When all BUs have valid (and optionally “reviewer approved”) files and no high-severity anomalies, auto-approve for global. | **Option A**: Keep “reviewer approved” as a manual step; **Option B**: If no approval table yet, treat “all BUs valid” as auto-approved and run global send (current behavior). **Option C**: Add “auto_approve_if” in config (e.g. all valid + no high anomalies); then Job 4 runs without waiting for a human “approve” click. |
| **Auto-create global closure** | When all BUs valid (and optionally all approved), create the global report and write to volume. | Already: Job 4 aggregates and writes global file. **Enrich**: Add anomaly check before write; if high severity, skip and notify orchestrator. |
| **Auto-notify global team** | When global closure is sent, send the final report (or link) to the global team list, not only Financial Lead. | **New**: In Job 4, after sending to Financial Lead, loop over `config/closure_roles.yaml` → `global_team` and send email (or one email to a distro list) with the same attachment/summary. Log in `global_closure_recipients`. |
| **Proactive BU nudges** | Remind BUs to submit or correct. | **Job**: Daily (or 2x/week) check expected BUs vs audit; for missing or only-rejected, email BU from `bu_contacts`. |
| **Proactive reviewer nudges** | Remind reviewer when many files are pending review. | **Alert or job**: If “pending_review” count > N or age > 24h, notify reviewers. (Requires “pending_review” state above.) |
| **Single pipeline** | One job runs: Ingest → Validate → Reject to SharePoint → (optional) Rejection explanation agent → (optional) SLA/quality + anomaly) → If all BUs valid (+ guardrails) → Global closure send → Notify global team. | Use **closure_pipeline**; add tasks for notification to BU, notification to global team, and optional “gate” notebook that checks approval/anomaly before global send. |

### 4.1 Suggested automation order

1. **Notify BU on rejection** (using bu_contacts) — high impact, low effort.  
2. **Notify global team when global is sent** (global_team list + global_closure_recipients log) — high impact, low effort.  
3. **Anomaly check before global send** (block or warn) — high impact, medium effort.  
4. **BU nudge job** (missing or only-rejected) — medium impact, medium effort.  
5. **Reviewer approval state + optional auto-approval** — high impact, higher effort (schema + UI or Genie actions).  
6. **Reviewer nudge** (pending review backlog) — medium impact, low effort once approval state exists.

---

## 5. Implementation checklist (concrete)

- [ ] **Schema**: Add to `closure_file_audit`: `reviewed_at`, `reviewed_by`, `approval_status`, `approved_at`, `approved_by` (or add `closure_reviewer_actions` table).  
- [ ] **Schema**: Add to `global_closure_sent`: `created_by`, `global_team_notified_at`; create `global_closure_recipients`.  
- [ ] **Config**: Add `config/bu_contacts.yaml` and `config/closure_roles.yaml` (reviewers, orchestrator, financial_lead, global_team).  
- [ ] **Jobs**: Notify BU on rejection (after Job 2 or 3); notify global team in Job 4; BU nudge job.  
- [ ] **Pipeline**: Extend closure_pipeline with anomaly gate and global-team notification task.  
- [ ] **App / Dashboard**: Document flow with “sent to BU” and “approved”; role-based views; “Global team notified” and recipients.  
- [ ] **Genie**: Document reviewer and orchestrator spaces and example questions.  
- [ ] **Optional**: Reviewer “approve” / “sent to BU” via app button or Genie (writes to audit or reviewer_actions).

---

## 6. Summary

- **Enrich the solution** by modeling **reviewer** (reviewed, sent to BU, approved) and **orchestrator/global team** (who created, who was notified) in the schema and config.  
- **Best insights in Databricks**: Role-based views and Genie spaces, document flow funnel including human steps, SLA by role, correction cycle metrics, anomalies, quality/SLA summary, and optional trends/forecast.  
- **Automate most** by: auto-notifying BU on rejection, auto-notifying global team on send, anomaly guardrails, BU (and optional reviewer) nudges, and a single pipeline that runs from ingest through global send and notifications, with optional auto-approval when guardrails are met.

This keeps the existing technical flow (SharePoint → volume → validate → reject to review folder → global send) and layers **workflow state**, **role-based insights**, and **targeted automation** so the system supports the full human process with minimal manual steps.
