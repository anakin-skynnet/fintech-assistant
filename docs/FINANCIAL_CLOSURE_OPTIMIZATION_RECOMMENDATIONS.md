# Financial Closure Flow — Optimization Recommendations

**Goal**: Minimize human intervention; use **agents** and **automation** for the full flow:

**BU files → check value rules → mark valid/invalid → if valid upload to global closure tables → if invalid notify BU to fix → when all BU files valid, generate global financial closure and notify team via Outlook/Teams.**

---

## 1. Flow alignment (current vs target)

| Step | Current | Recommendation |
|------|--------|----------------|
| 1. BU files | Ingest from SharePoint (Job 1) or app upload | ✅ Keep; ensure BUs re-upload to same path so re-ingestion picks up corrected files. |
| 2. Check value rules | Validate and load (Job 2); all-or-nothing per file | ✅ Keep; rules run automatically, no human. |
| 3. Mark valid/invalid | Audit row: `validation_status` = valid/rejected | ✅ Keep. |
| 4. If valid → upload to global closure tables | Valid rows appended to `closure_data` | ✅ Already automated. |
| 5. If invalid → notify BU to fix | Rejected files moved to SharePoint review folder; **no automatic BU notification** | **Add**: **Notify BU on rejection** (agent-style message + Outlook/Teams). |
| 6. When all BU files valid → generate global closure | Job 4 aggregates, writes file, sends via Outlook | ✅ Keep; add **Teams** as second channel. |
| 7. Notify team (Outlook/Teams) | Outlook only to Financial Lead + global_team | **Add**: **Teams** (webhook or Graph) in addition to Outlook. |

---

## 2. Agent-driven optimizations (avoid human intervention)

### 2.1 Rejection explanation agent (already in codebase)

- **What**: After validation, an **agent** turns `validation_errors_summary` (JSON) into plain-language **rejection_explanation** for BUs.
- **Where**: `src/notebooks/agent_rejection_explanation.py`.
- **Recommendation**: Run it **inside the pipeline** right after **validate_and_load**, before **reject_to_sharepoint**. Then when you notify the BU, the email/Teams message can include this explanation so BUs fix files without opening the app.

**Pipeline order**: `validate_and_load` → **agent_rejection_explanation** → `reject_to_sharepoint` → **notify_bu_on_rejection** → `global_closure_send`.

### 2.2 Notify BU on rejection (agent-style, automated)

- **What**: For each **newly rejected** file (e.g. `validation_status = 'rejected'` and not yet notified), send a **personalized message** to the BU contact with:
  - File name, BU, period
  - **Rejection explanation** (from agent above) or fallback to `rejection_reason` / link to app
  - Link to review folder or app so they can fix and re-upload
- **How**: Use **`config/bu_contacts.yaml`** to resolve `business_unit` → email; send via **Outlook** (existing Graph) and optionally **Teams** (webhook or channel post).
- **Implementation**: New notebook **`notify_bu_on_rejection.py`** (and optional **`teams_send.py`** for Teams); add as a **pipeline task** after `reject_to_sharepoint`.

### 2.3 Global closure summary agent (optional)

- **What**: In Job 4, before or with the send, an **agent** generates a 3–5 bullet **executive summary** of the global closure and attaches it to the Outlook email (and Teams message).
- **Where**: Stub in docs; implement in **global_closure_send** or a small task that runs just before send.
- **Value**: Team gets context at a glance; fewer “what does this file mean?” questions.

### 2.4 Anomaly gate (agent or rules)

- **What**: Before generating/sending the global closure, run an **anomaly check** (e.g. variance vs prior period, unexpected account codes). If **high severity**, **do not send**; notify orchestrator (and optionally Teams channel) with “Global closure blocked: anomalies detected” and link to dashboard.
- **Value**: Prevents sending obviously wrong aggregates; human only steps in to resolve anomaly, not to approve every run.

### 2.5 Proactive BU nudge agent (optional)

- **What**: Scheduled job (e.g. daily): for the **current period**, list BUs with **no valid file** (missing or only rejected). Send a short **nudge** (Outlook/Teams) to each BU from `bu_contacts`: “Your unit has no valid closure file for 2025-02. Please submit or fix and re-upload. [Link].”
- **Value**: Less chasing; BUs are reminded automatically.

---

## 3. Notifications: Outlook + Teams

- **Outlook**: Already used for Financial Lead and global_team; keep as primary.
- **Teams**: Add as **second channel** so the team sees closure status in the same place they work:
  - **Options**: (1) **Incoming webhook** — POST JSON to a Teams channel; (2) **Graph API** — post to a Team channel if you have the app and permissions.
- **Recommendation**: Implement a small **`teams_send.py`** (webhook or Graph) and call it from:
  - **notify_bu_on_rejection**: optional Teams DM or channel “BU X: file F rejected — see email / [link].”
  - **global_closure_send**: after sending Outlook, post to a “Closure” channel: “Global closure for 2025-02 sent. See email for attachment.”

---

## 4. Pipeline and schedule (single flow, no handoffs)

**Recommended pipeline order** (one job, one schedule):

1. **ingest** — SharePoint → volume  
2. **validate_and_load** — rules → valid/invalid, audit + `closure_data`  
3. **agent_rejection_explanation** — fill `rejection_explanation` for rejected files  
4. **reject_to_sharepoint** — move rejected files to review folder, set `moved_to_review_at`  
5. **notify_bu_on_rejection** — for each rejected file, email (and optionally Teams) BU with explanation + link  
6. **global_closure_send** — if all BUs valid: aggregate, write file, send Outlook + Teams, log recipients  

**Optional** (can be added as tasks or separate jobs):

- **Anomaly gate** before `global_closure_send` (run notebook; if high severity, exit and notify; else continue).  
- **BU nudge** job on a separate schedule (e.g. daily) for missing/only-rejected BUs.  

**Schedule**: Run the pipeline daily (e.g. 07:00 UTC); idempotency and “all BUs valid” gate avoid duplicate sends.

---

## 5. Implementation checklist

| Priority | Item | Status / Action |
|----------|------|------------------|
| 1 | **Notify BU on rejection** (notebook + pipeline task) | ✅ **Done**: `notify_bu_on_rejection.py`; task in `closure_pipeline.yml` after `reject_to_sharepoint`. Optional column `notified_bu_at` via `setup_uc_workflow_extension`. |
| 2 | **Rejection explanation agent in pipeline** | ✅ **Done**: Task `agent_rejection_explanation` between `validate_and_load` and `reject_to_sharepoint` in `closure_pipeline.yml`. |
| 3 | **Teams notification** (webhook or Graph) | Add `teams_send.py`; call from notify BU and from `global_closure_send` for “global sent” message. |
| 4 | **Global closure summary agent** | Optional: in Job 4, call LLM to generate short summary; add to email body and Teams post. |
| 5 | **Anomaly gate** | Optional: notebook or step in Job 4 that checks `closure_anomalies` (or similar); block send and notify if high severity. |
| 6 | **BU nudge job** | Optional: scheduled job that checks expected BUs vs audit and emails/Teams BUs with no valid file. |

---

## 6. Summary

- **Flow**: BU files → auto validate → valid → upload to global closure tables; invalid → move to review + **agent explanation** + **auto-notify BU** (Outlook/Teams) → when all valid → generate global closure → **notify team via Outlook and Teams**.
- **Key principle**: Use **agents** for explanations and summaries; use **automation** for notifications and gates so humans only step in when corrections or anomaly resolution are needed.

For existing automation details, see **REDESIGN_AND_ENRICHMENT.md**, **WORKFLOW_ENRICHMENT_AND_AUTOMATION.md**, and **ENRICHMENT_AND_AUTOMATION_ROADMAP.md**.
