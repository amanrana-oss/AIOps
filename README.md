# Synthetic Enterprise Log Simulator and Ops Agents

This FastAPI app is an end-to-end AIOps proof of concept for a synthetic service environment. Its operator workflow connects AG-01 Orchestration, AG-02 Knowledge, AG-08 Monitoring, AG-16 Correlation, AG-20 RCA hypotheses, AG-23 approval-gated response, AG-27 simulated ticketing, and incident reporting. The default workflow is deterministic and evidence-led. Optional loopback-only Ollama reasoning can add an evidence-validated AG-20 hypothesis; no production system is connected or modified.

## Flying Flea Customer Feedback: Local Pipeline

Negative manual complaints now start investigation even if their topic is `unknown`; a missing taxonomy keyword no longer discards them. Failure contractions (`isn't`, `won't`, `couldnt`) and freezing/unresponsive symptoms are recognized by the fallback. Negation matches whole words so `notification` is not treated as `not`. Monitoring routes complaints; it does not repair the vehicle. The existing specialist sequence searches knowledge, evaluates context and proposes guidance or escalation. Empty or unsupported knowledge returns `insufficient_knowledge`, not an invented fix, and ticket creation still requires authorized approval. Generic vague faults may require clarification; enabling a hosted model is separate from implementing these deterministic routing safeguards. Gemini is not configured or implemented by this routing change.

### Hugging Face AI Without Ollama

`customer_ai.py` adds optional hosted Hugging Face Inference Providers chat-completion calls for structured sentiment/topic/intent understanding and evidence-cited answer generation in AG-23. No Ollama installation, local GPU, model download, or extra Python dependency is required for this hosted adapter. It does require a Hugging Face account, inference-provider access/credits where applicable, a suitable available chat model, and a private token.

Configure the server environment with `CX_AI_PROVIDER=huggingface`, `CX_HF_MODEL=<available provider-supported chat model ID>`, and `HF_TOKEN=<private token>`, then restart Uvicorn. `.env.example` contains placeholders; `.env` is not loaded automatically. Never send tokens in chat or store them in the frontend/database. No model ID is hardcoded as universally available. `GET /api/customer-intelligence/ai` reports configuration presence, not successful connectivity or model quality.

Enabling this provider sends redacted feedback and up to eight selected knowledge excerpts to a hosted provider outside the local machine. Obtain approval for this processing, especially for internal SOPs; basic redaction is not a guarantee of anonymization. Leave the provider disabled if external processing is not permitted. This differs from downloading a Hugging Face model for fully local execution, which is not implemented here.

Model outputs are Pydantic validated. Understanding separates intent from sentiment so a neutral support question can require investigation. Opinion-only records remain excluded from automatic fault routing. Resolution receives only retrieved non-stale evidence, requires cited steps, and rejects unknown evidence IDs or contradictory support status. Citation existence does not prove semantic correctness; operator review remains required. Calls are serialized and time bounded, with no autonomous retry loop. Failures are explicitly labelled `ai_failed_fallback_requires_review` or `failed_fallback_requires_review`, never reported as AI success. Fallback manual guidance remains available. Ticket authorization/creation and workflow state remain deterministic.

This is a generation-backed RAG integration over the **existing lexical Chroma retrieval**, not a new multilingual semantic embedding index. Semantic embeddings and retrieval quality evaluation remain follow-up work. Live hosted inference has not been verified without configured credentials; adapter and complete agent-flow tests use clearly identified mock model outputs. No model can guarantee correct understanding of every comment or a solution for every issue.

### Manual PIN Feedback Routing

Manual entry is a feedback source, not a bypass of sentiment analysis. Authentication classification now recognizes bike PIN, passcode/password, PIN reset/change requests, and failure wording such as `couldnt`, `couldn't`, `can't`, and `unable`. The example `how to change the bike pin? i have so many times but i couldnt change it` is classified as a negative authentication issue and enters the existing specialist pipeline without requiring a star rating. PIN how-to requests may also enter knowledge investigation without falsely labelling a neutral request negative. The classifier remains a limited review-required deterministic fallback, not general language understanding.

Uninvestigated inbox records have **Reanalyze feedback**, backed by `POST /api/feedback/{feedback_id}/reanalyze`. If corrected rules now detect an issue, the saved original is reclassified and linked to an investigation without duplicating feedback. Already investigated records must use their investigation rerun instead. Positive/opinion records still do not automatically become technical incidents.

PIN-change retrieval targets the uploaded owner manual's Account/Change Bike PIN instructions. MA-24 can extract procedural PDF/DOCX passages without requiring tracker-style `Solution (FIX)` labels. The PIN answer uses source-derived numbered bullets and the documented completion check; sample keypad digits and adjacent secondary-PIN-sharing procedures are excluded. Citations distinguish manual/SOP passages from historical tracker resolutions. This is documented workflow guidance, not a confirmed root cause or permission to expose or change anyone's actual PIN.

### Uploaded Owner Manual

AG-02 now includes PDF and DOCX reference documents from `data/real-data` as well as existing supported SOP documents from `data/sops`. The uploaded `OM Domestic Full Book 31-08-2026.pdf` is 161.65 MB with 333 pages. It was previously missed because real-data ingestion handled only CSV issue trackers and the default document limit was 20 MB. Reference documents from real-data now have a separate 200 MB bound; the existing chunk-count limit still applies. Other document ingestion retains its default size limit. Raw real-data CSVs are still processed only through the allowlisted, privacy-filtered tracker reader.

Manual references use a `real-data/` source prefix so they cannot collide with SOPs of the same filename. Sources remain unreviewed until their applicability/revision is confirmed. File presence or indexing alone does not establish a correct answer: retrieval and the response planner must cite the matching procedure. `FF OPS -SOP Tracker.xlsx` remains unsupported and is reported as skipped, not silently counted as indexed knowledge.

### Visual Investigation Workbench

In Customer Intelligence, click the **Investigations** metric or an inbox investigation link, then select an issue. The workbench shows linked customer reviews and a seven-stage pipeline: AG-08 Monitoring, AG-01 Orchestration, AG-02 Knowledge, AG-16 Correlation, AG-20 RCA, AG-23 Resolution, and AG-27 Ticket Review. Select a stage to inspect its responsibility, micro-agents, recorded task result, evidence, attempts, timing, and error details. AG-01 is shown from the stored selection reason rather than inventing a separately timed task.

**Run investigation** starts a fresh bounded background routine and refreshes cited knowledge on the incident. The view polls persisted task states; it does not fabricate sequential animation or delay execution to simulate work. Fast tasks may complete between polls. The selected incident/run detail response summarizes repeated input context; complete inputs remain in the execution database. The selector uses compact summaries and latest-run queries are scoped to one incident. Reruns allow at most three concurrent local requests, reject duplicate starts, and have a 150-second execution bound. They are single-process POC jobs, not a durable distributed scheduler. Incidents with a saved ticket decision cannot be rerun through this control.

Knowledge passages are shown with source/row references and review/staleness flags alongside the recommendation. Retrieval is not a confirmed fix: the current planner offers inspection/verification guidance and does not synthesize a guaranteed repair from arbitrary unreviewed tracker text. Where there is no supported answer, **Review ticket proposal** opens the human-gated approval section. It never creates a ticket by itself. Existing proposal content stays immutable during reruns; review its snapshot before approval. Feedback intake behavior remains unchanged in this UI iteration.

### User-Supplied BikeDekho Text

The operator supplied 32 pasted BikeDekho entries on 2026-11-10. A privacy-minimized import of **31 records** is in `data/feedback/flying_flea_bikedekho_user_supplied.json`. The entry dated 2023-11-30 explicitly discussing a **Royal Enfield 350** was excluded from the Flying Flea dataset. No further BikeDekho website access or scraping was performed for this import, and supplying the text does not establish a publisher license or authorize future collection.

Titles/bodies, dates, and fractional ratings are retained as supplied. Names, profile details, helpfulness votes, and interface labels are omitted. Stable local IDs use the supplied date/title rather than inventing publisher review IDs; links point to the source listing, not fabricated review permalinks. Truncated text remains truncated and is marked in `quality_note`; missing Read More text was not reconstructed. Non-ASCII characters from supplied comments are intentionally preserved.

These are early-dated opinions/expectations with unverified Flying Flea applicability, not verified owner reports. Model-ambiguous entries are marked accordingly. All are `public_opinion` or `prelaunch_opinion`, can receive sentiment analysis, and cannot automatically trigger technical incidents. They remain unverified in the non-synthetic dataset; that view is not a verified-ownership count. The current heuristic returned 28 positive and 3 mixed labels for this batch, all requiring review; neither those labels nor high star ratings establish product quality or absence of issues. Both supplied 2.2 ratings are retained. The import created no incidents or tickets. Reimporting the same batch is deduplicated.

### Public Product Reviews: BikeWale Snapshot

On 2026-11-10, the Flying Flea C6 public BikeWale review page was checked through its visible **Most Recent** tab: https://www.bikewale.com/royalenfield-bikes/flying-flea/reviews/ . Eight written reviews appeared on page one. Six were selected in `data/feedback/flying_flea_bikewale_public.json`; promotional-copy record `1202482` and ambiguous engine-focused record `1152586` were excluded. The public second-page route did not expose usable additional records during this check. The site's displayed ten-review count is not an imported record count.

The snapshot omits reviewer identities, preserves public review IDs/permalinks and original title/body, and stores relative publication labels verbatim with the observation date in `quality_note`. Exact publication timestamps and overall ratings were not reliably extracted, so they are not invented or inferred from individual parameter scores. Five records carry **self-reported ownership**, not verified ownership. The record labelled **Never Owned** on the source is `public_opinion` and cannot automatically trigger a fault investigation. All analysis remains review-required, and alleged range figures are customer claims, not validated specifications. Two self-reported range complaints are eligible for the existing all-agent investigation routine and human-gated ticket preview.

Website imports enforce allowlisted Flying Flea EV review paths and `product_experience` scope. Ratings accept fractional values without rounding. `prelaunch_opinion` and `public_opinion` records may be analyzed but are excluded from automatic investigation. This is a one-time public-page collection, not continuous synchronization, an API agreement, or a claim of exhaustive coverage. BikeWale's internal `/api/*` routes are excluded in its robots rules and were not accessed directly by the collector.

BikeDekho and ZigWheels were inspected, but their published terms prohibit copying into another information-retrieval system without permission; **no comments from these sources were imported**. Source status is `permission_required`. Obtain publisher permission or an authorized export before importing their material. Several visible entries on those sites predate launch or discuss ICE models; a Flying Flea page heading alone does not establish genuine EV ownership. Instagram/Facebook/YouTube/X connectors remain unconfigured; no social-media comments were collected in this snapshot.

### Verified EV-Only Public Review Snapshot

On 2026-11-10, public store listings were checked for **Flying Flea App**, the electric-motorcycle companion app:

- Google Play: `com.flyingflea.ffapp`, published by Royal Enfield (A Unit of Eicher Motors), https://play.google.com/store/apps/details?id=com.flyingflea.ffapp&hl=en&gl=IN . The visible review dialog exposed **three written reviews**, each rated five stars. The page displayed an aggregate count of 10 reviews; that does not establish ten retrievable written comments.
- Apple India: `6745692820`, bundle `com.flyingflea.ffapp`, seller EICHER MOTORS LIMITED, https://apps.apple.com/in/app/flying-flea-app/id6745692820 . Apple's lookup returned 10 ratings (4.7 average), but the public India customer-review feed returned **no written entries** during this check. Do not interpret that as zero reviews globally.

A subsequent direct check of Apple's dedicated Ratings & Reviews page found one written two-star review dated 2026-07-08. It was imported into the persistent inbox with a content-derived stable ID and source link, without reviewer identity. The empty RSS result was therefore a feed-coverage limitation, not absence of written App Store feedback. Its mixed functionality complaint triggered an approval-paused customer investigation.

The three observed Google Play comments are stored in `data/feedback/flying_flea_google_play_public.json`, with original text, stable review IDs, rating, displayed date, country query, and listing/review links. Reviewer names are deliberately omitted. This is a one-time public-page snapshot, not an authorized API connector or continuous collection. It does not claim exhaustive coverage. The general Royal Enfield app ID `1472963660`, general Play package, and Flying Flea Mechanic apps are excluded. Store-source imports enforce the verified customer app IDs and URLs. Brand naming in a review is not sufficient by itself; source app identity controls scope.

The Customer Intelligence inbox now defaults to **real feedback only**, with explicit synthetic/all filters and stored sentiment counts and average rating. The snapshot's three comments classify as positive using the existing rating/keyword fallback and correctly create no incident. No synthetic review is presented as scraped data. Source status says `public_snapshot_imported`, never live-connected. Synthetic demo data is retained, not deleted. The older `royal_enfield_reviews.jsonl` remains a separate synthetic AG-08 demo dataset and is not a Flying Flea real-review source.

For further collection use permitted public access or authorized store APIs/exports, respecting platform limits. Do not substitute generic Royal Enfield reviews when Flying Flea written reviews are unavailable. A missing review feed must remain an empty/failure state, not trigger fabricated data. The current snapshot is not a new polling connector.

The **Customer Intelligence** navigation entry provides a persistent feedback inbox, single-review submission, JSON import, optional explicitly synthetic examples, investigation details, and local ticket proposals. Google Play, Apple App Store, and BikeWale have public snapshots; none is continuously synchronized. BikeDekho and ZigWheels require permission, and social-media sources remain unconfigured. Verified customer-app identifiers are known, but authorized API access and scheduled connector implementation are still needed for continuous collection.

Qualifying known-topic complaints follow **AG-08 Monitoring → AG-01 Orchestration → AG-02 Knowledge → AG-16 Correlation → AG-20 RCA → AG-23 Resolution → AG-27 ticket preview → operator approval**. A dedicated customer routine records the agent tasks in Execution Monitor. AG-27 prepares a proposal, not a ticket. Positive reviews do not trigger this workflow. Exact normalized duplicates within one source do not rerun an already analyzed record; recent same-topic complaints can merge through the existing incident deduplication window. This is not semantic clustering.

AG-02 startup ingestion now includes CSV trackers in `data/real-data` through MA-03. Tracker rows carry file/row citations and are marked **unreviewed historical issue evidence**, not approved SOPs. Only allowlisted issue/status/action fields are indexed; reporter, assignee, VIN, and internal-note columns are excluded, with basic email/phone/VIN redaction in included text. Free-text fields still require a human privacy review. Existing supported manuals/SOPs in `data/sops` are ingested as before. XLSX is not supported. Originals are not modified.

Feedback remains separate from the operational log stream. Operational correlation is limited to candidate service/time associations with synthetic telemetry; missing matches do not invalidate a complaint. RCA validates feedback and knowledge references and does not confirm a defect. Recommendations require manual review; synthetic recovery cannot verify a customer resolution. Current analysis is a **keyword/rating fallback**, explicitly marked `requires_review`; it is not multilingual sentiment, emotion analysis, or Qwen semantic classification. Unknown/unrecognized complaints remain reviewable feedback without automatic investigation.

Feedback records, analysis, persisted jobs, immutable proposal snapshots, local tickets, and decision audit events use `data/customer_intelligence.sqlite3`. Originals are immutable; avoid importing unnecessary personal data. Jobs run in a single bounded local worker, with a 200-record batch limit, a 20-job queue, a 180-second job timeout, and restart recovery for interrupted work. This worker assumes one Uvicorn process. Failed jobs expose a failure state rather than a successful analysis.

For approval, set a private `CX_OPERATOR_TOKEN` and `CX_OPERATOR_ID` in the server process environment before starting Uvicorn. See `.env.example` for names only; `.env` files are **not loaded automatically**. Never share credentials in chat or commit them. Enter the operator credential directly in the dashboard's password field for a decision; it is cleared after submission, not saved in browser storage or SQLite. This token is a local POC approval credential, not an external platform API token. Approvals are disabled when it is absent. Rejection requires a reason. Ticket creation is idempotent and uses AG-27 locally; existing generic ticket/approval shortcuts are blocked for customer incidents. Do not expose this unauthenticated local POC to a network.

API entry points:
- `POST /api/feedback/import` with `{"records":[{"source":"manual_import","external_id":"your-record-id","original_content":"Battery status is not refreshing","rating":1}]}` returns a job ID.
- `GET /api/customer-intelligence/jobs/{job_id}` reports completion/failure.
- `GET /api/feedback`, `GET /api/feedback/sources`, and `GET /api/customer-intelligence/overview` supply stored dashboard data.
- `POST /api/customer-intelligence/demo` submits labelled synthetic samples; repeating it does not duplicate analyzed records.
- `GET /api/customer-intelligence/approvals` returns proposals and their evidence snapshots.
- `POST /api/customer-intelligence/approvals/{incident_id}/approve` or `/reject`, with `Authorization: Bearer <operator credential>` and `{"note":"decision reason"}`, records an authorized decision.

This is the first functioning local feedback-to-investigation slice, not the complete CX platform. Live platform connectors, AG-30/MA-31–42, semantic clustering/trends, configurable multilingual embeddings, structured Qwen feedback analysis, release comparison, and the full six-page CX dashboard remain follow-up work. Existing optional AG-20 Ollama reasoning does not make the feedback classifier model-backed.

## Studio Capabilities

The existing Overview, Incidents, Service map, and Activity views remain available. The Studio adds:

- **Routine Studio:** seven seeded investigation/learning routines, trigger rules, editable ordered task nodes, duplication, enable/disable, and manual incident runs. AG-01 selects a matching enabled routine during automatic investigation.
- **Agent Registry:** seven specialist profiles with persisted instructions, model configuration, status, and memory preferences.
- **Micro-Agent Registry:** 24 profiles with independent local invocation and recorded results.
- **Tool Registry:** ten bounded local tools, input/output contracts, execution history, and confirmation checks for operator actions and simulations.
- **Agent Intelligence:** optional local Ollama endpoint/model settings and a connection test. It is disabled by default; no model is downloaded automatically.
- **Agent Memory:** persistent scoped notes and lexical search. AG-20 can retrieve relevant incident/historical memory when its preferences enable it.
- **Execution Monitor:** persisted routine/task inputs, outputs, evidence, status, attempts, duration, and optional model token counts.
- **Agent Evaluation:** six synthetic routing cases plus contract, evidence, cautious-RCA, benign-event, and metrics-tool probes. These are small regression fixtures, not a measured real-world false-positive rate or LLM benchmark.

Studio definitions, run history, memory, settings, and evaluations use a separate SQLite database at `data/ops_studio.sqlite3`. Existing incident history remains in `data/incidents.sqlite3`. The `routine_studio` package owns the catalog, store, and versioned execution contract (`1.0`). Tasks support conditions, adjacent parallel groups, retries, timeouts, and persisted operator approval/resume checkpoints. API routes are under `/api/studio`; interactive API documentation is available at `/docs`.

To try the workflow, simulate an issue, select the resulting incident, then open Routine Studio and run an enabled routine on that incident. Inspect its tasks in Execution Monitor. Independent micro-agent/tool tests are available from their registries.

For optional reasoning, install/run Ollama separately and make a compatible local Qwen model available. In Agent Intelligence, configure its loopback URL (default `http://127.0.0.1:11434`) and exact installed model name (default `qwen3:8b`), test the connection, then enable it. Unavailable models, connection failures, or invalid evidence references fall back to deterministic RCA. Only AG-20 currently uses this semantic enhancement; saving another agent's model preferences does not turn its deterministic implementation into an LLM agent.

### Studio limits

This is a single-user local POC without authentication or role-based authorization; keep Uvicorn bound to loopback. The editor is an ordered node list, not a free-form graph canvas: conditions skip tasks and parallel groups run adjacent nodes, but arbitrary branch edges are not implemented. Memory retrieval and model settings are not universally consumed by every agent. Approval checkpoints resume through the local API/UI, not an external approval service. Tools do not connect to production infrastructure or enterprise ticketing. Token counts are available only when an actual Ollama response supplies usage. LLM validity is reported as not run by the synthetic evaluation suite.

## Run on Windows

From this directory, install the dependencies and start the application:

```powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --host 127.0.0.1 --port 8001
```

Open [http://127.0.0.1:8001](http://127.0.0.1:8001) for the operator dashboard. The command `python -m uvicorn` works even when the Python Scripts directory is not on PATH.

Run the local regression tests with:

```powershell
python -m unittest discover -s tests -v
```

The simulator starts generating flows at 1 flow/second. Use the monitoring details panel to start/stop generation, change frequency (0.2–20 flows/second), trigger a scenario, recover, or reset. **Simulate an issue** visibly sends related synthetic failures through gateway, telemetry, and database monitoring, then waits for automatic trace correlation and investigation. The run status explains each stage. The demo remains explicitly synthetic; it does not call production systems.

For extra endpoint traffic, open a second PowerShell terminal:

```powershell
python load_generator.py --rps 5
```

The original `/health`, `/products`, `/orders/{order_id}`, `/login`, `/checkout`, `/bug/null`, `/bug/key`, `/bug/zero`, `/chaos`, and `/metrics` endpoints remain available. A real request to `/bug/zero` still raises `ZeroDivisionError` and is logged with its stack trace and HTTP 500 access event. The normal load generator no longer calls the deliberate `/bug/...` endpoints randomly; use the bug scenario or call them directly when you want to test those faults. `/api/vehicles/telemetry` is also available.

## Demonstrate the incidents

Choose an incident in the dashboard and select **Trigger scenario**:

| Scenario | Expected log sequence |
|---|---|
| Application code bug | The explicit `/bug/zero` request produces one real Python ZeroDivisionError and HTTP 500. Background telemetry bug events are intermittent and ramp in only after a delay. |
| PostgreSQL exhaustion | Pool pressure begins rising after a lead-in period; latency warnings appear before intermittent acquisition timeouts. |
| Kafka consumer lag | Lag accumulates after a delay; consumers periodically report warnings/retries and notifications can be delayed. |
| Faulty deployment | A simulated release starts first, rolls out after 15 seconds, and only then do latency and intermittent errors increase. |
| High API latency | After a 15-second lead-in, request duration rises progressively while most requests succeed. |
| Infrastructure pressure | Memory and disk use increase gradually; health warnings and occasional failures only begin at higher pressure. |
| Recovery | Select **Trigger recovery**; resource metrics, queue lag, and latency unwind over about 36 seconds, then return to normal. |

Existing chaos routes remain available. Database-outage and slow scenarios now feed the gradual simulator behavior rather than immediately forcing every request to fail.

## Knowledge Base Agent (AG-02)

The dashboard's Knowledge Base panel provides document ingestion, cited SOP search, and log search.

The agent now lives in its own [`knowledge_base_agent/`](./knowledge_base_agent/) Python package rather than a single implementation file. `agent.py` is the AG-02 coordinator; each `ma_*.py` file is a separate microagent implementation, and `common.py` contains shared types/helpers:

| File | Responsibility |
|---|---|
| `knowledge_base_agent/agent.py` | AG-02 coordinator; wires the microagents into one pipeline |
| `knowledge_base_agent/ma03_document_ingestion.py` | MA-03 document parsing, provenance, and chunk preparation |
| `knowledge_base_agent/ma04_document_retrieval.py` | MA-04 Chroma storage and cited document retrieval |
| `knowledge_base_agent/ma05_log_retrieval.py` | MA-05 OpenSearch indexing, JSONL backfill, and log search |
| `knowledge_base_agent/ma06_knowledge_graph.py` | MA-06 bounded local service relationship graph for POC correlation |
| `knowledge_base_agent/ma07_governance.py` | MA-07 provenance, review/conflict flags, incident-learning intake |
| `knowledge_base_agent/common.py` | Shared errors, constants, and local vector helper |

`knowledge_base.py` remains only as a compatibility re-export. The FastAPI application imports AG-02 from the new package directly. A synthetic triage guide is indexed at startup so the first incident can include local SOP citations.

- **MA-03 Document Ingestion and Indexing:** Copy approved `.txt`, `.md`, `.rst`, `.json`, `.yaml`, `.yml`, `.csv`, `.pdf`, or `.docx` files into `data/sops`, then select **Ingest SOP folder**. Documents are size-limited, split into overlapping chunks, fingerprinted, and stored locally in Chroma at `data/chroma`. Re-ingestion updates changed chunks and removes stale chunks for that source.
- **MA-04 Document Knowledge Retrieval:** Searches Chroma and returns matching excerpts with source path, chunk number, SHA-256 provenance, and review/staleness indicators. An optional adjacent `filename.ext.meta.json` sidecar can supply `effective_until`, `revision`, and `topic`.
- **MA-05 Log Knowledge Retrieval:** Searches the current bounded local log buffer by default. When OpenSearch is configured, each generated JSON log record is queued to a background bulk-index worker and searches use OpenSearch. The dashboard can also queue existing `logs/app.log` and up to three rotated JSONL files for backfill.
- **MA-06 Knowledge Graph and Relationship Manager:** A small in-memory service dependency graph supports one-hop neighborhood and observed-edge lookups. It is not a graph database and is populated from the simulator's explicit service map.
- **MA-07 Governance, Conflict and Incident Learning:** Preserves citations, flags expired `effective_until` dates and possible opposing directives for human review, and captures incident-learning submissions in `data/incident-learning/pending`. Submissions are not treated as approved or indexed automatically. After human review, move an approved record into `data/sops/incident-learning/approved`, then ingest the SOP folder.

No LLM or downloaded embedding model is used. Chroma vectors are deterministic local lexical vectors: they provide a lightweight search baseline, not transformer-grade semantic matching. If Chroma cannot initialize, MA-04 falls back to a persistent SQLite lexical index and continues returning source metadata and citations. The retrieval API returns evidence and possible conflicts, not an RCA conclusion.

## Monitoring Agent (AG-08)

The Monitoring Agent is in its own [`monitoring_agent/`](./monitoring_agent/) package. The AG-08 coordinator wires each microagent into one bounded background pipeline; the simulator feeds it the same events shown in the live log explorer. No real infrastructure is polled by default.

| File | Responsibility |
|---|---|
| `monitoring_agent/agent.py` | AG-08 event-pipeline coordinator and status |
| `monitoring_agent/ma09_source_collector.py` | MA-09 connector registry, bounded intake queue, rate limiting, and generic polling hook |
| `monitoring_agent/ma10_data_normalization.py` | MA-10 common event shape, timestamp/severity/unit normalization, and quality warnings |
| `monitoring_agent/ma11_service_health.py` | MA-11 rolling availability, error rate, p95 latency, throughput, saturation, and SLO/error-budget estimates |
| `monitoring_agent/ma12_log_trace_analyzer.py` | MA-12 error-signature grouping and bounded trace correlation |
| `monitoring_agent/ma13_anomaly_detection.py` | MA-13 thresholds and rolling baselines; no model required |
| `monitoring_agent/ma14_event_detection.py` | MA-14 alert candidates, severity mapping, and duplicate suppression |
| `monitoring_agent/ma15_feedback_analyzer.py` | MA-15 PII scrubbing and baseline sentiment/topic summaries |
| `monitoring_agent/generate_reviews.py` | Reproducible generator for the explicitly synthetic review dataset |

The dashboard displays service health, alert candidates, connector status, normalization quality counters, and the synthetic review summary. Alert candidates include evidence IDs and explicitly set `cause_confirmed` to `false`; they are not RCA conclusions. In-process simulator and HTTP push intake are ready. A reusable asynchronous polling hook is provided for future adapters, but no vendor API, production credential, polling task, or infrastructure connection is configured. The intake queue is capped at 20,000 events; external connectors are rate-limited and report rejection/health counters.

### Synthetic Royal Enfield review dataset

`data/feedback/royal_enfield_reviews.jsonl` contains exactly 1,000 deterministic demo records. Each row has `synthetic: true` and `source: "synthetic_demo"`; none are real customers or verified purchases. Regenerate it with:

```powershell
python -m monitoring_agent.generate_reviews
```

MA-15 can summarize this dataset or analyze review text from the dashboard/API. Sentiment is labeled from the supplied star rating when one is present; without a rating it uses a transparent keyword heuristic. This is a baseline for testing the pipeline, not validated sentiment analysis.

### Monitoring APIs

- `GET /api/monitoring/status` — AG-08 / MA-09–MA-15 counters, connector queue health, review summary, and optional future model notes.
- `GET /api/monitoring/health?service=...` — rolling health and SLI/SLO metrics for all services or one service.
- `GET /api/monitoring/alerts?limit=100` — recent deduplicated incident candidates.
- `GET /api/monitoring/traces/{trace_id}` — retained events correlated by trace ID.
- `POST /api/monitoring/connectors` — register a connector with `connector_id`, `kind`, optional `services`, `enabled`, and `rate_limit_per_minute` (default 6,000). `simulator` and `http_webhook` are built in.
- `POST /api/monitoring/ingest` — queue one normalized-later event: `{"connector_id":"http_webhook","event":{"service.name":"orders","message":"...","log.level":"info"}}`.
- `GET /api/monitoring/feedback/summary` and `POST /api/monitoring/feedback/analyze` — synthetic dataset summary and model-free review analysis.

No model is needed to run AG-08 now. If a later offline-model phase is approved, optional candidates are a multilingual sentiment classifier (such as an XLM-RoBERTa sentiment model) for MA-15 and a time-series anomaly model or scikit-learn baseline for MA-13. Those are suggestions only: no model is downloaded, configured, or invoked in this implementation, and there is no LLM integration.

## Master Orchestrator (AG-01)

AG-01 automatically watches new AG-08 alert candidates. For each new alert group it gathers AG-02 log/SOP evidence, AG-16 correlation, AG-20 hypotheses, an AG-23 approval-gated response plan, and an incident summary. Candidates sharing a trace are grouped into one incident; routine application self-monitoring latency noise is filtered. The incident appears in the live queue without an operator having to start the investigation. A local AG-27 ticket is created only when the operator chooses **Create ticket**. Operators can acknowledge/close incidents, approve or reject the simulated response, and update a created local ticket. Investigations are persisted in `data/incidents.sqlite3`.

The incident queue prioritizes severity, then review state, then recency; automatic selection still follows the newest incident. Open incidents with the same service and stable alert fingerprint are merged when a repeat arrives within ten minutes. Merges retain unique events, logs, related alert IDs, affected services, and distinct trace IDs; different fingerprints and closed incidents remain separate. The selected-incident view summarizes affected services, grouped signal/trace counts, and the recency of the latest signal. It also renders retained trace events chronologically with timestamps and event IDs. Response plans include their target service, evidence-based decision basis, and a suggested verification check. These checks guide operator review; they do not prove that a real service recovered.

AG-01's background coordinator automatically processes newly retained alerts; `GET /api/orchestrator/status` reports automatic processing readiness. `GET /api/orchestrator/incidents/summary` returns compact queue records for dashboard polling; `GET /api/orchestrator/incidents` lists full saved investigations. `POST /api/orchestrator/investigations` with `{"alert_id":"..."}` remains available for explicitly rerunning/creating an investigation. `GET /api/orchestrator/incidents/{incident_id}` retrieves a record and `POST /api/orchestrator/incidents/{incident_id}/review` accepts `{"status":"acknowledged"}` or `{"status":"closed"}` and an optional note. The POC action decision is available at `POST /api/orchestrator/incidents/{incident_id}/poc/approval`; approval only runs the local dry-run simulation.

## Correlation Agent (AG-16)

AG-16 runs when AG-01 processes an alert. Its MA-17 temporal correlator orders trace evidence and identifies deployment events within a 30-minute lead-up window; MA-18 reports observed service links from the simulator's explicit dependency map; MA-19 applies explainable rules for exception, database-pool, messaging-lag, deployment-with-symptoms, and high-latency patterns. The report is stored with the incident under `evidence.correlation`. The operator dashboard renders an interactive dependency graph: the incident service is marked as affected, services in its exact trace are marked observed, and selecting a node filters the evidence inspector to that service.

AG-16 uses no ML model or graph database. It includes MA-06's local static relationship view alongside MA-17 temporal, MA-18 observed dependency, and MA-19 transparent failure-pattern evidence. Pattern scores describe matched rule checks, not calibrated probabilities. Correlations never confirm a cause.

`GET /api/orchestrator/topology` returns the simulator's dependency nodes and edges for the dashboard graph. The topology is a static POC map, not a live service-discovery feed.

## RCA, Resolution, Ticketing, and Reporting POC

- **AG-20 RCA:** MA-21 creates hypotheses from AG-16 rule candidates. MA-22 validates event references against retained trace/timeline evidence and reports evidence coverage, not a probability. The cause remains explicitly unconfirmed.
- **AG-23 Resolution:** MA-24 proposes a bounded runbook action with target, decision basis, and suggested verification check. MA-25 marks it approval-required. MA-26 can only produce a local dry-run result; no shell, infrastructure, deployment, or external tool is invoked.
- **AG-27 Ticketing:** MA-28 creates an enriched local ticket only after the operator requests handoff. MA-29 validates status changes. Tickets are stored inside the local incident record; no ITSM provider is connected.
- **Incident Output and Reporting:** Generates a Markdown handoff with incident state, leading hypothesis, evidence IDs, proposed response, and limitations; the operator can download it.

Use the dashboard's **Simulate an issue** control to watch the four-step flow: synthetic signals are ingested, monitoring detects them, AG-01 correlates them into one incident, and the dashboard shows the assessment and proposed response. The incident and report are persisted in SQLite; no ticket exists until **Create ticket** is selected. Approving a response only runs a simulated dry run and never changes an external system. This proves workflow feasibility, not production readiness: authentication/RBAC, durable production graph and vector stores, vendor connectors, calibrated models, audited approvals, and real ITSM integration remain future work.

Example sidecar for `data/sops/payment-runbook.md` (`payment-runbook.md.meta.json`):

```json
{"effective_until": "2027-01-31", "revision": "4", "topic": "payment recovery"}
```

### OpenSearch configuration

Set environment variables before starting the app. Credentials remain outside source control:

```powershell
$env:OPENSEARCH_URL = "https://your-opensearch-host:9200"
$env:OPENSEARCH_USERNAME = "your-user"
$env:OPENSEARCH_PASSWORD = "your-password"
$env:OPENSEARCH_INDEX_PREFIX = "aiops-logs"
python -m uvicorn app:app --host 127.0.0.1 --port 8001
```

`OPENSEARCH_URL` is optional. If absent, log search explicitly reports `local_buffer`; generated logs are not sent externally. TLS verification is on by default and can be controlled with `OPENSEARCH_VERIFY_TLS` (prefer leaving it enabled). To validate a live connection/indexing, configure the user's OpenSearch endpoint and credentials, then inspect `GET /api/kb/status`; this workspace does not include those connection details.

### Knowledge Base APIs

- `GET /api/kb/status` — microagent and store readiness, queue/index counters, last indexing error, and graph deferral.
- `POST /api/kb/documents/ingest` — ingest supported files from `data/sops`.
- `GET /api/kb/documents/search?query=...&limit=6` — retrieve cited document chunks.
- `GET /api/kb/logs/search?query=...&limit=20&service=...&since=...` — search OpenSearch or the local bounded log buffer.
- `POST /api/kb/logs/backfill` — queue the current and rotated JSONL logs to OpenSearch. This requires `OPENSEARCH_URL`, reads at most 50 MB and queues at most 100,000 records per call; indexing progress/errors appear in KB status.
- `POST /api/kb/incident-learning` — capture a learning candidate. Body: `{"incident_id":"INC-123","summary":"...","evidence":["trace-id or log reference"]}`. It remains pending; a human must review it and move it into `data/sops/incident-learning/approved` before it is indexed as approved knowledge.

## Log and simulator APIs

- `GET /api/logs?after=0&limit=200` returns the newest live records. The in-memory explorer buffer is capped at 5,000 records; JSONL file output continues using rotating `logs/app.log`.
- Optional log filters: `service`, `severity`, `scenario`, `since` (ISO timestamp), and `search`. The scenario filter uses a private, bounded in-memory index; scenario labels are not included in JSONL records or API event payloads.
- `GET /api/simulator/status` reports scenario and simulated service metrics.
- `POST /api/simulator/start`, `/stop`, `/reset`, `/recover`.
- `POST /api/simulator/scenario/{name}` activates a scenario.
- `POST /api/simulator/frequency` with `{"frequency": 2}` configures flow frequency.

JSONL records use ECS-inspired fields including `@timestamp`, `log.level`, `log.logger`, `service.name`, `service.environment`, `service.version`, `event.action`, `event.outcome`, `trace.id`, `request_id`, and source-appropriate HTTP, database, cache, messaging, deployment, or container fields. Not every record contains every field. Incident names are deliberately omitted from operational logs; they remain available in the simulator status/control panel and are not handed to a future RCA consumer as the answer. Set `APP_ENV` and `APP_VERSION` to label the app's environment and version.

Incidents are modeled as gradual changes: database pool pressure and infrastructure use rise over time, Kafka lag accumulates before consumers report trouble, and deployment symptoms start after the rollout. Normal simulator traffic has a 0.5% per-flow transient-failure probability; incident failures are probabilistic rather than forced on every request. Scenario names are intentionally not attached to operational events, so the log evidence does not tell an RCA consumer which button was pressed. The selected scenario remains visible in the operator status panel. The simulated timeline is illustrative and is not a measured model of any particular production environment.
