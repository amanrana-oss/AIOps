# Synthetic Enterprise Log Simulator and Ops Agents

This FastAPI app generates bounded, structured, correlated logs for a synthetic company service environment and includes AG-02 Knowledge Base and AG-08 Monitoring Agent pipelines. It does not connect to real company infrastructure or contain AI/LLM analysis.

## Run on Windows

From this directory, install the dependencies and start the application:

```powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --host 127.0.0.1 --port 8000
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000) for the scenario-control panel and live log explorer. The command `python -m uvicorn` works even when the Python Scripts directory is not on PATH.

Run the local regression tests with:

```powershell
python -m unittest discover -s tests -v
```

The simulator starts generating flows at 1 flow/second. Use the panel to start/stop generation, change frequency (0.2–20 flows/second), trigger an incident, recover, or reset. Each simulated flow emits related events from the gateway, authentication, telemetry, PostgreSQL, Redis, Kafka, notification, and Kubernetes services with shared request and trace identifiers. A flow is not an error: normal operation generates mostly INFO events, with a low background rate of transient failures.

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
| `knowledge_base_agent/ma06_knowledge_graph.py` | MA-06 reserved boundary; graph implementation deferred |
| `knowledge_base_agent/ma07_governance.py` | MA-07 provenance, review/conflict flags, incident-learning intake |
| `knowledge_base_agent/common.py` | Shared errors, constants, and local vector helper |

`knowledge_base.py` remains only as a compatibility re-export. The FastAPI application imports AG-02 from the new package directly.

- **MA-03 Document Ingestion and Indexing:** Copy approved `.txt`, `.md`, `.rst`, `.json`, `.yaml`, `.yml`, `.csv`, `.pdf`, or `.docx` files into `data/sops`, then select **Ingest SOP folder**. Documents are size-limited, split into overlapping chunks, fingerprinted, and stored locally in Chroma at `data/chroma`. Re-ingestion updates changed chunks and removes stale chunks for that source.
- **MA-04 Document Knowledge Retrieval:** Searches Chroma and returns matching excerpts with source path, chunk number, SHA-256 provenance, and review/staleness indicators. An optional adjacent `filename.ext.meta.json` sidecar can supply `effective_until`, `revision`, and `topic`.
- **MA-05 Log Knowledge Retrieval:** Searches the current bounded local log buffer by default. When OpenSearch is configured, each generated JSON log record is queued to a background bulk-index worker and searches use OpenSearch. The dashboard can also queue existing `logs/app.log` and up to three rotated JSONL files for backfill.
- **MA-06 Knowledge Graph and Relationship Manager:** Explicitly deferred; no graph database or graph logic has been added.
- **MA-07 Governance, Conflict and Incident Learning:** Preserves citations, flags expired `effective_until` dates and possible opposing directives for human review, and captures incident-learning submissions in `data/incident-learning/pending`. Submissions are not treated as approved or indexed automatically. After human review, move an approved record into `data/sops/incident-learning/approved`, then ingest the SOP folder.

No LLM or downloaded embedding model is used. Chroma vectors are deterministic local lexical vectors: they provide a lightweight search baseline, not transformer-grade semantic matching. The retrieval API returns evidence and possible conflicts, not an RCA conclusion.

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
python -m uvicorn app:app --host 127.0.0.1 --port 8000
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
