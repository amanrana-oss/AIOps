"""Synthetic company service environment and structured log simulator.

Run with ``python -m uvicorn app:app --host 127.0.0.1 --port 8000``.
The dashboard and live event stream are served by this FastAPI application.
"""
import asyncio
import json
import logging
import os
import random
import re
import socket
import time
import threading
import uuid
from collections import deque
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from knowledge_base_agent import KnowledgeBaseAgent, KnowledgeBaseError, OpenSearchLogConnector
from monitoring_agent import MonitoringAgent
from simulator import SCENARIO_ALIASES, SCENARIO_DESCRIPTIONS, SERVICES, Simulator

# --------------------------------------------------------------------------
# Structured JSON logging
# --------------------------------------------------------------------------
request_id_ctx: ContextVar[str] = ContextVar("request_id", default="-")
trace_id_ctx: ContextVar[str] = ContextVar("trace_id", default="-")
span_id_ctx: ContextVar[str] = ContextVar("span_id", default="-")

SERVICE_ENVIRONMENT = os.getenv("APP_ENV", "development")
SERVICE_VERSION = os.getenv("APP_VERSION", "1.0.0")
HOST_NAME = socket.gethostname()
SERVICE_CATEGORIES = {
    "api": ["web"],
    "auth": ["authentication"],
    "database": ["database"],
    "inventory": ["database"],
    "orders": ["web"],
    "payment": ["web"],
    "runtime": ["host"],
    "chaos": ["configuration"],
}
SERVICE_ACTIONS = {
    "api": "http.request",
    "auth": "user.authentication",
    "database": "database.query",
    "inventory": "inventory.check",
    "orders": "order.process",
    "payment": "payment.charge",
    "runtime": "runtime.monitor",
    "chaos": "chaos.configure",
}
SYSLOG_SEVERITY = {
    logging.CRITICAL: 2,
    logging.ERROR: 3,
    logging.WARNING: 4,
    logging.INFO: 6,
    logging.DEBUG: 7,
}


class JsonFormatter(logging.Formatter):
    def format(self, record):
        service = getattr(record, "service", "app")
        fields = dict(getattr(record, "fields", {}))
        action = fields.pop("event_action", SERVICE_ACTIONS.get(service, "application.log"))
        outcome = fields.pop("event_outcome", "failure" if record.levelno >= logging.ERROR else "unknown")
        timestamp = getattr(record, "event_timestamp", None)
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            record.event_timestamp = timestamp
        event_id = getattr(record, "event_id", None)
        if event_id is None:
            event_id = uuid.uuid4().hex
            record.event_id = event_id
        data = {
            "@timestamp": timestamp,
            "ecs.version": "8.17.0",
            "timestamp": timestamp,
            "level": record.levelname,
            "log.level": record.levelname.lower(),
            "log.logger": record.name,
            "log.origin.file.name": os.path.basename(record.pathname),
            "log.origin.function": record.funcName,
            "log.syslog.severity.code": SYSLOG_SEVERITY.get(record.levelno, 6),
            "service": service,
            "service.name": service,
            "service.environment": SERVICE_ENVIRONMENT,
            "service.version": SERVICE_VERSION,
            "host.name": HOST_NAME,
            "process.pid": os.getpid(),
            "process.thread.name": threading.current_thread().name,
            "event.id": event_id,
            "event.dataset": f"dummy-app.{service}",
            "event.category": SERVICE_CATEGORIES.get(service, ["process"]),
            "event.type": [
                "error" if record.levelno >= logging.ERROR
                else "warning" if record.levelno >= logging.WARNING
                else "info"
            ],
            "event.action": action,
            "event.outcome": outcome,
            "request_id": request_id_ctx.get(),
            "trace.id": trace_id_ctx.get(),
            "span.id": span_id_ctx.get(),
            "transaction.id": request_id_ctx.get(),
            "message": record.getMessage(),
        }
        if "method" in fields:
            data["http.request.method"] = fields["method"]
        if "path" in fields:
            data["url.path"] = fields["path"]
        if "status_code" in fields:
            data["http.response.status_code"] = fields["status_code"]
        if "latency_ms" in fields:
            data["event.duration"] = round(fields["latency_ms"] * 1_000_000)
        elif "duration_ms" in fields:
            data["event.duration"] = round(fields["duration_ms"] * 1_000_000)
        if record.exc_info:
            data["error.type"] = record.exc_info[0].__name__
            data["error.stack_trace"] = self.formatException(record.exc_info)
        data.update(fields)
        if record.exc_info:
            data["error_type"] = record.exc_info[0].__name__
            data["stack_trace"] = data["error.stack_trace"]
        return json.dumps(data)


os.makedirs("logs", exist_ok=True)
LOG_BUFFER_LIMIT = 5000
log_buffer = deque(maxlen=LOG_BUFFER_LIMIT)
log_scenarios: dict[int, str] = {}
log_buffer_lock = threading.Lock()
log_sequence = 0
opensearch_connector = OpenSearchLogConnector()
knowledge_base = KnowledgeBaseAgent(Path(__file__).with_name("data"), opensearch_connector)
monitoring_agent = MonitoringAgent(
    SERVICES,
    Path(__file__).with_name("data") / "feedback" / "royal_enfield_reviews.jsonl",
)


class LogBufferHandler(logging.Handler):
    def emit(self, record):
        global log_sequence
        try:
            payload = json.loads(self.format(record))
            with log_buffer_lock:
                log_sequence += 1
                payload["sequence"] = log_sequence
                log_buffer.append(payload)
                active_simulator = globals().get("simulator")
                log_scenarios[log_sequence] = getattr(active_simulator, "scenario", "normal")
                expired_sequence = log_sequence - LOG_BUFFER_LIMIT
                if expired_sequence > 0:
                    log_scenarios.pop(expired_sequence, None)
            opensearch_connector.enqueue(payload)
            monitoring_agent.submit(payload, "simulator")
        except Exception:
            self.handleError(record)


logger = logging.getLogger("dummy-app")
logger.setLevel(logging.INFO)
logger.handlers.clear()
for h in (
    logging.StreamHandler(),
    RotatingFileHandler("logs/app.log", maxBytes=10_000_000, backupCount=3),
    LogBufferHandler(),
):
    h.setFormatter(JsonFormatter())
    logger.addHandler(h)


def log(level, service, msg, exc=False, **fields):
    component_logger = logging.getLogger(f"dummy-app.{service}")
    component_logger.log(
        level, msg, extra={"service": service, "fields": fields}, exc_info=exc,
        stacklevel=2,
    )


simulator = Simulator(log)


@asynccontextmanager
async def lifespan(application: FastAPI):
    simulator_task = asyncio.create_task(simulator.run())
    opensearch_task = asyncio.create_task(opensearch_connector.run())
    monitoring_task = asyncio.create_task(monitoring_agent.run())
    application.state.simulator_task = simulator_task
    application.state.opensearch_task = opensearch_task
    application.state.monitoring_task = monitoring_task
    try:
        yield
    finally:
        for task in (simulator_task, opensearch_task, monitoring_task):
            task.cancel()
        for task in (simulator_task, opensearch_task, monitoring_task):
            try:
                await task
            except asyncio.CancelledError:
                pass


# --------------------------------------------------------------------------
# Chaos / fault-injection state
# --------------------------------------------------------------------------
SCENARIOS = {
    "normal":        "Healthy system, ~2% random background errors",
    "error_spike":   "~40% of requests fail across services",
    "slow":          "High latency (1.5-4s) on all calls, timeouts begin",
    "db_outage":     "Database connection refused - everything touching DB fails",
    "payment_down":  "Payment gateway returns 502 - checkout fails",
    "memory_leak":   "Memory grows each request; warnings, then OOM + restart",
    "cascading":     "DB slows -> inventory times out -> checkout fails (cascade)",
    "brute_force":   "Flood of failed logins from a few IPs (security anomaly)",
}
SCENARIOS.update({
    name: description for name, description in SCENARIO_DESCRIPTIONS.items()
    if name not in SCENARIOS
})
state = {"scenario": "normal", "since": time.time(), "leak": [], "restarts": 0}

stats = {"requests": 0, "errors": 0, "latency_sum_ms": 0.0}


class DatabaseError(Exception): pass
class PaymentGatewayError(Exception): pass
class InventoryTimeout(Exception): pass
class AuthError(Exception): pass


def sc() -> str:
    return state["scenario"]


async def work(lo=20, hi=150):
    """Simulate processing time; scenarios inflate it."""
    if sc() == "slow":
        lo, hi = 1500, 4000
    elif sc() == "cascading":
        lo, hi = 400, 1200
    elif simulator.scenario == "high_latency":
        extra_ms = int(max(0, simulator.scenario_age - 15) * 10)
        lo, hi = min(1500, lo + extra_ms), min(3000, hi + extra_ms)
    elif simulator.scenario == "faulty_deployment" and simulator.deployment_step >= 2:
        extra_ms = int(max(0, simulator.scenario_age - 20) * 5)
        lo, hi = min(1000, lo + extra_ms), min(2000, hi + extra_ms)
    await asyncio.sleep(random.uniform(lo, hi) / 1000)


# --------------------------------------------------------------------------
# Fake downstream "services"
# --------------------------------------------------------------------------
async def db_query(query: str):
    query_start = time.perf_counter()
    await work(10, 80)
    if simulator.scenario == "db_exhaustion" and simulator.db_connections >= 8:
        pressure_seconds = max(0.0, simulator.scenario_age - 10)
        failure_rate = (
            min(0.12, 0.02 + pressure_seconds / 600)
            if simulator.db_connections >= 12 else 0.015
        )
        if random.random() < failure_rate:
            log(logging.ERROR, "database", "Connection pool acquisition timed out",
                event_action="database.connection", event_outcome="failure",
                query=query, host="postgres-primary", db_system="postgresql",
                db_name="commerce", db_operation=query.split(" ", 1)[0].upper(),
                pool_active=simulator.db_connections, pool_max=20,
                timeout_ms=1000,
                duration_ms=round((time.perf_counter() - query_start) * 1000))
            raise DatabaseError("connection pool acquisition timed out")
    if sc() == "db_outage":
        log(logging.ERROR, "database", "Connection refused to postgres-primary:5432",
            event_action="database.connection",
            event_outcome="failure",
            query=query, host="postgres-primary", port=5432, db_system="postgresql",
            db_name="commerce", db_operation=query.split(" ", 1)[0].upper(),
            error_code="CONNECTION_REFUSED")
        raise DatabaseError("connection refused: postgres-primary:5432")
    if sc() == "cascading":
        await asyncio.sleep(random.uniform(2, 3))
        log(logging.ERROR, "database", "Query timeout after 2500ms - connection pool exhausted",
            event_action="database.query",
            event_outcome="failure",
            query=query, host="postgres-primary", db_system="postgresql",
            db_name="commerce", db_operation=query.split(" ", 1)[0].upper(),
            pool_active=50, pool_max=50, timeout_ms=2500,
            duration_ms=round((time.perf_counter() - query_start) * 1000))
        raise DatabaseError("query timeout: connection pool exhausted")
    if random.random() < (0.03 if sc() == "error_spike" else 0.005):
        log(logging.ERROR, "database", "Deadlock detected, transaction rolled back",
            event_action="database.query",
            event_outcome="failure",
            query=query, host="postgres-primary", db_system="postgresql",
            db_name="commerce", db_operation=query.split(" ", 1)[0].upper(),
            error_code="DEADLOCK_DETECTED",
            duration_ms=round((time.perf_counter() - query_start) * 1000))
        raise DatabaseError("deadlock detected")
    rows = random.randint(1, 20)
    log(logging.INFO, "database", "Database query completed",
        event_action="database.query",
        event_outcome="success",
        query=query, host="postgres-primary", db_system="postgresql",
        db_name="commerce", db_operation=query.split(" ", 1)[0].upper(),
        rows_returned=rows,
        duration_ms=round((time.perf_counter() - query_start) * 1000))
    return {"rows": rows}


async def inventory_check(sku: str):
    await work()
    if sc() == "cascading":
        log(logging.WARNING, "inventory", "Upstream DB slow, retrying", sku=sku, attempt=1)
        await asyncio.sleep(1.5)
        log(logging.ERROR, "inventory", "Inventory lookup timed out", sku=sku, timeout_ms=1500)
        raise InventoryTimeout(f"inventory timeout for {sku}")
    if random.random() < 0.05:
        log(logging.WARNING, "inventory", "Low stock", sku=sku, stock=random.randint(1, 4))
    await db_query(f"SELECT stock FROM inventory WHERE sku='{sku}'")
    log(logging.INFO, "inventory", "Inventory available",
        event_outcome="success", sku=sku, available=True)
    return True


async def charge_payment(amount: float, user: str):
    for attempt in range(1, 4):
        await work(50, 300)
        payment_down_age = max(0.0, time.time() - state["since"] - 15)
        payment_down_rate = min(0.25, payment_down_age / 300)
        failed = (sc() == "payment_down" and random.random() < payment_down_rate) or \
                 (random.random() < (0.08 if sc() == "error_spike" else 0.02))
        if not failed:
            log(logging.INFO, "payment", "Payment captured", user=user, amount=amount,
                event_outcome="success", txn_id=uuid.uuid4().hex[:12],
                attempt=attempt, gateway="payments-api.internal", currency="USD")
            return True
        if attempt < 3:
            log(logging.WARNING, "payment", "Gateway error, retrying", user=user,
                event_outcome="unknown", attempt=attempt, status_code=502,
                gateway="payments-api.internal", retryable=True)
        else:
            log(logging.ERROR, "payment", "Payment gateway unavailable after 3 retries",
                event_outcome="failure", user=user, amount=amount, status_code=502,
                gateway="payments-api.internal", attempt=attempt, retryable=False)
            raise PaymentGatewayError("502 Bad Gateway from payments-api.internal")


def leak_memory():
    if sc() != "memory_leak":
        state["leak"].clear()
        return
    state["leak"].append(bytearray(2 * 1024 * 1024))  # 2 MB per call
    mb = len(state["leak"]) * 2
    if mb % 40 == 0 and mb < 200:
        log(logging.WARNING, "runtime", f"Memory usage rising: {mb}MB", memory_mb=mb, limit_mb=200)
    if mb >= 200:
        log(logging.CRITICAL, "runtime", "OutOfMemory: container exceeded 200MB limit, restarting",
            memory_mb=mb, limit_mb=200)
        state["leak"].clear()
        state["restarts"] += 1
        log(logging.INFO, "runtime", "Service restarted", restart_count=state["restarts"])


# --------------------------------------------------------------------------
# App + middleware
# --------------------------------------------------------------------------
app = FastAPI(title="Synthetic Enterprise Log Simulator", lifespan=lifespan)


class FrequencyRequest(BaseModel):
    frequency: float = Field(ge=0.2, le=20)


class IncidentLearningRequest(BaseModel):
    incident_id: str = Field(min_length=1, max_length=120)
    summary: str = Field(min_length=1, max_length=5000)
    evidence: list[str] = Field(min_length=1, max_length=30)


class MonitoringConnectorRequest(BaseModel):
    connector_id: str = Field(min_length=1, max_length=100)
    kind: str = Field(min_length=1, max_length=40)
    services: list[str] = Field(default_factory=list, max_length=100)
    enabled: bool = True
    rate_limit_per_minute: int = Field(default=6000, ge=1, le=1_000_000)


class MonitoringEventRequest(BaseModel):
    connector_id: str = Field(min_length=1, max_length=100)
    event: dict[str, Any]


class FeedbackAnalysisRequest(BaseModel):
    text: str = Field(min_length=1, max_length=5000)
    rating: int | None = Field(default=None, ge=1, le=5)


@app.get("/")
async def dashboard():
    return FileResponse(Path(__file__).with_name("dashboard.html"))


@app.get("/api/monitoring/status")
async def monitoring_status():
    return await monitoring_agent.status()


@app.get("/api/monitoring/health")
async def monitoring_health(service: str | None = None):
    return monitoring_agent.health_status(service)


@app.get("/api/monitoring/alerts")
async def monitoring_alerts(limit: int = Query(default=100, ge=1, le=500)):
    return {
        "items": monitoring_agent.alerts(limit),
        "status": monitoring_agent.events.status(),
    }


@app.get("/api/monitoring/traces/{trace_id}")
async def monitoring_trace(trace_id: str):
    return monitoring_agent.trace(trace_id)


@app.post("/api/monitoring/connectors")
async def register_monitoring_connector(payload: MonitoringConnectorRequest):
    try:
        monitoring_agent.collector.register(
            payload.connector_id,
            payload.kind,
            payload.services,
            payload.enabled,
            payload.rate_limit_per_minute,
        )
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    return monitoring_agent.collector.status()


@app.post("/api/monitoring/ingest")
async def ingest_monitoring_event(payload: MonitoringEventRequest):
    if not monitoring_agent.submit(payload.event, payload.connector_id):
        return JSONResponse(
            {"error": "Connector is unknown/disabled or the monitoring queue is full."},
            status_code=429,
        )
    return {"accepted": True, "connector_id": payload.connector_id}


@app.get("/api/monitoring/feedback/summary")
async def feedback_summary():
    return monitoring_agent.feedback_status()


@app.post("/api/monitoring/feedback/analyze")
async def analyze_feedback(payload: FeedbackAnalysisRequest):
    return monitoring_agent.feedback.analyze_text(payload.text, payload.rating)


@app.get("/api/kb/status")
async def knowledge_base_status():
    return knowledge_base.status(opensearch_connector)


@app.post("/api/kb/documents/ingest")
async def ingest_knowledge_documents():
    try:
        return await knowledge_base.ingest_documents()
    except KnowledgeBaseError as exc:
        return JSONResponse({"error": str(exc)}, status_code=503)


@app.get("/api/kb/documents/search")
async def search_knowledge_documents(
    query: str = Query(min_length=1, max_length=1000),
    limit: int = Query(default=6, ge=1, le=20),
):
    try:
        return knowledge_base.search_documents(query, limit)
    except KnowledgeBaseError as exc:
        return JSONResponse({"error": str(exc)}, status_code=503)


@app.get("/api/kb/logs/search")
async def search_knowledge_logs(
    query: str = Query(default="", max_length=1000),
    limit: int = Query(default=20, ge=1, le=100),
    service: str | None = None,
    since: str | None = None,
    until: str | None = None,
    host: str | None = None,
    trace_id: str | None = None,
    error_type: str | None = None,
    severity: str | None = None,
    exact: bool = False,
):
    with log_buffer_lock:
        records = list(log_buffer)
    try:
        return await knowledge_base.log_retrieval.search(
            query, records, limit, service, since, until, host, trace_id, error_type, severity, exact
        )
    except KnowledgeBaseError as exc:
        return JSONResponse({"error": str(exc)}, status_code=502)


@app.post("/api/kb/logs/backfill")
async def backfill_knowledge_logs():
    log_directory = Path(__file__).with_name("logs")
    log_paths = [
        log_directory / "app.log",
        *(log_directory / f"app.log.{index}" for index in range(1, 4)),
    ]
    try:
        return await asyncio.to_thread(opensearch_connector.backfill_jsonl, log_paths)
    except KnowledgeBaseError as exc:
        return JSONResponse({"error": str(exc)}, status_code=503)


@app.post("/api/kb/incident-learning")
async def submit_incident_learning(payload: IncidentLearningRequest):
    try:
        return knowledge_base.governance.submit_incident_learning(
            payload.incident_id,
            payload.summary,
            payload.evidence,
            knowledge_base.data_root / "incident-learning" / "pending",
        )
    except KnowledgeBaseError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


@app.get("/api/simulator/status")
async def simulator_status():
    return simulator.status()


@app.post("/api/simulator/start")
async def start_generation():
    simulator.running = True
    log(logging.INFO, "deployment-pipeline", "Log generation started",
        event_action="simulator.start", event_outcome="success")
    return simulator.status()


@app.post("/api/simulator/stop")
async def stop_generation():
    simulator.running = False
    log(logging.INFO, "deployment-pipeline", "Log generation paused",
        event_action="simulator.stop", event_outcome="success")
    return simulator.status()


@app.post("/api/simulator/frequency")
async def set_generation_frequency(payload: FrequencyRequest):
    simulator.frequency = payload.frequency
    log(logging.INFO, "deployment-pipeline", "Log generation frequency updated",
        event_action="simulator.configure", event_outcome="success",
        frequency_per_second=simulator.frequency)
    return simulator.status()


@app.post("/api/simulator/scenario/{name}")
async def activate_scenario(name: str):
    normalized = SCENARIO_ALIASES.get(name, name)
    if normalized not in SCENARIO_DESCRIPTIONS or normalized == "recovery":
        return JSONResponse({
            "error": "unknown scenario",
            "available": [item for item in SCENARIO_DESCRIPTIONS if item != "recovery"],
        }, status_code=400)
    simulator.set_scenario(normalized)
    state["scenario"], state["since"] = "normal", time.time()
    return simulator.status()


@app.post("/api/simulator/recover")
async def recover_simulation():
    simulator.recover()
    state["scenario"], state["since"] = "normal", time.time()
    return simulator.status()


@app.post("/api/simulator/reset")
async def reset_simulation():
    simulator.reset()
    state["scenario"], state["since"] = "normal", time.time()
    state["leak"].clear()
    state["restarts"] = 0
    stats.update(requests=0, errors=0, latency_sum_ms=0.0)
    return simulator.status()


@app.get("/api/logs")
async def get_logs(
    after: int = 0,
    limit: int = Query(default=200, ge=1, le=500),
    service: str | None = None,
    severity: str | None = None,
    scenario: str | None = None,
    since: str | None = None,
    search: str | None = None,
):
    with log_buffer_lock:
        snapshot = list(log_buffer)
        scenario_snapshot = dict(log_scenarios)
        latest = log_sequence
    items = [record for record in snapshot if record["sequence"] > after]
    if service:
        items = [record for record in items if record.get("service.name") == service]
    if severity:
        items = [record for record in items if record.get("log.level") == severity.lower()]
    if scenario:
        items = [record for record in items
                 if scenario_snapshot.get(record["sequence"]) == scenario]
    if since:
        items = [record for record in items if record.get("@timestamp", "") >= since]
    if search:
        needle = search.casefold()
        items = [record for record in items
                 if needle in record.get("message", "").casefold()
                 or needle in json.dumps(record, ensure_ascii=False).casefold()]
    return {"items": items[-limit:], "latest_sequence": latest, "buffer_limit": LOG_BUFFER_LIMIT}


@app.middleware("http")
async def request_logger(request: Request, call_next):
    rid = uuid.uuid4().hex[:8]
    traceparent = request.headers.get("traceparent", "")
    match = re.fullmatch(r"00-([0-9a-fA-F]{32})-([0-9a-fA-F]{16})-([0-9a-fA-F]{2})", traceparent)
    trace_id = match.group(1).lower() if match and set(match.group(1)) != {"0"} else uuid.uuid4().hex
    span_id = uuid.uuid4().hex[:16]
    request_token = request_id_ctx.set(rid)
    trace_token = trace_id_ctx.set(trace_id)
    span_token = span_id_ctx.set(span_id)
    start = time.perf_counter()
    status = 500
    try:
        leak_memory()
        response = await call_next(request)
        status = response.status_code
    except Exception as exc:
        log(logging.ERROR, "api", f"Unhandled exception: {exc}", exc=True,
            path=request.url.path, method=request.method)
        response = JSONResponse({"error": type(exc).__name__, "request_id": rid}, status_code=500)
        status = 500
    finally:
        ms = round((time.perf_counter() - start) * 1000, 1)
        stats["requests"] += 1
        stats["latency_sum_ms"] += ms
        if status >= 500:
            stats["errors"] += 1
        level = logging.ERROR if status >= 500 else logging.WARNING if status >= 400 else logging.INFO
        if ms > 1000 and level == logging.INFO:
            level = logging.WARNING
        log(level, "api", f"{request.method} {request.url.path} -> {status} in {ms}ms",
            event_action="http.request",
            event_outcome="failure" if status >= 500 else "success",
            path=request.url.path, method=request.method, status_code=status, latency_ms=ms,
        )
        request_id_ctx.reset(request_token)
        trace_id_ctx.reset(trace_token)
        span_id_ctx.reset(span_token)
    return response


# --------------------------------------------------------------------------
# Business endpoints
# --------------------------------------------------------------------------
@app.get("/health")
async def health():
    db_degraded = (
        simulator.scenario == "db_exhaustion"
        and simulator.db_connections >= 12
    )
    ok = sc() not in ("db_outage",) and not db_degraded
    return JSONResponse({"status": "ok" if ok else "degraded", "scenario": sc()},
                        status_code=200 if ok else 503)


@app.get("/products")
async def products():
    await db_query("SELECT * FROM products LIMIT 20")
    return {"products": [{"sku": f"SKU-{i}", "price": round(random.uniform(5, 500), 2)} for i in range(5)]}


@app.get("/api/vehicles/telemetry")
async def vehicle_telemetry():
    cache_hit = random.random() < 0.72
    log(logging.INFO, "redis", "Telemetry cache hit" if cache_hit else "Telemetry cache miss",
        event_action="cache.lookup", event_outcome="success" if cache_hit else "unknown",
        **{"cache.hit": cache_hit, "cache.key": "vehicle:telemetry:sample"})
    await db_query("SELECT * FROM vehicle_telemetry ORDER BY observed_at DESC LIMIT 20")
    log(logging.INFO, "vehicle-telemetry-api", "Vehicle telemetry retrieved",
        event_action="telemetry.read", event_outcome="success",
        vehicle_count=20, **{"url.path": "/api/vehicles/telemetry"})
    message_id = uuid.uuid4().hex[:16]
    log(logging.INFO, "kafka", "Telemetry event published",
        event_action="messaging.publish", event_outcome="success",
        **{"messaging.destination.name": "vehicle.telemetry",
           "messaging.message.id": message_id})
    return {"items": [{"vehicle_id": f"vehicle-{i}", "speed_kph": random.randint(0, 120)}
                      for i in range(1, 6)]}


@app.get("/orders/{order_id}")
async def get_order(order_id: int):
    await db_query(f"SELECT * FROM orders WHERE id={order_id}")
    if random.random() < 0.05:
        log(logging.WARNING, "orders", "Order not found", order_id=order_id)
        return JSONResponse({"error": "not found"}, status_code=404)
    return {"order_id": order_id, "status": random.choice(["PLACED", "SHIPPED", "DELIVERED"])}


@app.post("/login")
async def login(request: Request):
    ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown")
    user = request.query_params.get("user", f"user{random.randint(1, 50)}")
    await work(30, 120)
    brute_force_age = max(0.0, time.time() - state["since"] - 10)
    if sc() == "brute_force" and random.random() < min(0.35, brute_force_age / 180):
        ip = random.choice(["45.13.22.9", "103.77.1.4", "185.220.1.7"])
        user = random.choice(["admin", "root", "administrator"])
        log(logging.WARNING, "auth", "Failed login attempt",
            event_action="user.authentication", event_outcome="failure",
            user=user, ip=ip, reason="bad_password", auth_method="password")
        return JSONResponse({"error": "invalid credentials"}, status_code=401)
    if random.random() < 0.03:
        log(logging.WARNING, "auth", "Failed login attempt",
            event_action="user.authentication", event_outcome="failure",
            user=user, ip=ip, reason="bad_password", auth_method="password")
        return JSONResponse({"error": "invalid credentials"}, status_code=401)
    log(logging.INFO, "auth", "Login successful",
        event_action="user.authentication", event_outcome="success",
        user=user, ip=ip, auth_method="password")
    return {"token": uuid.uuid4().hex}


@app.post("/checkout")
async def checkout():
    user = f"user{random.randint(1, 50)}"
    sku = f"SKU-{random.randint(1, 100)}"
    amount = round(random.uniform(10, 900), 2)
    order_id = uuid.uuid4().hex[:12]
    log(logging.INFO, "orders", "Checkout started",
        event_action="order.checkout", event_outcome="unknown",
        order_id=order_id, user=user, sku=sku, amount=amount, currency="USD")
    await inventory_check(sku)
    await charge_payment(amount, user)
    await db_query("INSERT INTO orders ...")
    log(logging.INFO, "orders", "Order placed",
        event_action="order.created", event_outcome="success",
        order_id=order_id, user=user, sku=sku, amount=amount, currency="USD")
    return {"status": "order_placed", "amount": amount}


# --------------------------------------------------------------------------
# Real code bugs (give the agent genuine stack traces to analyse)
# --------------------------------------------------------------------------
@app.get("/bug/null")
async def bug_null():
    user = None
    return {"name": user["name"]}          # TypeError


@app.get("/bug/key")
async def bug_key():
    cfg = {"region": "ap-south-1"}
    return {"timeout": cfg["timeout_seconds"]}   # KeyError


@app.get("/bug/zero")
async def bug_zero():
    return {"ratio": 10 / 0}               # ZeroDivisionError


# --------------------------------------------------------------------------
# Chaos control + metrics
# --------------------------------------------------------------------------
@app.get("/chaos")
async def list_scenarios():
    return {"current": sc(), "available": SCENARIOS}


@app.post("/chaos/{scenario}")
async def set_scenario(scenario: str):
    if scenario not in SCENARIOS:
        return JSONResponse({"error": "unknown scenario", "available": list(SCENARIOS)}, status_code=400)
    if scenario in SCENARIO_DESCRIPTIONS or scenario in SCENARIO_ALIASES:
        normalized = SCENARIO_ALIASES.get(scenario, scenario)
        if normalized == "recovery":
            simulator.recover()
        else:
            simulator.set_scenario(normalized)
        state["scenario"] = (
            scenario if scenario in {"payment_down", "brute_force"} else "normal"
        )
    else:
        state["scenario"] = scenario
    state["since"] = time.time()
    log(logging.INFO, "chaos", "Simulation mode updated",
        event_action="simulator.configure", event_outcome="success")
    return {"current": scenario, "description": SCENARIOS[scenario]}


@app.get("/metrics")
async def metrics():
    n = max(stats["requests"], 1)
    return {
        "requests_total": stats["requests"],
        "errors_total": stats["errors"],
        "error_rate": round(stats["errors"] / n, 4),
        "avg_latency_ms": round(stats["latency_sum_ms"] / n, 1),
        "memory_leak_mb": len(state["leak"]) * 2,
        "restarts": state["restarts"],
        "simulator": simulator.status(),
    }
