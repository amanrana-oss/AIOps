"""Synthetic company service environment and structured log simulator.

Run with ``python -m uvicorn app:app --host 127.0.0.1 --port 8001``.
The dashboard and live event stream are served by this FastAPI application.
"""
import asyncio
import ipaddress
import json
import logging
import os
import random
import re
import socket
import time
import threading
import uuid
import urllib.parse
import urllib.request
from collections import deque
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from correlation_agent import CorrelationAgent
from customer_intelligence import CustomerIntelligence, FeedbackAnalysis, FeedbackInput, FeedbackStore, is_feature_request
from customer_ai import customer_ai
from correlation_agent.ma18_dependency_analyzer import SERVICE_DEPENDENCIES
from knowledge_base_agent import KnowledgeBaseAgent, KnowledgeBaseError, OpenSearchLogConnector
from monitoring_agent import MonitoringAgent
from orchestrator_agent import AlertRoutingRules, OrchestratorAgent
from poc_agents import IncidentReportingAgent, ResolutionAgent, RootCauseAgent, TicketingAgent
from routine_studio import ExecutionContractError, RoutineExecutor, RoutineStore, validate_task_result
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
orchestrator_agent = OrchestratorAgent(Path(__file__).with_name("data") / "incidents.sqlite3")
alert_routing = AlertRoutingRules()
correlation_agent = CorrelationAgent()
root_cause_agent = RootCauseAgent()
resolution_agent = ResolutionAgent()
ticketing_agent = TicketingAgent()
reporting_agent = IncidentReportingAgent()
routine_store = RoutineStore(Path(__file__).with_name("data") / "ops_studio.sqlite3")
routine_executor = RoutineExecutor(routine_store)


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


def _parse_event_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


async def _refresh_startup_knowledge() -> None:
    try:
        await knowledge_base.ingest_documents()
    except KnowledgeBaseError as exc:
        log(logging.WARNING, "knowledge-base", "Startup SOP indexing was unavailable", error=str(exc))


@asynccontextmanager
async def lifespan(application: FastAPI):
    _purge_legacy_archived_customer_investigations()
    knowledge_task = asyncio.create_task(_refresh_startup_knowledge())
    application.state.knowledge_task = knowledge_task
    simulator_task = asyncio.create_task(simulator.run())
    opensearch_task = asyncio.create_task(opensearch_connector.run())
    monitoring_task = asyncio.create_task(monitoring_agent.run())
    orchestration_task = asyncio.create_task(_orchestrate_new_alerts())
    customer_task = asyncio.create_task(customer_intelligence.run())
    application.state.simulator_task = simulator_task
    application.state.opensearch_task = opensearch_task
    application.state.monitoring_task = monitoring_task
    application.state.orchestration_task = orchestration_task
    try:
        yield
    finally:
        for task in (simulator_task, opensearch_task, monitoring_task, orchestration_task, customer_task, knowledge_task):
            task.cancel()
        for task in (simulator_task, opensearch_task, monitoring_task, orchestration_task, customer_task, knowledge_task):
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


class InvestigationRequest(BaseModel):
    alert_id: str = Field(min_length=1, max_length=100)


class IncidentReviewRequest(BaseModel):
    status: str = Field(pattern="^(acknowledged|closed)$")
    note: str | None = Field(default=None, max_length=1000)


class POCApprovalRequest(BaseModel):
    approved: bool
    note: str | None = Field(default=None, max_length=1000)


class POCTicketStatusRequest(BaseModel):
    status: str = Field(pattern="^(open|in_progress|resolved|closed)$")


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


@app.get("/api/orchestrator/status")
async def orchestrator_status():
    return {
        "agent": "AG-01 Master Orchestrator",
        "state": "ready",
        "workflow": "alert evidence, correlation, RCA candidates, approval-gated dry run, simulated ticket, and report",
        "automatic_alert_processing": True,
        "correlation_agent": correlation_agent.status(),
        "poc_agents": {
            "AG-20": {"state": "ready", "mode": "deterministic evidence hypotheses"},
            "AG-23": {"state": "ready", "mode": "approval-gated simulation only"},
            "AG-27": {"state": "ready", "mode": "local simulated tickets"},
            "incident_output": {"state": "ready", "format": "markdown"},
        },
        "root_cause_analysis": False,
        "automated_remediation": False,
    }


def _studio_health_probe(event: dict[str, Any]) -> dict[str, Any]:
    normalized=event if event.get("monitoring.normalized") else monitoring_agent.normalizer.normalize(event,"studio-test")
    monitoring_agent.health.observe(normalized)
    return monitoring_agent.health.service_status(str(normalized.get("service.name","unknown")))


async def _studio_log_search(data: dict[str, Any]) -> dict[str, Any]:
    with log_buffer_lock:
        log_records=list(log_buffer)
    return await knowledge_base.log_retrieval.search(str(data.get("query","")),log_records,limit=int(data.get("limit",20)),service=data.get("service"))


def _studio_micro_agent_dispatch(data: dict[str, Any]) -> dict[str, Any]:
    event = data.get("event", {})
    incident = data.get("incident", {})
    events = data.get("events", incident.get("evidence", {}).get("trace_events", []))
    alert = data.get("alert", incident.get("alert", {}))
    rca=data.get("rca",incident.get("poc",{}).get("rca",{}))
    normalized=event if event.get("monitoring.normalized") else monitoring_agent.normalizer.normalize(event,"studio-test")
    def select_routine():
        return {"routine":routine_store.select_routine(alert)[0],"reason":routine_store.select_routine(alert)[1]}

    def ingest_documents():
        return knowledge_base.ingest_documents()

    def normalize():
        return monitoring_agent.normalizer.normalize(event,str(data.get("connector_id","studio-test")))

    def anomaly():
        return {"signals":monitoring_agent.anomaly.analyze(normalized)}

    def failure_signal():
        return {"signal":monitoring_agent.log_trace.analyze(event)}

    def detected_events():
        return {"alerts":monitoring_agent.events.detect(event,data.get("signals",[]))}

    def temporal():
        return correlation_agent.temporal.analyze(alert,events)

    def dependencies():
        service=str(alert.get("service",incident.get("service","unknown")))
        return correlation_agent.dependencies.analyze(service,events)

    def patterns():
        return {"patterns":correlation_agent.patterns.analyze(events,data.get("temporal",{}))}

    def hypotheses():
        return {"hypotheses":root_cause_agent.generator.generate(incident)}

    def validate_hypotheses():
        return {"hypotheses":root_cause_agent.validator.validate(incident,data.get("hypotheses",[]))}

    def plan():
        return resolution_agent.planner.plan(incident,rca)

    def gate():
        return resolution_agent.approval_gate.gate(data.get("proposed_action",{}))

    def simulate():
        return resolution_agent.execute_simulation(data.get("plan",{}),approved=bool(data.get("approved",False)))

    def enrich_ticket():
        return ticketing_agent.enrichment.create(incident,rca)

    def ticket_lifecycle():
        return ticketing_agent.update_status(data.get("ticket",{}),str(data.get("status","open")))

    return {
        "MA-01":lambda:alert_routing.group(data.get("alerts",[alert] if alert else [])),
        "MA-02":select_routine,
        "MA-03":ingest_documents,
        "MA-04":lambda:knowledge_base.search_documents(str(data.get("query","")),limit=int(data.get("limit",6))),
        "MA-05":lambda:_studio_log_search(data),
        "MA-06":lambda:knowledge_base.graph.analyze(str(data.get("service",incident.get("service","unknown"))),set(data.get("observed_services",[]))),
        "MA-07":lambda:knowledge_base.governance.assess(data.get("evidence",[])),
        "MA-09":lambda:{"accepted":monitoring_agent.submit(event,str(data.get("connector_id","simulator")))},
        "MA-10":normalize,
        "MA-11":lambda:_studio_health_probe(event),
        "MA-12":failure_signal,
        "MA-13":anomaly,
        "MA-14":detected_events,
        "MA-15":lambda:monitoring_agent.feedback.analyze_text(str(data.get("text","")),data.get("rating"),synthetic=bool(data.get("synthetic",False))),
        "MA-17":temporal,"MA-18":dependencies,"MA-19":patterns,
        "MA-21":hypotheses,"MA-22":validate_hypotheses,
        "MA-24":plan,"MA-25":gate,"MA-26":simulate,
        "MA-28":enrich_ticket,"MA-29":ticket_lifecycle,
    }


async def _invoke_studio_micro_agent(micro_agent_id: str, data: dict[str, Any]) -> dict[str, Any]:
    handler=_studio_micro_agent_dispatch(data).get(micro_agent_id)
    if handler is None:
        raise KeyError(f"Unknown micro-agent: {micro_agent_id}")
    value=handler()
    if asyncio.iscoroutine(value):
        value=await value
    result=validate_task_result({"output":value,"evidence":[]},str(data.get("task_id") or f"test-{uuid.uuid4().hex[:12]}"))
    return result.to_dict()


@app.post("/api/studio/micro-agents/{micro_agent_id}/execute")
async def studio_execute_micro_agent(micro_agent_id: str, payload: dict[str, Any]):
    known={item["micro_agent_id"] for item in routine_store.list_registry("micro_agents")}
    if micro_agent_id not in known:
        return JSONResponse({"error":"micro-agent not found"},status_code=404)
    started=time.perf_counter()
    task_input=payload.get("input",{})
    try:
        result=await _invoke_studio_micro_agent(micro_agent_id,task_input)
        routine_store.record_registry_execution("micro_agent",micro_agent_id,task_input,status=result["status"],output=result,duration_ms=round((time.perf_counter()-started)*1000,3),tokens_used=result.get("tokens_used"))
        return result
    except (ValueError,TypeError,KeyError,ExecutionContractError) as exc:
        routine_store.record_registry_execution("micro_agent",micro_agent_id,task_input,status="failed",error=str(exc),duration_ms=round((time.perf_counter()-started)*1000,3))
        return JSONResponse({"error":str(exc)},status_code=422)
    except Exception as exc:
        routine_store.record_registry_execution("micro_agent",micro_agent_id,task_input,status="failed",error=str(exc),duration_ms=round((time.perf_counter()-started)*1000,3))
        return JSONResponse({"error":f"Micro-agent execution failed: {exc}"},status_code=500)


@app.get("/api/studio/routines")
async def studio_list_routines():
    return {"items": routine_store.list_routines()}


@app.post("/api/studio/routines")
async def studio_create_routine(payload: dict[str, Any]):
    try:
        return routine_store.save_routine(payload)
    except (TypeError, ValueError) as exc:
        return JSONResponse({"error": str(exc)}, status_code=422)


@app.put("/api/studio/routines/{routine_id}")
async def studio_update_routine(routine_id: str, payload: dict[str, Any]):
    if routine_id != payload.get("routine_id"):
        return JSONResponse({"error": "routine_id in path and body must match"}, status_code=422)
    try:
        return routine_store.save_routine(payload)
    except (TypeError, ValueError) as exc:
        return JSONResponse({"error": str(exc)}, status_code=422)


@app.post("/api/studio/routines/{routine_id}/duplicate")
async def studio_duplicate_routine(routine_id: str):
    try:
        return routine_store.duplicate_routine(routine_id)
    except KeyError as exc:
        return JSONResponse({"error": str(exc.args[0])}, status_code=404)


@app.patch("/api/studio/routines/{routine_id}/enabled")
async def studio_set_routine_enabled(routine_id: str, payload: dict[str, Any]):
    routine = routine_store.set_routine_enabled(routine_id, bool(payload.get("enabled")))
    if not routine:
        return JSONResponse({"error": "routine not found"}, status_code=404)
    return routine


@app.post("/api/studio/routines/{routine_id}/runs")
async def studio_execute_routine(routine_id: str, payload: dict[str, Any]):
    routine = routine_store.get_routine(routine_id)
    if not routine:
        return JSONResponse({"error": "routine not found"}, status_code=404)
    if not routine.get("enabled", True):
        return JSONResponse({"error": "routine is disabled"}, status_code=409)
    incident_id = payload.get("incident_id")
    incident = orchestrator_agent.get_incident(str(incident_id)) if incident_id else None
    if incident_id and not incident:
        return JSONResponse({"error": "incident not found"}, status_code=404)
    context = {"incident": incident or {}, "operator_input": payload.get("input", {})}
    try:
        run = await routine_executor.execute(
            routine,
            context,
            _studio_task_handlers(),
            trigger={"type":"operator_manual_run","incident_id":incident_id},
            selected_reason="Operator started this routine from Routine Studio",
            incident_id=str(incident_id) if incident_id else None,
        )
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=422)
    return run


@app.post("/api/studio/runs/{run_id}/approval")
async def studio_resume_run(run_id: str, payload: dict[str, Any]):
    existing_run = routine_store.get_run(run_id)
    existing_incident = orchestrator_agent.get_incident(existing_run.get("incident_id", "")) if existing_run else None
    if existing_incident and existing_incident.get("alert", {}).get("customer_feedback"):
        return JSONResponse({"error": "Customer runs require authenticated proposal approval"}, status_code=403)
    try:
        return await routine_executor.resume(run_id,_studio_task_handlers(),approved=bool(payload.get("approved")),operator_id=str(payload.get("operator_id") or "local-operator"))
    except KeyError as exc:
        return JSONResponse({"error": str(exc.args[0])}, status_code=404)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)


@app.get("/api/studio/runs")
async def studio_list_runs(limit: int = Query(default=50, ge=1, le=200)):
    return {"items": routine_store.list_runs(limit)}


@app.get("/api/studio/runs/{run_id}")
async def studio_get_run(run_id: str):
    run = routine_store.get_run(run_id)
    if not run:
        return JSONResponse({"error": "routine run not found"}, status_code=404)
    return run


@app.get("/api/studio/registries/{registry}")
async def studio_registry(registry: str):
    try:
        return {"items": routine_store.list_registry(registry)}
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)


@app.patch("/api/studio/agents/{agent_id}")
async def studio_update_agent(agent_id: str, payload: dict[str, Any]):
    agent = routine_store.update_agent(agent_id,payload)
    if not agent:
        return JSONResponse({"error": "agent not found"}, status_code=404)
    return agent


@app.get("/api/studio/agent-stats")
async def studio_agent_stats():
    stats: dict[str, dict[str, Any]] = {}
    for run in routine_store.list_runs(200):
        for task in run.get("tasks", []):
            owner=task.get("micro_agent_id") or task.get("agent_id")
            if not owner:
                continue
            row=stats.setdefault(owner,{"executions":0,"succeeded":0,"failed":0,"duration_ms":0.0,"last_status":None})
            row["executions"]+=1
            row["succeeded"]+=int(task.get("status")=="completed")
            row["failed"]+=int(task.get("status")=="failed")
            row["duration_ms"]+=float(task.get("duration_ms") or 0)
            row["last_status"]=task.get("status")
    for execution in routine_store.list_registry_executions(limit=500):
        owner=execution["owner_id"]
        row=stats.setdefault(owner,{"executions":0,"succeeded":0,"failed":0,"duration_ms":0.0,"last_status":None})
        row["executions"]+=1
        row["succeeded"]+=int(execution["status"]=="completed")
        row["failed"]+=int(execution["status"]=="failed")
        row["duration_ms"]+=float(execution.get("duration_ms") or 0)
        row["last_status"]=execution["status"]
    for row in stats.values():
        row["success_rate"]=round(row["succeeded"]/row["executions"],3) if row["executions"] else 0
        row["average_duration_ms"]=round(row["duration_ms"]/row["executions"],2) if row["executions"] else 0
    return {"agents":stats,"micro_agents":stats}


@app.get("/api/studio/registry-executions")
async def studio_registry_executions(registry_kind: str | None = None, owner_id: str | None = None, limit: int = Query(default=100,ge=1,le=500)):
    return {"items":routine_store.list_registry_executions(registry_kind,owner_id,limit)}


@app.post("/api/studio/tools/{tool_id}/execute")
async def studio_execute_tool(tool_id: str, payload: dict[str, Any]):
    tool=next((item for item in routine_store.list_registry("tools") if item.get("tool_id")==tool_id),None)
    if not tool:
        return JSONResponse({"error":"tool not found"},status_code=404)
    tool_input=payload.get("input",{})
    if tool.get("permission") in {"operator_action","simulation_only"} and not bool(tool_input.get("confirm")):
        return JSONResponse({"error":"this tool requires explicit operator confirmation"},status_code=409)
    incident_id=tool_input.get("incident_id")
    incident=orchestrator_agent.get_incident(str(incident_id)) if incident_id else None
    if incident_id and not incident:
        return JSONResponse({"error":"incident not found"},status_code=404)
    handlers=_studio_task_handlers()
    operation=str(tool.get("handler",""))
    handler=handlers.get(operation)
    if not handler:
        return JSONResponse({"error":"tool handler is not available"},status_code=501)
    started=time.perf_counter()
    try:
        result=handler({"incident":incident or {},"alert":incident.get("alert",{}) if incident else {},"tool_input":tool_input},tool)
        if asyncio.iscoroutine(result):
            result=await result
        validated=validate_task_result({"output":result.get("output",result),"evidence":result.get("evidence",[])},str(payload.get("task_id") or f"tool-{uuid.uuid4().hex[:12]}"))
        response=validated.to_dict()
        routine_store.record_registry_execution("tool",tool_id,tool_input,status=validated.status,output=response,duration_ms=round((time.perf_counter()-started)*1000,3),tokens_used=validated.tokens_used)
        return response
    except (ValueError,TypeError,KeyError,ExecutionContractError) as exc:
        routine_store.record_registry_execution("tool",tool_id,tool_input,status="failed",error=str(exc),duration_ms=round((time.perf_counter()-started)*1000,3))
        return JSONResponse({"error":str(exc)},status_code=422)
    except Exception as exc:
        routine_store.record_registry_execution("tool",tool_id,tool_input,status="failed",error=str(exc),duration_ms=round((time.perf_counter()-started)*1000,3))
        return JSONResponse({"error":f"Tool execution failed: {exc}"},status_code=500)


@app.get("/api/studio/memory")
async def studio_search_memory(query: str = "", scope: str | None = None, limit: int = Query(default=20, ge=1, le=100)):
    return {"items": routine_store.search_memory(query,scope=scope,limit=limit)}


@app.post("/api/studio/memory")
async def studio_save_memory(payload: dict[str, Any]):
    try:
        return routine_store.save_memory(str(payload.get("scope","incident_working")),str(payload.get("content","")),owner_id=payload.get("owner_id"),incident_id=payload.get("incident_id"),metadata=payload.get("metadata",{}))
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=422)


@app.get("/api/studio/evaluations")
async def studio_list_evaluations():
    return {"items": routine_store.list_evaluations()}


@app.post("/api/studio/evaluations/run")
async def studio_run_evaluation():
    started=time.perf_counter()
    fixtures = [
        ({"service":"postgresql","signal":{"type":"log_failure","message":"query timeout"}},"routine.database_failure"),
        ({"service":"api-gateway","signal":{"type":"log_failure","message":"HTTP 503"}},"routine.api_failure"),
        ({"service":"vehicle-telemetry-api","signal":{"type":"threshold_anomaly","metric":"latency_ms","message":"latency"}},"routine.performance_degradation"),
        ({"service":"kubernetes","signal":{"type":"threshold_anomaly","message":"memory pressure"}},"routine.infrastructure_failure"),
        ({"service":"api-gateway","signal":{"type":"log_failure","message":"deployment regression after release"}},"routine.deployment_regression"),
        ({"service":"api-gateway","related_actionable_signal_count":3,"signal":{"type":"log_failure","message":"cascade upstream failure"}},"routine.cascading_failure"),
    ]
    cases=[]
    for alert,expected in fixtures:
        selected,reason=routine_store.select_routine(alert)
        cases.append({"service":alert["service"],"expected":expected,"selected":selected["routine_id"],"passed":selected["routine_id"]==expected,"reason":reason})
    passed=sum(item["passed"] for item in cases)
    selection_duration_ms=round((time.perf_counter()-started)*1000,3)
    task_valid=True
    try:
        validate_task_result({"output":{"ok":True},"evidence":[{"event_id":"fixture-event"}]},"eval-task")
    except ExecutionContractError:
        task_valid=False
    fixture_incident={"incident_id":"EVAL-INCIDENT","service":"postgresql","severity":"high","alert":{"signal":{"type":"log_failure","message":"query timeout"}},"evidence":{"trace_events":[{"event.id":"eval-event","service.name":"postgresql","message":"Database query timed out","error.type":"QueryTimeout"}],"correlation":{"microagents":{"MA-19_failure_pattern_scorer":[],"MA-17_temporal_change_correlator":{"nearby_changes":[]}}}}}
    rca=root_cause_agent.analyze(fixture_incident)
    evidence_refs={event.get("event.id") for event in fixture_incident["evidence"]["trace_events"]}
    evidence_quality=bool(rca["hypotheses"][0].get("evidence_refs")) and set(rca["hypotheses"][0].get("valid_evidence_refs",[])).issubset(evidence_refs)
    rca_quality=bool(rca["hypotheses"]) and all(not hypothesis.get("cause_confirmed",True) for hypothesis in rca["hypotheses"])
    benign_correlation=correlation_agent.analyze({"service":"api-gateway","first_seen":datetime.now(timezone.utc).isoformat(),"signal":{"type":"baseline_anomaly"}},[{"@timestamp":datetime.now(timezone.utc).isoformat(),"service.name":"api-gateway","log.level":"info","event.outcome":"success","message":"Healthy request"}])
    false_positive_sanity=not bool(benign_correlation["microagents"].get("MA-19_failure_pattern_scorer"))
    handler=_studio_task_handlers()["get_metrics"]
    tool_started=time.perf_counter()
    tool_result=handler({"incident":{}},{"id":"eval-metrics"})
    if asyncio.iscoroutine(tool_result):
        tool_result=await tool_result
    validated_tool=validate_task_result(tool_result,"eval-metrics")
    tool_success=bool(tool_result.get("output",{}).get("simulator"))
    metrics={
        "routine_selection":{"passed":passed,"total":len(cases),"score":round(passed/len(cases),3)},
        "task_completion":{"passed":int(validated_tool.status=="completed"),"total":1,"score":float(validated_tool.status=="completed"),"scope":"metrics tool probe only; not full routine completion"},
        "evidence_quality":{"passed":int(evidence_quality),"total":1,"score":float(evidence_quality)},
        "rca_quality":{"passed":int(rca_quality),"total":1,"score":float(rca_quality)},
        "structured_output_validity":{"passed":int(task_valid),"total":1,"score":float(task_valid)},
        "tool_execution_success":{"passed":int(tool_success),"total":1,"score":float(tool_success)},
        "false_positive_sanity":{"passed":int(false_positive_sanity),"total":1,"score":float(false_positive_sanity)},
        "execution_latency_ms":{"routine_selection_total":selection_duration_ms,"tool_execution":round((time.perf_counter()-tool_started)*1000,3)},
        "llm_response_validity":{"status":"not_run","reason":"Optional model reasoning is disabled; RCA uses deterministic analysis."},
    }
    result={"cases":cases,"passed":passed,"total":len(cases),"selection_accuracy":round(passed/len(cases),3),"metrics":metrics}
    return routine_store.create_evaluation("synthetic_agent_routine_suite",result,"completed")


@app.get("/api/orchestrator/incidents")
async def orchestrator_incidents(limit: int = Query(default=100, ge=1, le=500)):
    return {"items": orchestrator_agent.list_incidents(limit)}


@app.get("/api/orchestrator/incidents/summary")
async def orchestrator_incident_summaries(limit: int = Query(default=100, ge=1, le=500)):
    summaries = []
    for incident in orchestrator_agent.list_incidents(limit):
        alert = incident.get("alert", {})
        hypotheses = incident.get("poc", {}).get("rca", {}).get("hypotheses", [])
        summaries.append({
            "incident_id": incident.get("incident_id"),
            "created_at": incident.get("created_at", ""),
            "service": incident.get("service", "unknown"),
            "severity": incident.get("severity", "medium"),
            "status": incident.get("status", "pending_review"),
            "summary": incident.get("summary", ""),
            "alert": {
                "signal": alert.get("signal", {}),
                "event_evidence": {
                    "trace_id": alert.get("event_evidence", {}).get("trace_id"),
                },
                "related_actionable_signal_count": alert.get("related_actionable_signal_count", 0),
            },
            "poc": {"rca": {"hypotheses": [{"title": hypotheses[0].get("title", "")}] if hypotheses else []}},
        })
    return {"items": summaries}


@app.get("/api/orchestrator/topology")
async def orchestrator_topology():
    edges = [
        {"source": source, "target": target}
        for source, targets in SERVICE_DEPENDENCIES.items()
        for target in targets
    ]
    nodes = sorted({edge["source"] for edge in edges} | {edge["target"] for edge in edges})
    return {
        "source": "simulator_dependency_map",
        "nodes": nodes,
        "edges": edges,
        "live_infrastructure_connected": False,
    }


def _attach_poc_workflow(incident: dict[str, Any]) -> dict[str, Any]:
    if "poc" in incident:
        return incident
    rca = root_cause_agent.analyze(incident)
    resolution = resolution_agent.plan(incident, rca)
    workflow = {"rca": rca, "resolution": resolution, "ticket": None}
    workflow["report"] = reporting_agent.build({**incident, "poc": workflow})
    return orchestrator_agent.save_poc_workflow(incident["incident_id"], workflow) or incident


def _refresh_merged_poc_workflow(incident: dict[str, Any]) -> dict[str, Any]:
    previous_workflow = incident.get("poc", {})
    rca = root_cause_agent.analyze(incident)
    resolution = resolution_agent.plan(incident, rca)
    previous_actions = previous_workflow.get("resolution", {}).get("actions", [])
    if previous_actions and resolution.get("actions"):
        previous_status = previous_actions[0].get("status")
        if previous_status and previous_status != "awaiting_approval":
            resolution["actions"][0]["status"] = previous_status
    workflow = {
        "rca": rca,
        "resolution": resolution,
        "ticket": previous_workflow.get("ticket"),
    }
    workflow["report"] = reporting_agent.build({**incident, "poc": workflow})
    return orchestrator_agent.save_poc_workflow(incident["incident_id"], workflow) or incident


async def _ollama_hypothesis(incident: dict[str, Any], task_instructions: str = "", relevant_memory: list[dict[str, Any]] | None = None) -> tuple[dict[str, Any] | None, int | None, str | None]:
    config=routine_store.get_setting("agent_intelligence",{"provider":"ollama","base_url":"http://127.0.0.1:11434","model":"qwen3:8b","enabled":False,"temperature":0.2,"timeout_seconds":45})
    agent_config=next((item for item in routine_store.list_registry("agents") if item.get("agent_id")=="AG-20"),{})
    agent_model=agent_config.get("model",{})
    if not config.get("enabled") or config.get("provider")!="ollama" or agent_config.get("status")=="disabled" or agent_model.get("provider")!="ollama" or not agent_model.get("enabled",False):
        return None,None,None
    base_url=str(config.get("base_url",""))
    parsed=urllib.parse.urlparse(base_url)
    host=(parsed.hostname or "").casefold()
    try:
        local_host=host=="localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        local_host=False
    if parsed.scheme not in {"http","https"} or not local_host:
        return None,None,"Ollama is enabled but the configured endpoint is not localhost/loopback; deterministic RCA used."
    event_rows=[{"id":event.get("event.id"),"service":event.get("service.name"),"message":event.get("message"),"error_type":event.get("error.type"),"status":event.get("http.response.status_code")} for event in incident.get("evidence",{}).get("trace_events",[])[:30]]
    allowed_refs={str(item["id"]) for item in event_rows if item.get("id")}
    system_instructions=str(agent_config.get("system_instructions") or agent_config.get("instructions") or "You are AG-20 in a local synthetic AIOps proof of concept.")
    task_instructions=task_instructions or str(agent_config.get("task_instructions") or "")
    system_prompt=(system_instructions+"\n"+task_instructions+"\nReturn one JSON object with title, rationale, and evidence_refs (array of event IDs). Describe only a possible explanation supported by provided events; never assert causality, never recommend changing infrastructure, and use only supplied event IDs.")
    request_payload={"model":agent_model.get("model") or config.get("model","qwen3:8b"),"stream":False,"format":"json","options":{"temperature":float(agent_config.get("temperature",config.get("temperature",0.2)))},"messages":[{"role":"system","content":system_prompt},{"role":"user","content":json.dumps({"service":incident.get("service"),"alert":incident.get("alert",{}).get("signal",{}),"events":event_rows,"relevant_historical_memory":[{"content":item.get("content"),"incident_id":item.get("incident_id")} for item in (relevant_memory or [])]},ensure_ascii=False)}]}
    url=urllib.parse.urljoin(base_url.rstrip("/")+"/","api/chat")
    def request_chat():
        request=urllib.request.Request(url,data=json.dumps(request_payload).encode("utf-8"),headers={"Content-Type":"application/json"},method="POST")
        with urllib.request.urlopen(request,timeout=int(config.get("timeout_seconds",45))) as response:
            return json.loads(response.read().decode("utf-8"))
    try:
        response=await asyncio.to_thread(request_chat)
        content=json.loads(response.get("message",{}).get("content","{}"))
        title=str(content.get("title","")).strip()[:160]
        rationale=str(content.get("rationale","")).strip()[:1000]
        refs=[str(ref) for ref in content.get("evidence_refs",[]) if str(ref) in allowed_refs]
        if not title or not rationale or not refs:
            return None,int(response.get("prompt_eval_count",0))+int(response.get("eval_count",0)),"Ollama output failed schema/evidence validation; deterministic RCA used."
        candidate={"id":"ollama_semantic_candidate","title":title,"rationale":rationale,"evidence_refs":refs,"supporting_evidence":[f"Model-cited event {ref}" for ref in refs],"cause_confirmed":False,"source":"local_ollama","model":request_payload["model"]}
        tokens=int(response.get("prompt_eval_count",0))+int(response.get("eval_count",0))
        return candidate,tokens,None
    except Exception as exc:
        return None,None,f"Local Ollama call failed; deterministic RCA used: {type(exc).__name__}: {exc}"


def _studio_task_handlers() -> dict[str, Any]:
    def collect_evidence(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        incident = context.get("incident", {})
        evidence = incident.get("evidence", {})
        trace_events = evidence.get("trace_events", [])
        return {"output":{"trace_events":len(trace_events),"logs":len(evidence.get("logs",{}).get("items",[])),"sop_passages":len(evidence.get("sops",{}).get("evidence",[]))},"evidence":[{"event_id":event.get("event.id"),"service":event.get("service.name"),"message":event.get("message")} for event in trace_events[:20]]}

    def correlate(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        correlation = context.get("incident", {}).get("evidence", {}).get("correlation", {})
        microagents = correlation.get("microagents", {})
        patterns = microagents.get("MA-19_failure_pattern_scorer", [])
        temporal = microagents.get("MA-17_temporal_change_correlator", {})
        dependency = microagents.get("MA-18_dependency_analyzer", {})
        output = {"evidence_event_count":correlation.get("evidence_event_count",0),"patterns":patterns,"nearby_changes":temporal.get("nearby_changes",[]),"observed_dependency_links":dependency.get("observed_dependency_links",[])}
        return {"output":output,"evidence":[{"pattern":item.get("pattern"),"rule":item.get("rule")} for item in patterns]}

    async def root_cause(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        incident = context.get("incident")
        if not incident:
            raise ValueError("Select an incident to run RCA")
        result = root_cause_agent.analyze(incident)
        agent_config=next((item for item in routine_store.list_registry("agents") if item.get("agent_id")=="AG-20"),{})
        memory_config=agent_config.get("memory",{})
        memory_query=" ".join((str(incident.get("service","")),str(incident.get("summary",""))))
        relevant_memory=[]
        if memory_config.get("historical"):
            relevant_memory=routine_store.search_memory(memory_query,scope="historical_incident",limit=3)
        if memory_config.get("incident"):
            relevant_memory.extend(routine_store.search_memory(memory_query,scope="incident_working",limit=3))
        context["routine_rca"] = result
        context["relevant_memory"]=[{"memory_id":item.get("memory_id"),"scope":item.get("scope"),"incident_id":item.get("incident_id")} for item in relevant_memory]
        return {"output":{**result,"relevant_memory_count":len(relevant_memory)},"evidence":[{"title":item.get("title"),"references":item.get("evidence_refs",[])} for item in result.get("hypotheses",[])]}

    async def resolution(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        incident = context.get("incident")
        rca = context.get("routine_rca") or incident.get("poc",{}).get("rca",{})
        if not incident:
            raise ValueError("Select an incident to plan a response")
        result = resolution_agent.plan(incident,rca)
        actions = result.get("actions", [])
        guidance = actions[0].get("knowledge_guidance", []) if actions else []
        result["ticket_recommended"] = not any(
            item.get("source_kind") == "manual_or_sop"
            and item.get("guidance_kind") in {"customer_manual_steps", "documented_resolution"}
            for item in guidance
        )
        if incident.get("alert", {}).get("customer_feedback") and customer_ai.status()["enabled"]:
            passages = [item for item in incident.get("evidence", {}).get("sops", {}).get("evidence", [])
                        if item.get("id") and not item.get("stale")
                        and item.get("metadata", {}).get("document_type") in {"pdf", "docx"}]
            try:
                safe_passages = [{**item, "text": monitoring_agent.feedback.scrub_pii(str(item.get("text", "")))} for item in passages]
                answer = await customer_ai.answer(monitoring_agent.feedback.scrub_pii(incident["summary"]), safe_passages)
                lookup = {str(item["id"]): item for item in passages}
                action = result["actions"][0]
                guidance = []
                for step in answer.steps:
                    for reference in step.evidence_ids:
                        evidence = lookup[reference]
                        guidance.append({"text": step.text, "citation": evidence.get("citation", {}), "evidence_id": reference,
                                         "review_required": evidence.get("review_required", True),
                                         "source_kind": "manual_or_sop" if evidence.get("metadata", {}).get("document_type") in {"pdf", "docx"} else "historical_tracker",
                                         "applicability": "AI-generated explanation: verify cited source and applicability."})
                result["actions"] = [{**action, "title": "AI answer from cited knowledge" if guidance else "No supported AI answer found",
                                      "description": answer.summary, "knowledge_guidance": guidance,
                                      "answer_status": "ai_supported_requires_review" if guidance else "insufficient_knowledge"}]
                result["ticket_recommended"] = not any(item.get("source_kind") == "manual_or_sop" for item in guidance)
                result["ai_status"] = "completed"
                result["model"] = customer_ai.status()["model"]
            except Exception as exc:
                result["ai_status"] = "failed_fallback_requires_review"
                result["ai_error"] = str(exc) if isinstance(exc, ValueError) else "AI response timed out or failed"
        context["routine_resolution"] = result
        return {"output":result,"evidence":[{"action":item.get("title"),"target":item.get("target")} for item in result.get("actions",[])]}

    async def get_metrics(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        service=context.get("tool_input",{}).get("service") or context.get("incident",{}).get("service")
        return {"output":{"simulator":simulator.status(),"service_health":monitoring_agent.health_status(str(service) if service else None)},"evidence":[]}

    def inspect_traces(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        trace_id=context.get("tool_input",{}).get("trace_id") or context.get("alert",{}).get("event_evidence",{}).get("trace_id")
        trace=monitoring_agent.trace(str(trace_id)) if trace_id else {"events":[]}
        return {"output":{"trace_id":trace_id,"event_count":len(trace.get("events",[]))},"evidence":[{"event_id":event.get("event.id"),"service":event.get("service.name")} for event in trace.get("events",[])[:20]]}

    def inspect_dependencies(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        incident=context.get("incident",{})
        tool_input=context.get("tool_input",{})
        service=str(tool_input.get("service") or incident.get("service","unknown"))
        services=set(tool_input.get("observed_services",[])) or {event.get("service.name") for event in incident.get("evidence",{}).get("trace_events",[]) if event.get("service.name")}
        result=knowledge_base.graph.analyze(service,services)
        return {"output":result,"evidence":result.get("observed_edges",[])}

    def search_sops(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        incident=context.get("incident",{})
        query=str(context.get("tool_input",{}).get("query") or " ".join((str(incident.get("service","")),str(incident.get("summary","")))))
        result=knowledge_base.search_documents(query,limit=5) if query.strip() else {"evidence":[]}
        return {"output":result,"evidence":result.get("evidence",[])}

    async def search_logs(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        incident=context.get("incident",{})
        tool_input=context.get("tool_input",{})
        query=str(tool_input.get("query") or "")
        if query:
            with log_buffer_lock:
                records=list(log_buffer)
            result=await knowledge_base.log_retrieval.search(query,records,limit=20,service=tool_input.get("service"))
            items=result.get("items",[])
            source=result.get("source","local_buffer")
        else:
            items=incident.get("evidence",{}).get("logs",{}).get("items",[])
            source=incident.get("evidence",{}).get("logs",{}).get("source","incident evidence")
        return {"output":{"count":len(items),"source":source},"evidence":[{"event_id":item.get("event.id"),"message":item.get("message")} for item in items[:20]]}

    def search_incidents(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        incidents=orchestrator_agent.list_incidents(100)
        service=context.get("incident",{}).get("service")
        query=str(context.get("tool_input",{}).get("query") or service or "").casefold()
        similar=[{"incident_id":item.get("incident_id"),"service":item.get("service"),"status":item.get("status"),"summary":item.get("summary")} for item in incidents if query and query in " ".join((str(item.get("service","")),str(item.get("summary","")),str(item.get("incident_id","")))).casefold()][:10]
        return {"output":{"matches":similar},"evidence":[]}

    async def plan_response(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        return await resolution(context,node)

    async def simulate_recovery(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        confirmed=bool(context.get("tool_input",{}).get("confirm"))
        if not confirmed:
            return {"output":{"mode":"simulation_only","requires_operator_approval":True,"note":"Preview only. Confirm explicitly to start the local simulator recovery sequence."},"evidence":[]}
        simulator.recover()
        return {"output":{"mode":"simulation_only","simulator_status":await simulator.status(),"note":"Local synthetic recovery sequence started; no external system was changed."},"evidence":[]}

    def verify_recovery(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        return {"output":monitoring_agent.health_status(context.get("incident",{}).get("service")),"evidence":[]}

    def incident_learning_candidate(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        return {"output":{"status":"pending_human_review","incident_id":context.get("incident",{}).get("incident_id"),"note":"No learning is ingested automatically."},"evidence":[]}

    def create_ticket(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        incident=context.get("incident",{})
        if incident.get("alert", {}).get("customer_feedback"):
            raise ValueError("Customer tickets require authenticated proposal approval")
        if not context.get("tool_input",{}).get("confirm"):
            return {"output":{"status":"waiting_operator_confirmation","note":"Ticket creation is an operator-only local action."},"evidence":[]}
        if not incident.get("incident_id"):
            raise ValueError("An incident_id is required to create a local ticket")
        workflow=incident.get("poc",{})
        ticket=workflow.get("ticket") or ticketing_agent.create_draft(incident,workflow.get("rca",{}))
        workflow={**workflow,"ticket":ticket}
        saved=orchestrator_agent.save_poc_workflow(incident["incident_id"],workflow) or {**incident,"poc":workflow}
        return {"output":{"ticket":ticket,"incident_id":saved["incident_id"],"external_systems_contacted":False},"evidence":[]}

    return {"customer_monitor":_customer_monitor_result,"customer_preview":_customer_preview_result,"collect_evidence":collect_evidence,"correlate":correlate,"root_cause":root_cause,"resolution":resolution,"get_metrics":get_metrics,"inspect_traces":inspect_traces,"inspect_dependencies":inspect_dependencies,"search_sops":search_sops,"search_logs":search_logs,"search_incidents":search_incidents,"plan_response":plan_response,"simulate_recovery":simulate_recovery,"verify_recovery":verify_recovery,"incident_learning_candidate":incident_learning_candidate,"create_ticket":create_ticket}


async def _run_selected_routine(alert: dict[str, Any], incident: dict[str, Any]) -> dict[str, Any]:
    if alert.get("customer_feedback"):
        routine = routine_store.get_routine("routine.customer_feedback") or {
            "routine_id": "routine.customer_feedback", "name": "Customer Feedback Investigation",
            "description": "Investigate customer-reported symptoms without asserting an operational defect.",
            "enabled": True, "trigger": {"signal_types": ["customer_complaint"]},
            "nodes": [
                {"id": "monitor", "type": "agent", "agent_id": "AG-08", "operation": "customer_monitor", "timeout_seconds": 10, "retries": 1},
                {"id": "collect", "type": "agent", "agent_id": "AG-02", "operation": "collect_evidence", "timeout_seconds": 30, "retries": 1},
                {"id": "correlate", "type": "agent", "agent_id": "AG-16", "operation": "correlate", "timeout_seconds": 30, "retries": 1},
                {"id": "hypothesis", "type": "agent", "agent_id": "AG-20", "operation": "root_cause", "timeout_seconds": 60, "retries": 1},
                {"id": "response", "type": "agent", "agent_id": "AG-23", "operation": "resolution", "timeout_seconds": 30, "retries": 1},
                {"id": "preview", "type": "agent", "agent_id": "AG-27", "operation": "customer_preview", "timeout_seconds": 10, "retries": 0},
                {"id": "approval", "type": "approval", "agent_id": "AG-27", "label": "Human review required; no ticket created"},
            ],
        }
        escalation_condition = {"field": "tasks.response.ticket_recommended", "operator": "equals", "value": True}
        for node in routine.get("nodes", []):
            if node.get("id") in {"preview", "approval"}:
                node["condition"] = escalation_condition
        routine = routine_store.save_routine(routine)
        reason = "AG-01 selected customer feedback investigation for an AG-08 customer complaint signal"
    else:
        routine, reason = routine_store.select_routine(alert)
    try:
        run = await routine_executor.execute(
            routine,
            {"alert":alert,"incident":incident},
            _studio_task_handlers(),
            trigger={"type":"monitoring_alert","service":alert.get("service"),"signal_type":alert.get("signal",{}).get("type")},
            selected_reason=reason,
            incident_id=incident.get("incident_id"),
        )
    except Exception as exc:
        log(logging.ERROR,"routine-studio","Selected routine failed",routine_id=routine.get("routine_id"),incident_id=incident.get("incident_id"),error=str(exc))
        return incident
    workflow=incident.get("poc",{})
    task_outputs={task["node_id"]:task.get("output",{}) for task in run.get("tasks",[]) if task.get("status")=="completed"}
    if task_outputs.get("hypothesis"):
        workflow["rca"]=task_outputs["hypothesis"]
    if task_outputs.get("response"):
        workflow["resolution"]=task_outputs["response"]
    workflow["routine_execution"]={"routine_id":routine["routine_id"],"routine_name":routine["name"],"run_id":run["run_id"],"status":run["status"],"selected_reason":reason}
    if alert.get("customer_feedback") and task_outputs.get("response", {}).get("ticket_recommended"):
        customer_intelligence.store.propose(incident)
    if workflow.get("rca"):
        workflow["report"]=reporting_agent.build({**incident,"poc":workflow})
    updated=orchestrator_agent.save_poc_workflow(incident["incident_id"],workflow)
    return updated or incident


async def _build_investigation(alert: dict[str, Any]) -> dict[str, Any]:
    trace_id = alert.get("event_evidence", {}).get("trace_id")
    trace_events = monitoring_agent.trace(str(trace_id)).get("events", []) if trace_id else []
    with log_buffer_lock:
        records = list(log_buffer)
    if alert.get("customer_feedback"):
        records = [record for record in records if record.get("service.name") == alert.get("service")]
    try:
        log_evidence = await knowledge_base.log_retrieval.search(
            "", records, limit=100, trace_id=str(trace_id) if trace_id else None
        )
    except KnowledgeBaseError as exc:
        log_evidence = {"source": "unavailable", "items": [], "error": str(exc)}

    signal = alert.get("signal", {})
    feedback = alert.get("customer_feedback")
    sop_query = _customer_knowledge_query(feedback) if feedback else " ".join((str(alert.get("service", "")), str(signal.get("message", ""))))[:300]
    try:
        sop_evidence = knowledge_base.search_documents(sop_query, limit=8)
    except KnowledgeBaseError as exc:
        sop_evidence = {"evidence": [], "possible_conflicts": [], "error": str(exc)}

    alert_timestamp = str(alert.get("first_seen") or "")
    alert_datetime = _parse_event_timestamp(alert_timestamp)
    context_events = []
    if alert_datetime:
        for record in records:
            record_datetime = _parse_event_timestamp(str(record.get("@timestamp", "")))
            if not record_datetime:
                continue
            offset_seconds = (record_datetime - alert_datetime).total_seconds()
            if -1800 <= offset_seconds <= 300:
                context_events.append(record)
    correlation_events = {str(event.get("event.id")): event for event in trace_events}
    correlation_events.update({str(event.get("event.id")): event for event in context_events})
    correlation_evidence = correlation_agent.analyze(
        alert, list(correlation_events.values())[-500:]
    )
    if alert.get("customer_feedback"):
        correlation_evidence["customer_evidence_note"] = "Time/service association with synthetic telemetry only, not causal proof. Missing matches do not invalidate the complaint."
        correlation_evidence["service_health"] = monitoring_agent.health_status(str(alert.get("service")))
    observed_services = {
        str(event.get("service.name"))
        for event in correlation_events.values()
        if event.get("service.name")
    }
    correlation_evidence["microagents"]["MA-06_knowledge_graph_relationship_manager"] = (
        knowledge_base.graph.analyze(str(alert.get("service", "unknown")), observed_services)
    )
    merged_incident = orchestrator_agent.merge_recent_investigation(
        alert, trace_events, log_evidence, sop_evidence, correlation_evidence
    )
    if merged_incident:
        merged_incident = _refresh_merged_poc_workflow(merged_incident)
        return await _run_selected_routine(alert,merged_incident)

    incident = orchestrator_agent.create_investigation(
        alert, trace_events, log_evidence, sop_evidence, correlation_evidence
    )
    incident = _attach_poc_workflow(incident)
    return await _run_selected_routine(alert,incident)


def _customer_knowledge_query(feedback: dict[str, Any]) -> str:
    content = str(feedback.get("content") or "")
    analysis = feedback.get("analysis", {})
    topic = analysis.get("topic")
    if analysis.get("intent") == "feature_request" or is_feature_request(content):
        return "Flying Flea riding modes selected twist shifter instrument cluster selected mode reflected in app profiles charging mode app features"
    if topic == "vehicle_range":
        return "Estimated Range display Conservative Average Aggressive projections current and recent riding style battery level"
    if topic == "login_authentication":
        return "changing primary PIN account select OTP" if re.search(r"\bpin\b", content, re.IGNORECASE) else "OTP registered contact passcode"
    topic_terms = {
        "battery_status": "battery SOC state of charge battery percentage mobile app data refresh",
        "charging": "battery charging status charge indicator charging cable",
        "connectivity": "vehicle application Bluetooth pairing mobile app cloud connection",
        "mobile_app": "mobile application cloud data sync Bluetooth connection network",
    }
    return f"{content} {topic_terms.get(topic, '')}".strip()[:1000]


CUSTOMER_INVESTIGATION_NOT_FOUND = "Customer investigation not found"
customer_investigations_running: set[str] = set()


@app.get("/api/customer-intelligence/investigations")
async def customer_investigations(dataset: str = "real"):
    incidents = orchestrator_agent.list_incidents(500)
    archived = orchestrator_agent.archived_customer_investigation_ids()
    items = []
    for incident in incidents:
        feedback = incident.get("alert", {}).get("customer_feedback")
        if (not feedback or incident["incident_id"] in archived
                or (dataset != "all" and (feedback.get("source") == "synthetic") != (dataset == "synthetic"))):
            continue
        items.append({"incident_id": incident["incident_id"], "summary": incident["summary"], "service": incident["service"],
                      "evidence": {"customer_feedback": [{"feedback_id": item.get("feedback_id")} for item in incident.get("evidence", {}).get("customer_feedback", [])]},
                      "poc": {"routine_execution": incident.get("poc", {}).get("routine_execution", {})}})
    return {"items": items}


@app.delete("/api/customer-intelligence/investigations/{incident_id}")
async def remove_customer_investigation(incident_id: str):
    incident = orchestrator_agent.get_incident(incident_id)
    if not incident or not incident.get("alert", {}).get("customer_feedback"):
        return JSONResponse({"error": CUSTOMER_INVESTIGATION_NOT_FOUND}, status_code=404)
    if incident_id in customer_investigations_running:
        return JSONResponse({"error": "Investigation is currently running"}, status_code=409)
    if not _delete_customer_investigation_data(incident_id, incident):
        return JSONResponse({"error": CUSTOMER_INVESTIGATION_NOT_FOUND}, status_code=404)
    return {"deleted": True, "incident_id": incident_id}


def _delete_customer_investigation_data(incident_id: str, incident: dict[str, Any]) -> bool:
    linked_feedback_ids = {
        item.get("feedback_id")
        for item in incident.get("evidence", {}).get("customer_feedback", [])
        if item.get("feedback_id")
    }
    alert_feedback_id = incident.get("alert", {}).get("customer_feedback", {}).get("feedback_id")
    if alert_feedback_id:
        linked_feedback_ids.add(alert_feedback_id)
    routine_store.delete_incident_data(incident_id)
    customer_intelligence.store.delete_investigation_data(incident_id, linked_feedback_ids)
    return orchestrator_agent.delete_customer_investigation(incident_id)


def _purge_legacy_archived_customer_investigations() -> None:
    for incident_id in orchestrator_agent.archived_customer_investigation_ids():
        incident = orchestrator_agent.get_incident(incident_id)
        if incident:
            _delete_customer_investigation_data(incident_id, incident)


async def _rerun_customer_investigation(incident_id: str) -> None:
    try:
        incident = orchestrator_agent.get_incident(incident_id)
        if not incident:
            return
        feedback = incident.get("alert", {}).get("customer_feedback", {})
        query = _customer_knowledge_query(feedback) if feedback else str(incident.get("summary", ""))[:300]
        try:
            incident["evidence"]["sops"] = knowledge_base.search_documents(query, limit=8)
        except KnowledgeBaseError:
            incident["evidence"]["sops"] = {"evidence": [], "error": "Knowledge retrieval unavailable"}
        incident = orchestrator_agent.save_knowledge_evidence(incident_id, incident["evidence"]["sops"]) or incident
        await asyncio.wait_for(_run_selected_routine(incident["alert"], incident), timeout=150)
    finally:
        customer_investigations_running.discard(incident_id)


@app.post("/api/customer-intelligence/investigations/{incident_id}/run", status_code=202)
async def rerun_customer_investigation(incident_id: str, background_tasks: BackgroundTasks):
    incident = orchestrator_agent.get_incident(incident_id)
    if (not incident or not incident.get("alert", {}).get("customer_feedback")
            or orchestrator_agent.is_customer_investigation_archived(incident_id)):
        return JSONResponse({"error": CUSTOMER_INVESTIGATION_NOT_FOUND}, status_code=404)
    if incident_id in customer_investigations_running:
        return JSONResponse({"error": "Investigation is already running"}, status_code=409)
    if len(customer_investigations_running) >= 3:
        return JSONResponse({"error": "Investigation worker is busy"}, status_code=429)
    if incident.get("poc", {}).get("customer_ticket_decision") or incident.get("poc", {}).get("ticket"):
        return JSONResponse({"error": "This investigation has an immutable ticket decision; review its saved results"}, status_code=409)
    customer_investigations_running.add(incident_id)
    background_tasks.add_task(_rerun_customer_investigation, incident_id)
    return {"status": "queued", "incident_id": incident_id,
            "previous_run_id": incident.get("poc", {}).get("routine_execution", {}).get("run_id")}


@app.get("/api/customer-intelligence/investigations/{incident_id}")
async def customer_investigation_detail(incident_id: str):
    incident = orchestrator_agent.get_incident(incident_id)
    if not incident or not incident.get("alert", {}).get("customer_feedback"):
        return JSONResponse({"error": CUSTOMER_INVESTIGATION_NOT_FOUND}, status_code=404)
    run = routine_store.latest_incident_run(incident_id)
    if run:
        run.pop("input", None)
        run.pop("output", None)
        for task in run.get("tasks", []):
            task["input"] = {"incident_id": incident_id, "feedback_ids": [item.get("feedback_id") for item in incident.get("evidence", {}).get("customer_feedback", [])],
                             "context_note": "Complete input is retained in the execution store"}
    return {"incident": incident, "run": run, "running": incident_id in customer_investigations_running}


def _customer_monitor_result(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    feedback = context.get("alert", {}).get("customer_feedback", {})
    return {"output": {"agent": "AG-08", "classification": feedback.get("analysis", {}), "source": feedback.get("source")},
            "evidence": [{"feedback_id": feedback.get("feedback_id"), "kind": "customer_report_not_log"}]}


def _customer_preview_result(context: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    incident = context["incident"]
    return {"output": {"agent": "AG-27", "status": "PENDING_APPROVAL", "ticket_created": False,
                        "summary": incident["summary"], "recommendation": context.get("routine_resolution", {}),
                        "approval_endpoint": "/api/customer-intelligence/approvals/{incident_id}/approve"},
            "evidence": [{"feedback_id": incident["alert"]["customer_feedback"]["feedback_id"]}]}


async def _investigate_customer_feedback(identifier: str, record: FeedbackInput, analysis: FeedbackAnalysis) -> dict[str, Any]:
    services = {"battery_status": "vehicle-telemetry-api", "connectivity": "vehicle-telemetry-api",
                "login_authentication": "authentication-service"}
    service = services.get(analysis.topic, "customer-experience")
    content = monitoring_agent.feedback.scrub_pii(record.original_content)
    event = {"@timestamp": datetime.now(timezone.utc).isoformat(), "event.id": identifier,
             "service.name": service, "message": content}
    signal = {"type": "customer_complaint", "service": service, "message": content,
              "signature": f"customer:{record.source}:{analysis.topic}", "metric": analysis.topic}
    alerts = monitoring_agent.events.detect(event, [signal])
    if alerts:
        alert = alerts[0]
    else:
        alert = {"first_seen": event["@timestamp"], "detected_at": time.time(), "severity": "medium",
                 "service": service, "signal": signal, "event_evidence": {"event_id": identifier}}
    alert["alert_id"] = f"customer-{identifier}"
    alert["fingerprint"] = signal["signature"]
    alert["customer_feedback"] = {"feedback_id": identifier, "source": record.source, "content": content,
                                  "product": record.product, "analysis": analysis.model_dump(),
                                  "source_url": record.source_url, "experience_type": record.experience_type,
                                  "quality_note": record.quality_note, "published_date_display": record.published_date_display,
                                  "service_mapping": "candidate only; not a verified affected service"}
    if analysis.topic == "safety_concern":
        alert["severity"] = "high"
    incident = await _build_investigation(alert)
    return incident


async def _customer_ticket_decided(incident_id: str, decision: dict[str, Any]) -> None:
    incident = orchestrator_agent.get_incident(incident_id)
    if not incident:
        return
    workflow = incident.get("poc", {})
    workflow["customer_ticket_decision"] = decision["status"]
    workflow["ticket"] = decision.get("ticket")
    execution = workflow.get("routine_execution", {})
    run = routine_store.get_run(execution.get("run_id", ""))
    if run and run["status"] == "waiting_approval":
        resumed = await routine_executor.resume(run["run_id"], _studio_task_handlers(), approved=decision["status"] == "CREATED",
                                                operator_id=os.environ.get("CX_OPERATOR_ID", "local-cx-operator"))
        workflow["routine_execution"] = {**execution, "status": resumed["status"]}
    workflow["report"] = reporting_agent.build({**incident, "poc": workflow})
    orchestrator_agent.save_poc_workflow(incident_id, workflow)


async def _answer_customer_question(record: FeedbackInput) -> dict[str, Any]:
    query = monitoring_agent.feedback.scrub_pii(record.original_content)
    try:
        retrieval = knowledge_base.search_documents(query, limit=8)
    except KnowledgeBaseError:
        return {"status": "insufficient_evidence", "summary": "The knowledge base could not be searched. Please contact Flying Flea support.",
                "steps": [], "evidence": []}
    passages = [item for item in retrieval.get("evidence", []) if item.get("id") and not item.get("stale")]
    if customer_ai.status()["enabled"]:
        try:
            answer = await customer_ai.answer(query, passages)
            lookup = {str(item["id"]): item for item in passages}
            return {"status": answer.status, "summary": answer.summary,
                    "steps": [{"text": step.text, "evidence": [lookup[reference].get("citation", {}) for reference in step.evidence_ids]}
                              for step in answer.steps],
                    "evidence": [{"id": item["id"], "text": item.get("text", ""),
                                  "citation": item.get("citation", {}), "score": item.get("score")}
                                 for item in passages]}
        except Exception as exc:
            ai_error = str(exc) if isinstance(exc, ValueError) else "Configured answer model failed"
    else:
        ai_error = "No answer model is configured; showing retrieved evidence without generating instructions."
    return {"status": "evidence_only" if passages else "insufficient_evidence",
            "summary": "The following source excerpts may help. They are not a generated procedure; check the cited document." if passages
            else "No relevant documented answer was found. Please add details or request support.",
            "steps": [], "ai_note": ai_error,
            "evidence": [{"id": item["id"], "text": item.get("text", ""),
                          "citation": item.get("citation", {}), "score": item.get("score")}
                         for item in passages]}


customer_intelligence = CustomerIntelligence(
    FeedbackStore(Path(__file__).with_name("data") / "customer_intelligence.sqlite3"),
    monitoring_agent.feedback.scrub_pii, _investigate_customer_feedback, _customer_ticket_decided,
    _answer_customer_question,
)
app.include_router(customer_intelligence.router())


async def _orchestrate_new_alerts() -> None:
    processed_groups: dict[str, float] = {}
    while True:
        alert_groups = alert_routing.group(monitoring_agent.alerts(100))
        cutoff = time.time() - 600
        processed_groups = {
            key: processed_at for key, processed_at in processed_groups.items()
            if processed_at >= cutoff
        }
        for group_key, related_alerts in alert_groups.items():
            if any(alert.get("customer_feedback") for alert in related_alerts):
                continue
            if group_key in processed_groups:
                continue
            meaningful_alerts = alert_routing.actionable_signals(related_alerts)
            if not meaningful_alerts:
                processed_groups[group_key] = time.time()
                continue
            meaningful_alerts = [
                alert for alert in meaningful_alerts
                if alert.get("signal", {}).get("type") != "baseline_anomaly"
            ]
            if not meaningful_alerts:
                processed_groups[group_key] = time.time()
                continue
            detected_at = min(float(alert.get("detected_at", time.time())) for alert in related_alerts)
            if time.time() - detected_at < 1.0:
                continue
            primary_alert = alert_routing.attach_related_signals(
                alert_routing.primary_alert(meaningful_alerts),
                related_alerts,
                meaningful_alerts,
            )
            processed_groups[group_key] = time.time()
            try:
                await _build_investigation(primary_alert)
                log(
                    logging.INFO,
                    "orchestrator",
                    "Monitoring alert automatically entered the incident workflow",
                    event_action="incident.workflow.started",
                    alert_id=primary_alert["alert_id"],
                    related_alert_count=len(related_alerts),
                )
            except Exception as exc:
                processed_groups.pop(group_key, None)
                log(
                    logging.ERROR,
                    "orchestrator",
                    "Automatic incident workflow could not process alert",
                    event_action="incident.workflow.failed",
                    alert_id=primary_alert["alert_id"],
                    error=str(exc),
                )
        await asyncio.sleep(0.5)


@app.post("/api/orchestrator/investigations")
async def create_investigation(payload: InvestigationRequest):
    alert = next(
        (item for item in monitoring_agent.alerts(500) if item["alert_id"] == payload.alert_id),
        None,
    )
    if alert is None:
        return JSONResponse({"error": "alert not found or no longer retained"}, status_code=404)
    return await _build_investigation(alert)


@app.post("/api/orchestrator/incidents/{incident_id}/poc/ticket")
async def create_poc_ticket(incident_id: str):
    incident = orchestrator_agent.get_incident(incident_id)
    if incident is None:
        return JSONResponse({"error": "incident not found"}, status_code=404)
    if incident.get("alert", {}).get("customer_feedback"):
        return JSONResponse({"error": "Customer tickets require the authenticated Customer Intelligence approval endpoint."}, status_code=403)
    workflow = incident.get("poc")
    if not workflow or not workflow.get("rca"):
        return JSONResponse({"error": "incident assessment is not ready"}, status_code=409)
    if not workflow.get("ticket"):
        workflow = {
            **workflow,
            "ticket": ticketing_agent.create_draft(incident, workflow["rca"]),
        }
        incident = {**incident, "poc": workflow}
        workflow["report"] = reporting_agent.build(incident)
        incident = orchestrator_agent.save_poc_workflow(incident_id, workflow)
    routine_execution=workflow.get("routine_execution",{})
    run_id=routine_execution.get("run_id")
    run=routine_store.get_run(run_id) if run_id else None
    if run and run.get("status")=="waiting_operator":
        resumed=await routine_executor.resume(run_id,_studio_task_handlers(),approved=True,operator_id="incident-ticket-action")
        workflow={**workflow,"routine_execution":{**routine_execution,"status":resumed.get("status")}}
        incident=orchestrator_agent.save_poc_workflow(incident_id,workflow) or incident
    return incident


@app.post("/api/orchestrator/incidents/{incident_id}/poc/approval")
async def review_poc_action(incident_id: str, payload: POCApprovalRequest):
    incident = orchestrator_agent.get_incident(incident_id)
    if incident is None:
        return JSONResponse({"error": "incident not found"}, status_code=404)
    if incident.get("alert", {}).get("customer_feedback"):
        return JSONResponse({"error": "Customer issues require manual verification; synthetic recovery cannot resolve them."}, status_code=409)
    workflow = incident.get("poc")
    if not workflow or not workflow.get("resolution", {}).get("actions"):
        return JSONResponse({"error": "run POC analysis before reviewing an action"}, status_code=409)
    resolution = workflow["resolution"]
    if payload.approved:
        resolution = resolution_agent.execute_simulation(resolution, approved=True)
    else:
        action = resolution["actions"][0]
        resolution = {**resolution, "actions": [{**action, "status": "rejected_by_operator"}]}
    workflow = {**workflow, "resolution": resolution}
    workflow["approval"] = {
        "decision": "approved" if payload.approved else "rejected",
        "note": payload.note,
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    routine_execution=workflow.get("routine_execution",{})
    run_id=routine_execution.get("run_id")
    run=routine_store.get_run(run_id) if run_id else None
    if run and run.get("status")=="waiting_approval":
        resumed=await routine_executor.resume(run_id,_studio_task_handlers(),approved=payload.approved,operator_id="incident-approval")
        workflow["routine_execution"]={**routine_execution,"status":resumed.get("status")}
    incident = {**incident, "poc": workflow}
    workflow["report"] = reporting_agent.build(incident)
    return orchestrator_agent.save_poc_workflow(incident_id, workflow)


@app.post("/api/orchestrator/incidents/{incident_id}/poc/ticket/status")
async def update_poc_ticket(incident_id: str, payload: POCTicketStatusRequest):
    incident = orchestrator_agent.get_incident(incident_id)
    if incident is None:
        return JSONResponse({"error": "incident not found"}, status_code=404)
    workflow = incident.get("poc")
    if not workflow or not workflow.get("ticket"):
        return JSONResponse({"error": "run POC analysis before updating its ticket"}, status_code=409)
    try:
        workflow = {**workflow, "ticket": ticketing_agent.update_status(workflow["ticket"], payload.status)}
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=422)
    incident = {**incident, "poc": workflow}
    workflow["report"] = reporting_agent.build(incident)
    return orchestrator_agent.save_poc_workflow(incident_id, workflow)


@app.get("/api/orchestrator/incidents/{incident_id}")
async def get_orchestrator_incident(incident_id: str):
    incident = orchestrator_agent.get_incident(incident_id)
    if incident is None:
        return JSONResponse({"error": "incident not found"}, status_code=404)
    return incident


@app.post("/api/orchestrator/incidents/{incident_id}/review")
async def review_orchestrator_incident(incident_id: str, payload: IncidentReviewRequest):
    try:
        incident = orchestrator_agent.review_incident(incident_id, payload.status, payload.note)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    if incident is None:
        return JSONResponse({"error": "incident not found"}, status_code=404)
    return incident


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
