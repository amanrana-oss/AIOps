"""Synthetic, correlated multi-service event generator."""

import asyncio
import logging
import random
import time
import uuid
from collections.abc import Callable

EmitLog = Callable[..., None]

SCENARIO_DESCRIPTIONS = {
    "normal": "Normal requests across the simulated service environment.",
    "code_bug": "Telemetry API raises a ZeroDivisionError and returns HTTP 500.",
    "db_exhaustion": "PostgreSQL connection usage ramps up and requests time out.",
    "kafka_lag": "Kafka consumer throughput drops and queue lag accumulates.",
    "faulty_deployment": "A simulated release increases latency and request failures.",
    "high_latency": "API response latency gradually rises while most requests succeed.",
    "infrastructure": "Memory and disk pressure cause health warnings and restarts.",
    "recovery": "Service health, database pressure, and consumer lag gradually improve.",
}

SCENARIO_ALIASES = {
    "db_outage": "db_exhaustion",
    "cascading": "db_exhaustion",
    "slow": "high_latency",
    "memory_leak": "infrastructure",
    "error_spike": "code_bug",
}

SERVICES = (
    "api-gateway",
    "authentication-service",
    "vehicle-telemetry-api",
    "order-management-service",
    "postgresql",
    "redis",
    "kafka",
    "notification-service",
    "kubernetes",
    "deployment-pipeline",
)


class Simulator:
    """Runs bounded-rate synthetic service flows on the app's event loop."""

    def __init__(self, emit: EmitLog) -> None:
        self.emit = self._emit_observation
        self._write_log = emit
        self.running = True
        self.frequency = 1.0
        self.scenario = "normal"
        self.started_at = time.time()
        self.scenario_started_at = self.started_at
        self.recovery_from = "normal"
        self.recovery_steps = 0
        self.db_connections = 4
        self.kafka_lag = 0
        self.memory_mb = 256
        self.disk_percent = 42
        self.container_restarts = 0
        self.requests = 0
        self.errors = 0
        self.version = "1.0.0"
        self.deployment_step = 0
        self.deployment_id = ""

    def _emit_observation(self, level: int, service: str, message: str, **fields) -> None:
        public_fields = {
            key: value for key, value in fields.items()
            if not key.startswith("scenario.")
        }
        self._write_log(level, service, message, **public_fields)

    @property
    def scenario_age(self) -> float:
        return max(0.0, time.time() - self.scenario_started_at)

    def set_scenario(self, name: str) -> None:
        name = SCENARIO_ALIASES.get(name, name)
        if name not in SCENARIO_DESCRIPTIONS:
            raise ValueError(f"Unknown scenario: {name}")
        if name == "recovery":
            self.recover()
            return
        self.scenario = name
        self.scenario_started_at = time.time()
        self.recovery_steps = 0
        self.deployment_step = 0
        self.deployment_id = uuid.uuid4().hex[:12] if name == "faulty_deployment" else ""
        self.emit(
            logging.INFO, "deployment-pipeline", "Simulation configuration updated",
            event_action="simulator.configure", event_outcome="success",
        )

    def recover(self) -> None:
        if self.scenario != "recovery":
            self.recovery_from = self.scenario
        self.scenario = "recovery"
        self.scenario_started_at = time.time()
        self.recovery_steps = 0
        self.emit(
            logging.WARNING, "deployment-pipeline", "Recovery sequence started",
            event_action="incident.recovery", event_outcome="unknown",
        )

    def reset(self) -> None:
        self.running = True
        self.scenario = "normal"
        self.scenario_started_at = time.time()
        self.db_connections = 4
        self.kafka_lag = 0
        self.memory_mb = 256
        self.disk_percent = 42
        self.container_restarts = 0
        self.requests = 0
        self.errors = 0
        self.frequency = 1.0
        self.started_at = time.time()
        self.version = "1.0.0"
        self.deployment_step = 0
        self.recovery_steps = 0
        self.deployment_id = ""
        self.emit(
            logging.INFO, "deployment-pipeline", "Simulator state reset",
            event_action="simulator.reset", event_outcome="success",
        )

    def status(self) -> dict:
        return {
            "running": self.running,
            "frequency": self.frequency,
            "scenario": self.scenario,
            "scenario_description": SCENARIO_DESCRIPTIONS[self.scenario],
            "scenario_age_seconds": round(self.scenario_age, 1),
            "uptime_seconds": round(max(0.0, time.time() - self.started_at), 1),
            "requests": self.requests,
            "errors": self.errors,
            "error_rate": round(self.errors / max(self.requests, 1), 4),
            "db_connections_active": self.db_connections,
            "db_connections_max": 20,
            "kafka_consumer_lag": self.kafka_lag,
            "memory_mb": self.memory_mb,
            "disk_percent": self.disk_percent,
            "container_restarts": self.container_restarts,
            "application_version": self.version,
            "services": list(SERVICES),
        }

    def _ids(self) -> dict[str, str]:
        return {
            "request_id": uuid.uuid4().hex[:16],
            "trace.id": uuid.uuid4().hex,
            "span.id": uuid.uuid4().hex[:16],
            "transaction.id": uuid.uuid4().hex[:16],
        }

    def _emit_flow(self) -> None:
        scenario = self.scenario
        ids = self._ids()
        status = 200
        latency_ms = random.uniform(35, 180)
        failed = False
        database_failed = False
        consumer_failed = False
        code_bug_failed = False

        if scenario == "db_exhaustion":
            pressure_seconds = max(0.0, self.scenario_age - 10)
            self.db_connections = min(20, 4 + int(pressure_seconds / 6))
            if self.db_connections >= 12:
                latency_ms = random.uniform(900, 2200)
                database_failed = random.random() < min(0.12, 0.02 + pressure_seconds / 600)
                failed = database_failed
                status = 503 if failed else 200
            elif self.db_connections >= 8:
                latency_ms = random.uniform(700, 1600)
                database_failed = random.random() < 0.015
                failed = database_failed
                status = 503 if failed else 200
        elif scenario == "kafka_lag":
            lag_seconds = max(0.0, self.scenario_age - 15)
            self.kafka_lag = min(100_000, int(lag_seconds * 4))
            latency_ms = random.uniform(100, 300) + min(700, lag_seconds * 2)
        elif scenario == "faulty_deployment":
            if self.deployment_step == 0:
                self.deployment_step = 1
                self.emit(
                    logging.INFO, "deployment-pipeline", "Deployment started",
                    event_action="deployment.started", event_outcome="unknown",
                    **{"scenario.name": scenario, "service.version": "1.1.0",
                       "deployment.id": self.deployment_id, "environment": "staging"},
                )
            if self.scenario_age >= 15 and self.deployment_step == 1:
                self.deployment_step = 2
                self.version = "1.1.0"
                self.emit(
                    logging.INFO, "deployment-pipeline", "Deployment completed",
                    event_action="deployment.completed", event_outcome="success",
                    **{"scenario.name": scenario, "service.version": self.version,
                       "deployment.id": self.deployment_id, "environment": "staging"},
                )
            rollout_age = max(0.0, self.scenario_age - 20)
            if self.deployment_step == 2:
                self.db_connections = min(20, 4 + int(rollout_age / 10))
                self.kafka_lag = min(100_000, int(rollout_age * 2))
                database_failed = (
                    self.db_connections >= 12 and random.random() < 0.04
                )
                latency_ms = random.uniform(100, 250) + min(1600, rollout_age * 12)
                failed = database_failed or random.random() < min(0.25, 0.01 + rollout_age / 400)
                status = 500 if failed else 200
        elif scenario == "high_latency":
            latency_ramp = max(0.0, self.scenario_age - 15)
            latency_ms = random.uniform(70, 180) + min(1700, latency_ramp * 18)
        elif scenario == "infrastructure":
            pressure_seconds = max(0.0, self.scenario_age - 10)
            self.memory_mb = min(2048, 256 + int(pressure_seconds * 5))
            self.disk_percent = min(99, 42 + int(pressure_seconds / 4))
            if self.memory_mb >= 1000 and random.random() < 0.08:
                self.container_restarts += 1
                self.memory_mb = 320
                status = 503
                failed = True
            elif self.memory_mb >= 800 and random.random() < 0.04:
                status = 503
                failed = True
            latency_ms = random.uniform(100, 250) + max(0, self.memory_mb - 700) * 0.7
        elif scenario == "recovery":
            recovery_step = int(self.scenario_age / 3)
            if recovery_step > self.recovery_steps:
                step_delta = recovery_step - self.recovery_steps
                self.recovery_steps = recovery_step
                self.db_connections = max(4, self.db_connections - 2 * step_delta)
                self.kafka_lag = max(0, self.kafka_lag - random.randint(40, 120) * step_delta)
                self.memory_mb = max(256, self.memory_mb - 60 * step_delta)
                self.disk_percent = max(42, self.disk_percent - 3 * step_delta)
            latency_ms = max(60, 900 - self.recovery_steps * 70)
            if self.recovery_from == "faulty_deployment" and self.recovery_steps == 1:
                self.emit(
                    logging.WARNING, "deployment-pipeline", "Rollback initiated",
                    event_action="deployment.rollback", event_outcome="unknown",
                    **{"scenario.name": "recovery", "deployment.id": self.deployment_id,
                       "service.version": self.version},
                )
            elif self.recovery_from == "faulty_deployment" and self.recovery_steps == 2:
                self.version = "1.0.0"
                self.emit(
                    logging.INFO, "deployment-pipeline", "Rollback completed",
                    event_action="deployment.rollback", event_outcome="success",
                    **{"scenario.name": "recovery", "deployment.id": self.deployment_id,
                       "service.version": self.version},
                )
            if self.recovery_steps >= 12:
                self.scenario = "normal"
                self.version = "1.0.0"
                self.emit(
                    logging.INFO, "deployment-pipeline", "Recovery sequence completed",
                    event_action="incident.recovery", event_outcome="success",
                    **{"scenario.name": "recovery", "scenario.previous": self.recovery_from},
                )
                scenario = "normal"

        if scenario == "code_bug":
            bug_age = max(0.0, self.scenario_age - 15)
            failed = random.random() < min(0.12, 0.01 + bug_age / 900)
            code_bug_failed = failed
            status = 500 if failed else 200
            if failed:
                latency_ms = random.uniform(80, 260)

        if scenario == "normal" and random.random() < 0.005:
            failed = True
            status = 503
            latency_ms = random.uniform(500, 1400)

        self.requests += 1
        self.errors += int(failed)
        self.emit(
            logging.INFO, "api-gateway", "Inbound request routed",
            event_action="http.request.received", event_outcome="success",
            **ids, **{"scenario.name": scenario, "http.request.method": "GET",
                      "url.path": "/api/vehicles/telemetry", "client.address": "10.0.4.21"},
        )
        self.emit(
            logging.INFO, "authentication-service", "Service token validated",
            event_action="token.validate", event_outcome="success",
            **ids, **{"scenario.name": scenario, "user.name": "service-telemetry",
                      "auth.method": "service_token"},
        )
        if code_bug_failed:
            try:
                _ = 10 / 0
            except ZeroDivisionError:
                self.emit(
                    logging.ERROR, "vehicle-telemetry-api",
                    "Telemetry calculation failed",
                    exc=True, event_action="telemetry.calculate", event_outcome="failure",
                    **ids, **{"scenario.name": scenario, "error.code": "ZERO_DIVISION"},
                )

        if scenario == "normal" and failed:
            self.emit(
                logging.ERROR, "postgresql", "Transient query timeout",
                event_action="database.query", event_outcome="failure",
                **ids, **{"db.system": "postgresql", "db.name": "fleet",
                          "db.operation": "SELECT", "error.type": "QueryTimeout",
                          "duration_ms": round(latency_ms)},
            )

        if scenario in ("db_exhaustion", "faulty_deployment") and self.db_connections >= 8:
            if self.requests % 5 == 0 or database_failed:
                self.emit(
                    logging.ERROR if database_failed else logging.WARNING, "postgresql",
                    "Connection acquisition timed out" if database_failed
                    else "Connection pool utilization elevated",
                    event_action="database.connection",
                    event_outcome="failure" if database_failed else "unknown",
                    **ids, **{"scenario.name": scenario, "db.system": "postgresql",
                              "db.name": "fleet", "db.operation": "SELECT",
                              "db.pool.active": self.db_connections, "db.pool.max": 20,
                              "duration_ms": round(latency_ms)},
                )
        elif not failed:
            self.emit(
                logging.INFO, "postgresql", "Telemetry query completed",
                event_action="database.query", event_outcome="success",
                **ids, **{"scenario.name": scenario, "db.system": "postgresql",
                          "db.name": "fleet", "db.operation": "SELECT",
                          "duration_ms": round(random.uniform(4, 45))},
            )

        cache_hit = random.random() < 0.72
        self.emit(
            logging.INFO, "redis", "Cache hit" if cache_hit else "Cache miss",
            event_action="cache.lookup", event_outcome="success" if cache_hit else "unknown",
            **ids, **{"scenario.name": scenario, "cache.key": "vehicle:telemetry:sample",
                      "cache.hit": cache_hit},
        )

        self.emit(
            logging.INFO if not failed else logging.ERROR,
            "vehicle-telemetry-api",
            "Telemetry request completed" if not failed else "Telemetry request failed",
            event_action="telemetry.read",
            event_outcome="failure" if failed else "success",
            **ids, **{"scenario.name": scenario, "url.path": "/api/vehicles/telemetry",
                      "http.response.status_code": status, "latency_ms": round(latency_ms, 1),
                      "service.version": self.version},
        )
        self.emit(
            logging.INFO, "kafka", "Telemetry event published",
            event_action="messaging.publish", event_outcome="success",
            **ids, **{"scenario.name": scenario, "messaging.destination.name": "vehicle.telemetry",
                      "messaging.message.id": uuid.uuid4().hex[:16]},
        )
        if scenario in ("kafka_lag", "faulty_deployment") and self.kafka_lag > 150:
            consumer_failed = random.random() < min(0.25, 0.02 + self.kafka_lag / 12_000)
            if self.requests % 5 == 0 or consumer_failed:
                self.emit(
                    logging.ERROR if consumer_failed else logging.WARNING, "kafka",
                    "Consumer processing failed; message will be retried"
                    if consumer_failed else "Consumer lag above normal range",
                    event_action="messaging.process",
                    event_outcome="failure" if consumer_failed else "unknown",
                    **ids, **{"scenario.name": scenario,
                              "messaging.destination.name": "vehicle.telemetry",
                              "messaging.consumer.group": "notification-workers",
                              "messaging.consumer.lag": self.kafka_lag,
                              "retry.count": 2 if consumer_failed else 0},
                )
            if consumer_failed:
                self.emit(
                    logging.WARNING, "notification-service", "Notification dispatch delayed",
                    event_action="notification.dispatch", event_outcome="unknown",
                    **ids, **{"scenario.name": scenario,
                              "messaging.consumer.lag": self.kafka_lag,
                              "delay_ms": min(30_000, self.kafka_lag * 10)},
                )
        else:
            self.kafka_lag = max(0, self.kafka_lag - 8)
            self.emit(
                logging.INFO, "notification-service", "Telemetry notification dispatched",
                event_action="notification.dispatch", event_outcome="success",
                **ids, **{"scenario.name": scenario, "messaging.consumer.group": "notification-workers",
                          "messaging.consumer.lag": self.kafka_lag},
            )

        if scenario == "infrastructure" and self.requests % 5 == 0:
            level = logging.WARNING if self.memory_mb > 700 or self.disk_percent > 80 else logging.INFO
            self.emit(
                level, "kubernetes", "Container resource health sampled",
                event_action="container.health", event_outcome="unknown",
                **{"scenario.name": scenario, "container.name": "telemetry-api-7c8d",
                   "container.memory.usage.bytes": self.memory_mb * 1024 * 1024,
                   "system.filesystem.percent": self.disk_percent,
                   "container.restarts": self.container_restarts,
                   "health.status": "degraded" if status == 503 else "healthy"},
            )

        if scenario == "recovery":
            self.emit(
                logging.INFO, "kubernetes", "Service health improving",
                event_action="container.health", event_outcome="success",
                **ids, **{"scenario.name": "recovery", "health.status": "healthy",
                          "db.pool.active": self.db_connections,
                          "messaging.consumer.lag": self.kafka_lag,
                          "memory_mb": self.memory_mb, "disk_percent": self.disk_percent},
            )

        if self.requests % 3 == 0:
            order_id = uuid.uuid4().hex[:12]
            self.emit(
                logging.INFO, "order-management-service", "Order event processed",
                event_action="order.process", event_outcome="success",
                **ids, **{"scenario.name": scenario, "order.id": order_id,
                          "messaging.destination.name": "orders.created",
                          "order.status": "ACCEPTED"},
            )
        if self.requests % 5 == 0:
            self.emit(
                logging.WARNING if failed else logging.INFO, "kubernetes",
                "Container readiness probe failed" if failed else "Container readiness probe passed",
                event_action="container.health", event_outcome="failure" if failed else "success",
                **{"scenario.name": scenario, "container.name": "telemetry-api-7c8d",
                   "health.status": "degraded" if failed else "healthy",
                   "container.restarts": self.container_restarts},
            )
        if self.requests % 20 == 0:
            self.emit(
                logging.INFO, "deployment-pipeline", "Last successful release remains active",
                event_action="deployment.status", event_outcome="success",
                **{"scenario.name": scenario, "service.version": self.version,
                   "environment": "staging"},
            )

        self.emit(
            logging.ERROR if failed else logging.INFO,
            "api-gateway", "HTTP access log",
            event_action="http.request", event_outcome="failure" if failed else "success",
            **ids, **{"scenario.name": scenario, "http.request.method": "GET",
                      "url.path": "/api/vehicles/telemetry",
                      "http.response.status_code": status, "latency_ms": round(latency_ms, 1)},
        )

    async def run(self) -> None:
        while True:
            if not self.running:
                await asyncio.sleep(0.25)
                continue
            started = time.monotonic()
            self._emit_flow()
            await asyncio.sleep(max(0.05, 1 / max(self.frequency, 0.1) - (time.monotonic() - started)))
