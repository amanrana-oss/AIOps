"""MA-19: transparent evidence rules for candidate failure-pattern matching."""

from __future__ import annotations

from typing import Any


class FailurePatternScorer:
    """Score explicit signal matches without treating patterns as causal findings."""

    DB_POOL_MAX_FIELD = "db.pool.max"
    DB_POOL_ACTIVE_FIELD = "db.pool.active"

    def analyze(
        self,
        events: list[dict[str, Any]],
        temporal: dict[str, Any],
    ) -> list[dict[str, Any]]:
        text = [str(event.get("message", "")).casefold() for event in events]
        failed = any(
            event.get("event.outcome") == "failure"
            or event.get("log.level") in {"error", "critical"}
            or (isinstance(event.get("http.response.status_code"), (int, float))
                and event["http.response.status_code"] >= 500)
            for event in events
        )
        deployment_nearby = bool(temporal.get("nearby_changes"))
        database_pressure = any(
            (isinstance(event.get(self.DB_POOL_ACTIVE_FIELD), (int, float))
             and isinstance(event.get(self.DB_POOL_MAX_FIELD), (int, float))
             and event[self.DB_POOL_MAX_FIELD] > 0
             and event[self.DB_POOL_ACTIVE_FIELD] / event[self.DB_POOL_MAX_FIELD] >= 0.8)
            or any(term in str(event.get("message", "")).casefold()
                   for term in ("connection pool", "acquisition timed out", "pool utilization"))
            for event in events
        )
        kafka_lag = any(
            isinstance(event.get("messaging.consumer.lag"), (int, float))
            and event["messaging.consumer.lag"] >= 150
            for event in events
        )
        exception_evidence = any(
            event.get("error.type") or event.get("error.stack_trace") or event.get("stack_trace")
            for event in events
        )
        high_latency = any(
            isinstance(event.get("latency_ms"), (int, float)) and event["latency_ms"] >= 1000
            or isinstance(event.get("monitoring.latency_ms"), (int, float))
            and event["monitoring.latency_ms"] >= 1000
            for event in events
        )
        kafka_retry = any("retry" in message or "lag" in message for message in text)

        rules = [
            ("application_exception", [exception_evidence, failed], "Exception evidence and a failed event"),
            ("database_pool_pressure", [database_pressure, failed], "Database pool pressure and a failed event"),
            ("messaging_lag", [kafka_lag, kafka_retry], "Elevated consumer lag and retry/lag evidence"),
            ("deployment_with_symptoms", [deployment_nearby, failed or high_latency], "A nearby deployment and failure/latency evidence"),
            ("high_latency", [high_latency], "Latency at or above 1000 ms"),
        ]
        candidates = []
        for pattern, checks, description in rules:
            matched = sum(checks)
            if not matched:
                continue
            candidates.append({
                "pattern": pattern,
                "rule": description,
                "rule_score": round(matched / len(checks), 2),
                "matched_checks": matched,
                "total_checks": len(checks),
                "cause_confirmed": False,
            })
        return candidates