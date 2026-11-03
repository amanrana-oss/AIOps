"""MA-11: rolling service health, SLIs, SLOs, and error-budget estimates."""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Any


class ServiceHealthMicroAgent:
    """Maintain bounded rolling health samples and evaluate configured SLOs."""

    def __init__(
        self,
        window_seconds: int = 300,
        availability_slo: float = 0.99,
        latency_slo_ms: float = 1000.0,
        max_samples_per_service: int = 2_000,
        max_services: int = 100,
    ) -> None:
        self.window_seconds = window_seconds
        self.availability_slo = availability_slo
        self.latency_slo_ms = latency_slo_ms
        self.max_samples_per_service = max_samples_per_service
        self.max_services = max_services
        self._untracked_service_events = 0
        self._samples: dict[str, deque[dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self.max_samples_per_service)
        )
        self._lock = threading.Lock()

    def observe(self, event: dict[str, Any]) -> None:
        service = str(event.get("service.name", "unknown"))
        try:
            timestamp = datetime.fromisoformat(str(event["@timestamp"]).replace("Z", "+00:00")).timestamp()
        except (KeyError, ValueError, TypeError):
            timestamp = time.time()
        status = event.get("http.response.status_code")
        failed = event.get("event.outcome") == "failure" or (
            isinstance(status, (int, float)) and status >= 500
        )
        latency = event.get("monitoring.latency_ms")
        latency_value = float(latency) if isinstance(latency, (int, float)) else None
        sample: dict[str, Any] = {
            "timestamp": timestamp,
            "success": not failed,
            "latency_ms": latency_value,
        }
        metric_fields = {
            "cpu_percent": ("cpu_percent", "system.cpu.utilization.percent"),
            "disk_percent": ("disk_percent", "system.filesystem.percent"),
            "memory_percent": ("memory_percent", "container.memory.usage.percent"),
            "kafka_lag": ("kafka_lag", "messaging.consumer.lag", "messaging.kafka.consumer.lag"),
            "container_restarts": ("container_restarts", "container.restarts"),
            "db_pool_active": ("db_pool_active", "db.pool.active", "pool_active"),
            "db_pool_max": ("db_pool_max", "db.pool.max", "pool_max"),
        }
        for normalized_name, *source_names in metric_fields.values():
            for source_name in source_names:
                metric_value = event.get(source_name)
                if isinstance(metric_value, (int, float)):
                    sample[normalized_name] = metric_value
                    break
        memory_bytes = event.get("container.memory.usage.bytes") or event.get("memory.usage.bytes")
        if isinstance(memory_bytes, (int, float)):
            sample["memory_mb"] = round(memory_bytes / (1024 * 1024), 2)
        if "db_pool_active" in sample and "db_pool_max" in sample and sample["db_pool_max"]:
            sample["db_pool_utilization_percent"] = round(
                sample["db_pool_active"] / sample["db_pool_max"] * 100, 2
            )
        with self._lock:
            if service not in self._samples and len(self._samples) >= self.max_services:
                self._untracked_service_events += 1
                return
            samples = self._samples[service]
            samples.append(sample)
            cutoff = time.time() - self.window_seconds
            while samples and samples[0]["timestamp"] < cutoff:
                samples.popleft()

    def service_status(self, service: str | None = None) -> dict[str, Any]:
        with self._lock:
            names = [service] if service else sorted(self._samples)
            now = time.time()
            results: dict[str, Any] = {}
            for name in names:
                all_samples = self._samples.get(name, ())
                samples = [
                    sample for sample in all_samples
                    if sample["timestamp"] >= now - self.window_seconds
                ]
                total = len(samples)
                successes = sum(1 for sample in samples if sample["success"])
                latencies = sorted(
                    sample["latency_ms"] for sample in samples if sample["latency_ms"] is not None
                )
                availability = successes / total if total else None
                p95 = self._percentile(latencies, 0.95)
                latest_metrics = {
                    key: value
                    for key, value in (samples[-1].items() if samples else [])
                    if key not in {"timestamp", "success", "latency_ms"}
                }
                error_budget_remaining = (
                    max(0.0, min(1.0, (availability - self.availability_slo) / (1 - self.availability_slo)))
                    if availability is not None and self.availability_slo < 1
                    else None
                )
                latency_ok = p95 is None or p95 <= self.latency_slo_ms
                availability_ok = availability is None or availability >= self.availability_slo
                results[name] = {
                    "window_seconds": self.window_seconds,
                    "samples": total,
                    "throughput_per_minute": round(total * 60 / self.window_seconds, 2),
                    "throughput_unit": "normalized_events_per_minute",
                    "availability_failure_rule": "event.outcome=failure or HTTP status >= 500",
                    "availability": round(availability, 4) if availability is not None else None,
                    "error_rate": round(1 - availability, 4) if availability is not None else None,
                    "latency_p95_ms": round(p95, 2) if p95 is not None else None,
                    "availability_slo": self.availability_slo,
                    "latency_slo_ms": self.latency_slo_ms,
                    "error_budget_remaining": round(error_budget_remaining, 4) if error_budget_remaining is not None else None,
                    "latest_saturation_and_dependency_metrics": latest_metrics,
                    "health": "insufficient_data" if total < 5 else "healthy" if availability_ok and latency_ok else "degraded",
                }
            return {
                "window_seconds": self.window_seconds,
                "max_services": self.max_services,
                "untracked_service_events": self._untracked_service_events,
                "services": results,
            }

    def status(self) -> dict[str, Any]:
        return self.service_status()

    def _percentile(self, values: list[float], percentile: float) -> float | None:
        if not values:
            return None
        return values[min(len(values) - 1, math.ceil(len(values) * percentile) - 1)]
