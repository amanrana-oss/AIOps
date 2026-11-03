"""MA-13: rolling baseline and threshold anomaly signals without model inference."""

from __future__ import annotations

import statistics
import threading
import time
from collections import defaultdict, deque
from typing import Any


class AnomalyDetectionMicroAgent:
    """Score high latency, saturation, and outliers against recent baselines."""

    def __init__(
        self,
        baseline_size: int = 120,
        min_baseline_samples: int = 20,
        max_series: int = 2_000,
    ) -> None:
        self.baseline_size = baseline_size
        self.min_baseline_samples = min_baseline_samples
        self.max_series = max_series
        self._baselines: dict[tuple[str, str], deque[float]] = defaultdict(
            lambda: deque(maxlen=self.baseline_size)
        )
        self._dropped_series = 0
        self._lock = threading.Lock()

    def analyze(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        service = str(event.get("service.name", "unknown"))
        signals: list[dict[str, Any]] = []
        latency = event.get("monitoring.latency_ms")
        if isinstance(latency, (int, float)):
            self._observe_metric(service, "latency_ms", float(latency), signals, threshold=1000.0)
        for aliases, metric, threshold in (
            (("memory_percent", "container.memory.usage.percent"), "memory_percent", 90.0),
            (("cpu_percent", "system.cpu.utilization.percent"), "cpu_percent", 90.0),
            (("disk_percent", "system.filesystem.percent"), "disk_percent", 90.0),
            (("kafka_lag", "messaging.consumer.lag", "messaging.kafka.consumer.lag"), "kafka_lag", 150.0),
        ):
            value = next((event.get(alias) for alias in aliases if event.get(alias) is not None), None)
            if isinstance(value, (int, float)):
                self._observe_metric(service, metric, float(value), signals, threshold=threshold)
        memory_bytes = event.get("container.memory.usage.bytes") or event.get("memory.usage.bytes")
        if isinstance(memory_bytes, (int, float)):
            self._observe_metric(service, "memory_mb", float(memory_bytes) / (1024 * 1024), signals, threshold=900.0)
        pool_active = event.get("pool_active", event.get("db.pool.active"))
        pool_max = event.get("pool_max", event.get("db.pool.max"))
        if isinstance(pool_active, (int, float)) and isinstance(pool_max, (int, float)) and pool_max > 0:
            utilization = float(pool_active) / float(pool_max) * 100
            self._observe_metric(service, "db_pool_percent", utilization, signals, threshold=90.0)
        return signals

    def _observe_metric(
        self,
        service: str,
        metric: str,
        value: float,
        signals: list[dict[str, Any]],
        threshold: float,
    ) -> None:
        now = time.time()
        key = (service, metric)
        with self._lock:
            if key not in self._baselines and len(self._baselines) >= self.max_series:
                self._dropped_series += 1
                return
            baseline = self._baselines[key]
            historical = list(baseline)
            baseline.append(value)
        if value >= threshold:
            signals.append({
                "type": "threshold_anomaly",
                "service": service,
                "metric": metric,
                "value": round(value, 3),
                "threshold": threshold,
                "observed_at": now,
            })
        if len(historical) >= self.min_baseline_samples:
            mean = statistics.fmean(historical)
            deviation = statistics.pstdev(historical)
            if value > mean + max(3 * deviation, threshold * 0.5):
                signals.append({
                    "type": "baseline_anomaly",
                    "service": service,
                    "metric": metric,
                    "value": round(value, 3),
                    "baseline_mean": round(mean, 3),
                    "baseline_stddev": round(deviation, 3),
                    "observed_at": now,
                })

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "baseline_series": len(self._baselines),
                "baseline_capacity": self.baseline_size,
                "minimum_samples": self.min_baseline_samples,
                "maximum_series": self.max_series,
                "dropped_series_observations": self._dropped_series,
                "model_required": False,
            }
