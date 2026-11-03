"""AG-08 coordinator: normalize, monitor, correlate, and structure events."""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from pathlib import Path
from typing import Any

from .ma09_source_collector import SourceCollectorMicroAgent
from .ma10_data_normalization import DataNormalizationMicroAgent
from .ma11_service_health import ServiceHealthMicroAgent
from .ma12_log_trace_analyzer import LogTraceAnalyzerMicroAgent
from .ma13_anomaly_detection import AnomalyDetectionMicroAgent
from .ma14_event_detection import EventDetectionMicroAgent
from .ma15_feedback_analyzer import FeedbackAnalyzerMicroAgent


class MonitoringAgent:
    """Connect monitor microagents into one bounded background event pipeline."""

    def __init__(self, services: tuple[str, ...], review_dataset_path: Path) -> None:
        self.collector = SourceCollectorMicroAgent(services)
        self.normalizer = DataNormalizationMicroAgent()
        self.health = ServiceHealthMicroAgent()
        self.log_trace = LogTraceAnalyzerMicroAgent()
        self.anomaly = AnomalyDetectionMicroAgent()
        self.events = EventDetectionMicroAgent()
        self.feedback = FeedbackAnalyzerMicroAgent(review_dataset_path)
        self._processed = 0
        self._malformed = 0
        self._quality_warnings: Counter[str] = Counter()
        self._last_processed_at: str | None = None
        self._started_at = time.time()
        self._counter_lock = asyncio.Lock()

    def submit(self, event: dict[str, Any], connector_id: str = "simulator") -> bool:
        return self.collector.collect(event, connector_id)

    async def run(self) -> None:
        while True:
            envelope = await self.collector.next_event()
            try:
                event = self.normalizer.normalize(envelope["event"], envelope["connector_id"])
                self.health.observe(event)
                signals = self.anomaly.analyze(event)
                failure = self.log_trace.analyze(event)
                if failure:
                    signals.append(failure)
                self.events.detect(event, signals)
                async with self._counter_lock:
                    self._processed += 1
                    self._last_processed_at = event["@timestamp"]
                    self._quality_warnings.update(event["monitoring.quality_warnings"])
            except (TypeError, ValueError, KeyError):
                async with self._counter_lock:
                    self._malformed += 1

    async def status(self) -> dict[str, Any]:
        async with self._counter_lock:
            processed = self._processed
            malformed = self._malformed
            last_processed = self._last_processed_at
            quality_warnings = dict(self._quality_warnings)
        return {
            "agent": "AG-08 Monitoring Agent",
            "running": True,
            "uptime_seconds": round(time.time() - self._started_at, 1),
            "events_processed": processed,
            "malformed_events": malformed,
            "last_processed_at": last_processed,
            "microagents": {
                "MA-09_source_collector_connector_manager": self.collector.status(),
                "MA-10_data_normalization_quality": {
                    "state": "ready",
                    "events_processed": processed,
                    "malformed_events": malformed,
                    "quality_warnings": quality_warnings,
                },
                "MA-11_service_health_sli_slo": self.health.status(),
                "MA-12_log_trace_signal_analyzer": self.log_trace.status(),
                "MA-13_anomaly_detection": self.anomaly.status(),
                "MA-14_event_detection_dedup_alerting": self.events.status(),
                "MA-15_feedback_sentiment_topic_analyzer": self.feedback.summarize_dataset(),
            },
            "model_requirements": {
                "required_now": [],
                "optional_future": {
                    "MA-13": "Offline time-series anomaly model; rules and baselines work without it.",
                    "MA-15": "Offline multilingual sentiment/topic model; deterministic heuristic works without it.",
                },
            },
        }

    def health_status(self, service: str | None = None) -> dict[str, Any]:
        return self.health.service_status(service)

    def alerts(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.events.alerts(limit)

    def trace(self, trace_id: str) -> dict[str, Any]:
        return self.log_trace.trace(trace_id)

    def feedback_status(self) -> dict[str, Any]:
        return self.feedback.summarize_dataset()
