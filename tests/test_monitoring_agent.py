import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from monitoring_agent.ma09_source_collector import SourceCollectorMicroAgent
from monitoring_agent.ma10_data_normalization import DataNormalizationMicroAgent
from monitoring_agent.ma11_service_health import ServiceHealthMicroAgent
from monitoring_agent.ma12_log_trace_analyzer import LogTraceAnalyzerMicroAgent
from monitoring_agent.ma13_anomaly_detection import AnomalyDetectionMicroAgent
from monitoring_agent.ma14_event_detection import EventDetectionMicroAgent
from monitoring_agent.ma15_feedback_analyzer import FeedbackAnalyzerMicroAgent
from knowledge_base_agent.ma05_log_retrieval import LogKnowledgeRetrievalMicroAgent


class MonitoringMicroagentTests(unittest.TestCase):
    def test_normalization_maps_aliases_and_discards_placeholder_ids(self):
        normalized = DataNormalizationMicroAgent().normalize({
            "service": {"name": "orders"},
            "severity": "WARN",
            "timestamp": "2026-01-01T00:00:00Z",
            "trace.id": "-",
            "event.duration": 2_500_000,
        })

        self.assertEqual(normalized["service.name"], "orders")
        self.assertEqual(normalized["log.level"], "warning")
        self.assertEqual(normalized["monitoring.latency_ms"], 2.5)
        self.assertNotIn("trace.id", normalized)
        self.assertTrue(DataNormalizationMicroAgent().normalize({})["monitoring.quality_warnings"])

    def test_collector_applies_rate_limits_and_queue_capacity(self):
        collector = SourceCollectorMicroAgent(("orders",), queue_limit=1)
        collector.register("webhook", "http_push", rate_limit_per_minute=10)

        self.assertTrue(collector.collect({"message": "first"}, "webhook"))
        self.assertFalse(collector.collect({"message": "second"}, "webhook"))
        connector = next(item for item in collector.status()["connectors"] if item["connector_id"] == "webhook")
        self.assertEqual(connector["rejected"], 1)
        self.assertEqual(connector["health"], "degraded")

    def test_health_calculates_rolling_availability_and_latency(self):
        health = ServiceHealthMicroAgent(availability_slo=0.8, latency_slo_ms=1000)
        now = datetime.now(timezone.utc).isoformat()
        for index in range(5):
            failed = index == 4
            health.observe({
                "@timestamp": now,
                "service.name": "orders",
                "event.outcome": "failure" if failed else "success",
                "http.response.status_code": 500 if failed else 200,
                "monitoring.latency_ms": 2000 if failed else 100,
            })

        result = health.service_status()["services"]["orders"]
        self.assertEqual(result["availability"], 0.8)
        self.assertEqual(result["latency_p95_ms"], 2000)
        self.assertEqual(result["health"], "degraded")
        self.assertEqual(result["throughput_unit"], "normalized_events_per_minute")

    def test_anomaly_detection_emits_threshold_signals(self):
        signals = AnomalyDetectionMicroAgent().analyze({
            "service.name": "platform",
            "memory_percent": 95,
            "messaging.consumer.lag": 200,
        })

        self.assertEqual({signal["metric"] for signal in signals}, {"memory_percent", "kafka_lag"})

    def test_trace_retention_is_bounded_and_info_errors_are_not_failures(self):
        analyzer = LogTraceAnalyzerMicroAgent(max_traces=2)
        for trace_id in ("trace-a", "trace-b", "trace-c"):
            analyzer.analyze({
                "service.name": "orders",
                "trace.id": trace_id,
                "log.level": "info",
                "event.outcome": "success",
                "message": "No error observed",
            })

        self.assertEqual(analyzer.status()["correlated_traces"], 2)
        self.assertEqual(analyzer.trace("trace-a")["event_count"], 0)
        self.assertIsNone(analyzer.analyze({
            "service.name": "orders",
            "log.level": "info",
            "event.outcome": "success",
            "message": "No error observed",
        }))

    def test_event_detection_deduplicates_without_confirming_cause(self):
        detector = EventDetectionMicroAgent()
        event = {
            "@timestamp": "2026-01-01T00:00:00Z",
            "service.name": "orders",
            "message": "Connection timed out",
            "event.id": "event-1",
        }
        signal = {"type": "log_failure", "service": "orders", "signature": "same"}

        alerts = detector.detect(event, [signal])
        self.assertEqual(len(alerts), 1)
        self.assertFalse(alerts[0]["cause_confirmed"])
        self.assertEqual(detector.detect(event, [signal]), [])
        self.assertEqual(detector.status()["suppressed_duplicates"], 1)

    def test_feedback_analysis_redacts_pii_and_uses_supplied_rating(self):
        analyzer = FeedbackAnalyzerMicroAgent(Path(tempfile.gettempdir()) / "missing_reviews.jsonl")
        result = analyzer.analyze_text(
            "Comfortable engine; contact me at rider@example.com or 9876543210.",
            rating=5,
        )

        self.assertEqual(result["sentiment"], "positive")
        self.assertNotIn("rider@example.com", result["text"])
        self.assertNotIn("9876543210", result["text"])
        self.assertFalse(result["synthetic"])
        self.assertTrue(analyzer.analyze_text("Synthetic review example", synthetic=True)["synthetic"])

    def test_local_log_retrieval_applies_exact_and_trace_filters(self):
        class LocalOnlyConnector:
            configured = False

        retrieval = LogKnowledgeRetrievalMicroAgent(LocalOnlyConnector())
        records = [
            {
                "@timestamp": "2026-01-01T00:00:00Z",
                "service.name": "postgresql",
                "host.name": "db-1",
                "trace.id": "trace-1",
                "error.type": "TimeoutError",
                "log.level": "error",
                "message": "Connection timed out",
            },
            {
                "@timestamp": "2026-01-01T00:00:01Z",
                "service.name": "redis",
                "trace.id": "trace-2",
                "log.level": "error",
                "message": "Connection timed out",
            },
        ]
        result = asyncio.run(retrieval.search(
            "Connection timed out",
            records,
            service="postgresql",
            host="db-1",
            trace_id="trace-1",
            error_type="TimeoutError",
            severity="ERROR",
            exact=True,
        ))

        self.assertEqual(result["source"], "local_buffer")
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(result["items"][0]["trace.id"], "trace-1")


if __name__ == "__main__":
    unittest.main()
