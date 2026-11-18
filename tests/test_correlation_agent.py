import unittest

from correlation_agent import CorrelationAgent


class CorrelationAgentTests(unittest.TestCase):
    def test_correlates_change_dependency_and_database_pattern_as_candidates(self):
        alert = {
            "first_seen": "2026-11-03T10:00:00+00:00",
            "service": "vehicle-telemetry-api",
            "signal": {"observed_at": 1793700000},
        }
        events = [
            {
                "@timestamp": "2026-11-03T09:55:00+00:00",
                "event.id": "deploy-1",
                "event.action": "deployment.completed",
                "service.name": "deployment-pipeline",
                "message": "Deployment completed",
            },
            {
                "@timestamp": "2026-11-03T09:56:00+00:00",
                "event.id": "status-1",
                "event.action": "deployment.status",
                "service.name": "deployment-pipeline",
                "message": "Last successful release remains active",
            },
            {
                "@timestamp": "2026-11-03T09:59:30+00:00",
                "event.id": "db-1",
                "event.action": "database.connection",
                "service.name": "postgresql",
                "log.level": "error",
                "event.outcome": "failure",
                "db.pool.active": 18,
                "db.pool.max": 20,
                "message": "Connection pool acquisition timed out",
            },
            {
                "@timestamp": "2026-11-03T09:59:45+00:00",
                "event.id": "telemetry-1",
                "service.name": "vehicle-telemetry-api",
                "event.outcome": "failure",
                "http.response.status_code": 503,
                "message": "Telemetry request failed",
            },
        ]

        result = CorrelationAgent().analyze(alert, events)
        agents = result["microagents"]

        self.assertEqual(len(agents["MA-17_temporal_change_correlator"]["nearby_changes"]), 1)
        self.assertEqual(
            agents["MA-17_temporal_change_correlator"]["nearby_changes"][0]["event_id"],
            "deploy-1",
        )
        self.assertIn(
            {"upstream": "vehicle-telemetry-api", "downstream": "postgresql"},
            agents["MA-18_dependency_analyzer"]["observed_dependency_links"],
        )
        patterns = {item["pattern"]: item for item in agents["MA-19_failure_pattern_scorer"]}
        self.assertIn("database_pool_pressure", patterns)
        self.assertEqual(patterns["database_pool_pressure"]["rule_score"], 1.0)
        self.assertFalse(result["cause_confirmed"])

    def test_empty_evidence_returns_no_candidates_without_confirming_cause(self):
        result = CorrelationAgent().analyze({"service": "unknown"}, [])

        self.assertEqual(result["evidence_event_count"], 0)
        self.assertEqual(result["microagents"]["MA-19_failure_pattern_scorer"], [])
        self.assertFalse(result["cause_confirmed"])


if __name__ == "__main__":
    unittest.main()