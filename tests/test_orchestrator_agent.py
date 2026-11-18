import tempfile
import unittest
from pathlib import Path

from orchestrator_agent import AlertRoutingRules, OrchestratorAgent


class OrchestratorAgentTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.agent = OrchestratorAgent(Path(self.temp_dir.name) / "incidents.sqlite3")
        self.alert = {
            "alert_id": "alert-123",
            "severity": "medium",
            "service": "orders",
            "signal": {"type": "log_failure", "message": "Request timed out"},
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_investigation_is_persisted_idempotently_with_evidence(self):
        first = self.agent.create_investigation(self.alert, [{"trace.id": "trace-a"}], {"items": []}, {"evidence": []})
        duplicate = self.agent.create_investigation(self.alert, [], {}, {})

        self.assertEqual(first["incident_id"], duplicate["incident_id"])
        self.assertEqual(first["status"], "pending_review")
        self.assertFalse(first["cause_confirmed"])
        self.assertEqual(first["evidence"]["trace_events"][0]["trace.id"], "trace-a")
        self.assertEqual(self.agent.get_incident(first["incident_id"])["incident_id"], first["incident_id"])

    def test_recent_matching_signature_merges_traces_and_impact(self):
        first_alert = {
            **self.alert,
            "alert_id": "alert-first",
            "fingerprint": "stable-timeout-signature",
            "detected_at": 1000,
            "first_seen": "2026-11-09T10:00:00+00:00",
            "event_evidence": {"trace_id": "trace-a"},
            "related_alerts": [{"alert_id": "alert-first", "service": "orders", "severity": "medium", "actionable": True}],
        }
        first = self.agent.create_investigation(
            first_alert,
            [{"event.id": "event-a", "trace.id": "trace-a", "service.name": "orders"}],
            {"items": []},
            {"evidence": []},
        )
        repeat_alert = {
            **first_alert,
            "alert_id": "alert-repeat",
            "detected_at": 1015,
            "first_seen": "2026-11-09T10:00:15+00:00",
            "event_evidence": {"trace_id": "trace-b"},
            "related_alerts": [{"alert_id": "alert-repeat", "service": "orders", "severity": "high", "actionable": True}],
        }

        merged = self.agent.merge_recent_investigation(
            repeat_alert,
            [{"event.id": "event-b", "trace.id": "trace-b", "service.name": "payments"}],
            {"items": []},
            {"evidence": []},
        )

        self.assertEqual(merged["incident_id"], first["incident_id"])
        self.assertEqual(merged["severity"], "high")
        self.assertEqual(merged["deduplication"]["occurrences"], 2)
        self.assertEqual(merged["impact"]["signal_count"], 2)
        self.assertEqual(merged["impact"]["trace_count"], 2)
        self.assertEqual(merged["impact"]["affected_services"], ["orders", "payments"])
        self.assertEqual({event["event.id"] for event in merged["evidence"]["trace_events"]}, {"event-a", "event-b"})

    def test_different_signatures_and_closed_incidents_are_not_merged(self):
        first_alert = {
            **self.alert,
            "alert_id": "alert-first",
            "fingerprint": "timeout-a",
            "detected_at": 2000,
        }
        first = self.agent.create_investigation(first_alert, [], {}, {})
        unrelated = self.agent.merge_recent_investigation(
            {**first_alert, "alert_id": "alert-other", "fingerprint": "timeout-b", "detected_at": 2010},
            [], {}, {},
        )
        self.assertIsNone(unrelated)
        outside_window = self.agent.merge_recent_investigation(
            {**first_alert, "alert_id": "alert-later", "detected_at": 2601},
            [], {}, {},
        )
        self.assertIsNone(outside_window)

        self.agent.review_incident(first["incident_id"], "closed")
        after_close = self.agent.merge_recent_investigation(
            {**first_alert, "alert_id": "alert-after-close", "detected_at": 2020},
            [], {}, {},
        )
        self.assertIsNone(after_close)

    def test_review_transitions_and_closed_incidents_cannot_reopen(self):
        incident = self.agent.create_investigation(self.alert, [], {}, {})
        acknowledged = self.agent.review_incident(incident["incident_id"], "acknowledged", "Reviewed")
        closed = self.agent.review_incident(incident["incident_id"], "closed")

        self.assertEqual(acknowledged["review_note"], "Reviewed")
        self.assertEqual(closed["status"], "closed")
        with self.assertRaises(ValueError):
            self.agent.review_incident(incident["incident_id"], "acknowledged")

    def test_poc_workflow_is_persisted_with_incident(self):
        incident = self.agent.create_investigation(self.alert, [], {}, {})
        workflow = {"ticket": {"ticket_id": "POC-123", "status": "open"}}

        saved = self.agent.save_poc_workflow(incident["incident_id"], workflow)
        loaded = self.agent.get_incident(incident["incident_id"])

        self.assertEqual(saved["poc"], workflow)
        self.assertEqual(loaded["poc"]["ticket"]["ticket_id"], "POC-123")

    def test_alert_routing_groups_trace_and_filters_internal_latency_noise(self):
        routing = AlertRoutingRules()
        alerts = [
            {
                "alert_id": "api-latency",
                "service": "api",
                "severity": "medium",
                "event_evidence": {"trace_id": "trace-a"},
                "signal": {"type": "baseline_anomaly"},
            },
            {
                "alert_id": "gateway-failure",
                "service": "api-gateway",
                "severity": "high",
                "event_evidence": {"trace_id": "trace-a"},
                "signal": {"type": "log_failure"},
            },
            {
                "alert_id": "db-failure",
                "service": "postgresql",
                "severity": "high",
                "event_evidence": {"trace_id": "trace-a"},
                "signal": {"type": "log_failure"},
            },
        ]

        groups = routing.group(alerts)
        actionable = routing.actionable_signals(groups["trace:trace-a"])
        primary = routing.primary_alert(actionable)
        incident_alert = routing.attach_related_signals(primary, alerts, actionable)

        self.assertEqual(len(groups), 1)
        self.assertEqual(len(actionable), 2)
        self.assertEqual(incident_alert["related_actionable_signal_count"], 2)
        self.assertEqual(len(incident_alert["related_alerts"]), 3)
        self.assertEqual(primary["service"], "api-gateway")


if __name__ == "__main__":
    unittest.main()