import unittest

from poc_agents import IncidentReportingAgent, ResolutionAgent, RootCauseAgent, TicketingAgent


class POCAgentTests(unittest.TestCase):
    def setUp(self):
        self.incident = {
            "incident_id": "INC-12345678",
            "source_alert_id": "alert-1",
            "service": "vehicle-telemetry-api",
            "severity": "high",
            "status": "pending_review",
            "alert": {"signal": {"type": "threshold_anomaly", "metric": "latency_ms"}},
            "evidence": {
                "trace_events": [{"event.id": "event-1"}],
                "sops": {"evidence": [{
                    "citation": {"source": "triage.md", "chunk": 0},
                    "review_required": True,
                }]},
                "correlation": {
                    "microagents": {
                        "MA-19_failure_pattern_scorer": [{"pattern": "high_latency"}],
                        "MA-17_temporal_change_correlator": {"nearby_changes": []},
                    }
                },
            },
        }

    def test_rca_and_report_keep_causality_unconfirmed(self):
        rca = RootCauseAgent().analyze(self.incident)
        self.assertEqual(rca["hypotheses"][0]["id"], "high_latency")
        self.assertFalse(rca["cause_confirmed"])
        plan = ResolutionAgent().plan(self.incident, rca)
        workflow = {
            "rca": rca,
            "resolution": plan,
            "ticket": TicketingAgent().create_draft(self.incident, rca),
        }
        report = IncidentReportingAgent().build({**self.incident, "poc": workflow})
        self.assertIn("unconfirmed", report["markdown"])
        self.assertIn("event-1", report["markdown"])
        self.assertIn("triage.md", report["markdown"])
        self.assertIn("review required", report["markdown"])

    def test_resolution_requires_explicit_approval_and_never_touches_external_systems(self):
        agent = ResolutionAgent()
        plan = agent.plan(self.incident, RootCauseAgent().analyze(self.incident))

        blocked = agent.execute_simulation(plan, approved=False)
        approved = agent.execute_simulation(plan, approved=True)

        self.assertEqual(blocked["actions"][0]["status"], "blocked_pending_approval")
        self.assertEqual(approved["actions"][0]["status"], "simulated_complete")
        self.assertFalse(approved["external_systems_modified"])

    def test_unmatched_log_failure_uses_plain_language_hypothesis(self):
        incident = {
            **self.incident,
            "alert": {"signal": {"type": "log_failure"}},
            "evidence": {"trace_events": [], "correlation": {"microagents": {}}},
        }

        result = RootCauseAgent().analyze(incident)

        self.assertEqual(result["hypotheses"][0]["title"], "Service request failure")
        self.assertNotIn("log_failure", result["hypotheses"][0]["title"])

    def test_postgres_timeout_uses_observed_evidence_and_specific_response(self):
        incident = {
            **self.incident,
            "service": "postgresql",
            "alert": {"signal": {"type": "log_failure", "message": "Database query timed out"}},
            "evidence": {
                "trace_events": [{
                    "event.id": "query-timeout-1",
                    "service.name": "postgresql",
                    "message": "Database query timed out",
                    "error.type": "QueryTimeout",
                    "event.outcome": "failure",
                    "http.response.status_code": 503,
                }],
                "correlation": {"microagents": {
                    "MA-19_failure_pattern_scorer": [{"pattern": "application_exception"}],
                    "MA-17_temporal_change_correlator": {"nearby_changes": []},
                }},
            },
        }

        rca = RootCauseAgent().analyze(incident)
        action = ResolutionAgent().plan(incident, rca)["actions"][0]

        self.assertEqual(rca["hypotheses"][0]["title"], "PostgreSQL query timeout")
        self.assertIn("Database query timed out", rca["hypotheses"][0]["supporting_evidence"][0])
        self.assertFalse(rca["hypotheses"][0]["cause_confirmed"])
        self.assertEqual(action["title"], "Inspect PostgreSQL query timeout")
        self.assertIn("active connections", action["description"])
        self.assertIn("QueryTimeout", action["decision_basis"])
        self.assertIn("query succeeds", action["verification"])

    def test_postgres_timeout_fallback_matches_handoff_summary(self):
        incident = {
            **self.incident,
            "service": "postgresql",
            "alert": {"signal": {"type": "log_failure", "message": "Database query timed out"}},
            "evidence": {"trace_events": [{
                "event.id": "query-timeout-2",
                "service.name": "postgresql",
                "message": "Database query timed out",
                "error.type": "QueryTimeout",
                "event.outcome": "failure",
                "http.response.status_code": 503.0,
            }]},
        }

        rca = RootCauseAgent().analyze(incident)
        resolution = ResolutionAgent().plan(incident, rca)
        workflow = {"rca": rca, "resolution": resolution}
        report = IncidentReportingAgent().build({**incident, "poc": workflow})

        self.assertEqual(rca["hypotheses"][0]["id"], "database_query_timeout")
        self.assertEqual(report["summary"], "HIGH · postgresql · PostgreSQL query timeout")
        self.assertIn("HTTP 503", rca["hypotheses"][0]["supporting_evidence"][0])


if __name__ == "__main__":
    unittest.main()