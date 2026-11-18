"""MA-21: generate hypotheses from explicit alert and correlation rules."""

from __future__ import annotations

from typing import Any


class RootCauseHypothesisGenerator:
    SERVICE_LABELS = {
        "api-gateway": "API gateway",
        "authentication-service": "Authentication service",
        "vehicle-telemetry-api": "Vehicle telemetry API",
        "order-management-service": "Order management service",
        "postgresql": "PostgreSQL",
    }
    ERROR_LABELS = {
        "QueryTimeout": "query timeout",
        "TimeoutError": "request timeout",
        "UpstreamError": "upstream error",
        "ZeroDivisionError": "application exception",
    }

    def _event_summary(self, event: dict[str, Any], default_service: str) -> str:
        service = str(event.get("service.name") or default_service or "service")
        service = self.SERVICE_LABELS.get(service, service.replace("-", " ").title())
        parts = [service]
        message = str(event.get("message", "")).strip()
        if message:
            parts.append(message[:180])
        error_type = str(event.get("error.type") or "")
        if error_type and error_type not in message:
            parts.append(error_type)
        status = event.get("http.response.status_code")
        if status:
            status_label = f"{status:g}" if isinstance(status, (int, float)) else str(status)
            parts.append(f"HTTP {status_label}")
        latency = event.get("latency_ms")
        if isinstance(latency, (int, float)):
            parts.append(f"{latency:g} ms")
        return " · ".join(parts)

    @staticmethod
    def _change_summary(change: dict[str, Any]) -> str:
        service = str(change.get("service") or "deployment pipeline")
        message = str(change.get("message") or change.get("action") or "Operational change")
        return f"Related change · {service} · {message[:150]}"

    def _supporting_evidence(self, incident: dict[str, Any]) -> list[str]:
        evidence = incident.get("evidence", {})
        service = str(incident.get("service") or "service")
        events = evidence.get("trace_events", [])
        summaries = [self._event_summary(event, service) for event in events[:4]]
        if not summaries:
            signal_message = str(incident.get("alert", {}).get("signal", {}).get("message", "")).strip()
            if signal_message:
                summaries.append(signal_message[:180])
        changes = evidence.get("correlation", {}).get("microagents", {}).get(
            "MA-17_temporal_change_correlator", {}
        ).get("nearby_changes", [])
        summaries.extend(self._change_summary(change) for change in changes[:4 - len(summaries)])
        return summaries[:4]

    def generate(self, incident: dict[str, Any]) -> list[dict[str, Any]]:
        alert = incident.get("alert", {})
        signal = alert.get("signal", {})
        evidence = incident.get("evidence", {})
        customer = alert.get("customer_feedback")
        if customer:
            passages = evidence.get("sops", {}).get("evidence", [])
            references = [customer["feedback_id"]] + [str(item["id"]) for item in passages if item.get("id")]
            return [{
                "id": "customer_report_requires_validation", "title": "Customer-reported symptom requires validation",
                "rationale": "The complaint is evidence of a reported experience, not a confirmed technical defect. Historical tracker matches are investigation leads, not approved fixes. Verify product/version applicability with the responsible team.",
                "evidence_refs": references, "supporting_evidence": [str(customer.get("content", ""))[:300]],
                "cause_confirmed": False,
            }]
        microagents = evidence.get("correlation", {}).get("microagents", {})
        patterns = microagents.get("MA-19_failure_pattern_scorer", [])
        changes = microagents.get("MA-17_temporal_change_correlator", {}).get("nearby_changes", [])
        matched_rules = {item.get("pattern") for item in patterns if isinstance(item, dict)}
        references = [str(event["event.id"]) for event in evidence.get("trace_events", []) if event.get("event.id")]
        references.extend(str(change["event_id"]) for change in changes if change.get("event_id"))
        supporting_evidence = self._supporting_evidence(incident)
        trace_events = evidence.get("trace_events", [])
        primary_event = next(
            (event for event in trace_events if event.get("service.name") == incident.get("service")),
            trace_events[0] if trace_events else {},
        )
        error_type = str(primary_event.get("error.type") or "")
        service = str(incident.get("service") or "service")
        service_label = self.SERVICE_LABELS.get(service, service.replace("-", " ").title())
        error_label = self.ERROR_LABELS.get(error_type, error_type.replace("Error", " error").replace("Timeout", " timeout"))
        candidates = []
        for rule, title, rationale in (
            ("application_exception", "Application exception", "Failure-pattern rules found exception and failure evidence."),
            ("database_pool_pressure", "Database pool pressure", "Failure-pattern rules found database pressure alongside a failed event."),
            ("messaging_lag", "Messaging lag", "Failure-pattern rules found elevated lag or retry evidence."),
            ("deployment_with_symptoms", "Recent deployment with symptoms", "A deployment change is close in time to observed failure or latency evidence."),
            ("high_latency", "Elevated service latency", "At least one correlated event exceeded the 1,000 ms latency rule."),
        ):
            if rule in matched_rules:
                if rule == "application_exception" and error_label:
                    title = f"{service_label} {error_label}"
                    rationale = (
                        f"The incident trace records {error_type or error_label} on {service_label}. "
                        "This is a candidate explanation, not a confirmed root cause."
                    )
                candidates.append({
                    "id": rule,
                    "title": title,
                    "rationale": rationale,
                    "evidence_refs": references[:8],
                    "supporting_evidence": supporting_evidence,
                    "cause_confirmed": False,
                })
        if not candidates:
            if service == "postgresql" and "timeout" in f"{error_type} {primary_event.get('message', '')}".casefold():
                candidates.append({
                    "id": "database_query_timeout",
                    "title": "PostgreSQL query timeout",
                    "rationale": "The incident trace records a PostgreSQL query timeout. This identifies the observed failure, not its underlying cause.",
                    "evidence_refs": references[:8],
                    "supporting_evidence": supporting_evidence,
                    "cause_confirmed": False,
                })
            elif error_type:
                candidates.append({
                    "id": "observed_failure",
                    "title": f"{service_label}: {error_type}",
                    "rationale": f"The incident trace records {error_type} on {service_label}. This is an observed failure, not a confirmed root cause.",
                    "evidence_refs": references[:8],
                    "supporting_evidence": supporting_evidence,
                    "cause_confirmed": False,
                })
        if not candidates:
            signal_type = str(signal.get("type", ""))
            signal_label = {
                "log_failure": "Service request failure",
                "threshold_anomaly": f"Elevated {signal.get('metric') or 'service metric'}",
                "baseline_anomaly": f"Unexpected {signal.get('metric') or 'service metric'} pattern",
            }.get(signal_type, "Monitoring alert")
            candidates.append({
                "id": signal_type or "alert_signal",
                "title": signal_label,
                "rationale": "Monitoring detected an issue, but the available evidence does not yet point to a specific failure pattern.",
                "evidence_refs": references[:8],
                "supporting_evidence": supporting_evidence,
                "cause_confirmed": False,
            })
        return candidates