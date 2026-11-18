"""MA-22: validate hypothesis evidence references against retained incident data."""

from __future__ import annotations

from typing import Any


class HypothesisEvidenceValidator:
    def validate(
        self, incident: dict[str, Any], hypotheses: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        evidence = incident.get("evidence", {})
        known_ids = {
            str(event["event.id"])
            for event in evidence.get("trace_events", [])
            if event.get("event.id")
        }
        timeline = evidence.get("correlation", {}).get("microagents", {}).get(
            "MA-17_temporal_change_correlator", {}
        ).get("timeline", [])
        known_ids.update(str(event["event_id"]) for event in timeline if event.get("event_id"))
        customer = incident.get("alert", {}).get("customer_feedback")
        if customer:
            known_ids.add(str(customer["feedback_id"]))
            known_ids.update(str(item["id"]) for item in evidence.get("sops", {}).get("evidence", []) if item.get("id"))
        validated = []
        for hypothesis in hypotheses:
            references = hypothesis.get("evidence_refs", [])
            valid = [reference for reference in references if reference in known_ids]
            validated.append({
                **hypothesis,
                "valid_evidence_refs": valid,
                "invalid_evidence_refs": [reference for reference in references if reference not in known_ids],
                "evidence_coverage": round(len(valid) / len(references), 2) if references else 0.0,
                "cause_confirmed": False,
            })
        return validated