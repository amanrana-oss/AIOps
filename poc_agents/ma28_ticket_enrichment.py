"""MA-28: enrich and route an incident into a local simulated ticket."""

from __future__ import annotations

from typing import Any


class TicketEnrichmentRouter:
    def create(self, incident: dict[str, Any], rca: dict[str, Any]) -> dict[str, Any]:
        hypothesis = rca.get("hypotheses", [{}])[0]
        number = incident["incident_id"].replace("INC-", "")[-8:]
        return {
            "ticket_id": f"POC-{number}",
            "provider": "local_simulation",
            "status": "open",
            "priority": "P1" if incident.get("severity") == "high" else "P2",
            "title": f"[{incident.get('severity', 'medium').upper()}] {incident.get('service', 'service')} incident",
            "description": f"Alert {incident.get('source_alert_id')} requires review. Candidate: {hypothesis.get('title', 'unknown')}. Cause is unconfirmed.",
            "assigned_group": "Platform Operations",
            "external_ticket_created": False,
        }