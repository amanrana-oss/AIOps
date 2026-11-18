"""AG-27: create and track a local simulated incident ticket."""

from __future__ import annotations

from typing import Any

from .ma28_ticket_enrichment import TicketEnrichmentRouter
from .ma29_ticket_lifecycle import TicketLifecycleFeedbackSync


class TicketingAgent:
    """Build a self-contained ticket record without an external ITSM connector."""

    def __init__(self) -> None:
        self.enrichment = TicketEnrichmentRouter()
        self.lifecycle = TicketLifecycleFeedbackSync()

    def create_draft(self, incident: dict[str, Any], rca: dict[str, Any]) -> dict[str, Any]:
        return {
            "agent": "AG-27 Ticketing Agent",
            "microagents": ["MA-28 ticket enrichment and routing", "MA-29 ticket lifecycle and feedback sync"],
            **self.enrichment.create(incident, rca),
        }

    def update_status(self, ticket: dict[str, Any], status: str) -> dict[str, Any]:
        return self.lifecycle.update(ticket, status)