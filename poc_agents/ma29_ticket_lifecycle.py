"""MA-29: validate local ticket lifecycle updates."""

from __future__ import annotations

from typing import Any


class TicketLifecycleFeedbackSync:
    STATUSES = {"open", "in_progress", "resolved", "closed"}

    def update(self, ticket: dict[str, Any], status: str) -> dict[str, Any]:
        if status not in self.STATUSES:
            raise ValueError(f"Ticket status must be one of: {', '.join(sorted(self.STATUSES))}")
        return {**ticket, "status": status, "external_ticket_created": False}