"""MA-25: require an explicit operator approval before the dry-run executor."""

from __future__ import annotations

from typing import Any


class SafetyRiskApprovalGate:
    def gate(self, proposed_action: dict[str, Any]) -> dict[str, Any]:
        return {
            **proposed_action,
            "risk": "medium",
            "requires_approval": True,
            "status": "awaiting_approval",
        }