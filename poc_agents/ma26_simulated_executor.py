"""MA-26: demonstrate executor state transitions without external side effects."""

from __future__ import annotations

from typing import Any


class SimulatedRemediationExecutor:
    def execute(self, plan: dict[str, Any], approved: bool) -> dict[str, Any]:
        action = plan["actions"][0]
        if not approved:
            return {**plan, "actions": [{**action, "status": "blocked_pending_approval"}]}
        return {
            **plan,
            "actions": [{**action, "status": "simulated_complete"}],
            "simulation_result": "Dry run completed. No infrastructure or external system was changed.",
            "external_systems_modified": False,
        }