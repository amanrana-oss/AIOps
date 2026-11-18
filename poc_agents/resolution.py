"""AG-23: propose bounded runbook actions behind explicit human approval."""

from __future__ import annotations

from typing import Any

from .ma24_runbook_planner import RunbookFixPlanner
from .ma25_safety_gate import SafetyRiskApprovalGate
from .ma26_simulated_executor import SimulatedRemediationExecutor


class ResolutionAgent:
    """Plan and simulate recovery actions; never call external infrastructure."""

    def __init__(self) -> None:
        self.planner = RunbookFixPlanner()
        self.approval_gate = SafetyRiskApprovalGate()
        self.executor = SimulatedRemediationExecutor()

    def plan(self, incident: dict[str, Any], rca: dict[str, Any]) -> dict[str, Any]:
        proposed_action = self.planner.plan(incident, rca)
        action = self.approval_gate.gate(proposed_action)
        return {
            "agent": "AG-23 Resolution Agent",
            "microagents": ["MA-24 runbook and fix planner", "MA-25 safety and approval gate", "MA-26 simulated executor"],
            "actions": [action],
            "approval_required": True,
            "execution_mode": proposed_action.get("mode", "simulation_only"),
            "external_systems_modified": False,
        }

    def execute_simulation(self, plan: dict[str, Any], approved: bool) -> dict[str, Any]:
        return self.executor.execute(plan, approved)