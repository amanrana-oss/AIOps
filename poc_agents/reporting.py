"""Incident output/reporting agent for concise operator handoff briefs."""

from __future__ import annotations

from typing import Any


class IncidentReportingAgent:
    """Render incident workflow state into a compact, exportable Markdown brief."""

    def build(self, incident: dict[str, Any]) -> dict[str, Any]:
        workflow = incident.get("poc", {})
        hypotheses = workflow.get("rca", {}).get("hypotheses", [])
        primary = hypotheses[0] if hypotheses else {}
        ticket = workflow.get("ticket") or {}
        resolution = workflow.get("resolution", {})
        action = (resolution.get("actions") or [{}])[0]
        refs = primary.get("evidence_refs", [])
        sop_items = incident.get("evidence", {}).get("sops", {}).get("evidence", [])
        sop_citations = [
            f"{item.get('citation', {}).get('source', item.get('metadata', {}).get('source_path', 'unknown source'))} "
            f"(chunk {item.get('citation', {}).get('chunk', item.get('metadata', {}).get('chunk_index', 'n/a'))}; "
            f"{'review required' if item.get('review_required') else 'reviewed'})"
            for item in sop_items
        ]
        text = "\n".join([
            f"# Incident brief: {incident.get('incident_id', 'unknown')}",
            "",
            f"- Status: {incident.get('status', 'unknown')}",
            f"- Severity: {incident.get('severity', 'unknown').upper()}",
            f"- Service: {incident.get('service', 'unknown')}",
            f"- Alert: {incident.get('source_alert_id', 'unknown')}",
            f"- Ticket: {ticket.get('ticket_id', 'not created')} ({ticket.get('status', 'n/a')})",
            "",
            "## Leading hypothesis (unconfirmed)",
            primary.get("title", "No specific hypothesis yet"),
            primary.get("rationale", "Evidence is still being collected."),
            "",
            "## Evidence references",
            *((f"- `{ref}`" for ref in refs) if refs else ["- No event IDs available in retained evidence."]),
            "",
            "## Retrieved SOP sources",
            *((f"- {citation}" for citation in sop_citations) if sop_citations else ["- No SOP citation was available."]),
            "",
            "## Proposed response",
            f"{action.get('title', 'No action proposed')} ({action.get('status', 'awaiting_analysis')})",
            "Human approval is required. POC execution is a dry run and changes no external systems.",
            "",
            "## Confidence and limitations",
            "Correlation and rule matches are candidates, not causal proof. Verify against source systems before acting.",
        ])
        return {
            "agent": "Incident Output and Reporting Agent",
            "incident_id": incident.get("incident_id"),
            "format": "markdown",
            "summary": f"{incident.get('severity', 'medium').upper()} · {incident.get('service', 'unknown')} · {primary.get('title', 'evidence review required')}",
            "markdown": text,
        }