"""AG-01 alert grouping and prioritization rules for the local POC."""

from __future__ import annotations

from typing import Any


class AlertRoutingRules:
    """Coalesce related signal candidates into one operator-facing incident."""

    SELF_MONITORING_SERVICE = "api"
    ROUTINE_SIGNAL_TYPES = {"threshold_anomaly", "baseline_anomaly"}
    SEVERITY_ORDER = {"high": 3, "medium": 2, "low": 1}
    SIGNAL_ORDER = {"log_failure": 3, "threshold_anomaly": 2, "baseline_anomaly": 1}

    def group(self, alerts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        groups: dict[str, list[dict[str, Any]]] = {}
        for alert in alerts:
            alert_id = str(alert.get("alert_id", ""))
            if not alert_id:
                continue
            trace_id = str(alert.get("event_evidence", {}).get("trace_id") or "")
            group_id = f"trace:{trace_id}" if trace_id else f"alert:{alert_id}"
            groups.setdefault(group_id, []).append(alert)
        return groups

    def actionable_signals(self, alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            alert for alert in alerts
            if not (
                alert.get("service") == self.SELF_MONITORING_SERVICE
                and alert.get("signal", {}).get("type") in self.ROUTINE_SIGNAL_TYPES
            )
        ]

    def primary_alert(self, alerts: list[dict[str, Any]]) -> dict[str, Any]:
        if not alerts:
            raise ValueError("Cannot select a primary alert from an empty group")
        return max(
            alerts,
            key=lambda alert: (
                self.SEVERITY_ORDER.get(alert.get("severity"), 0),
                self.SIGNAL_ORDER.get(alert.get("signal", {}).get("type"), 0),
            ),
        )

    def attach_related_signals(
        self,
        primary: dict[str, Any],
        all_alerts: list[dict[str, Any]],
        actionable_alerts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            **primary,
            "related_alerts": [
                {
                    "alert_id": alert.get("alert_id"),
                    "service": alert.get("service"),
                    "severity": alert.get("severity"),
                    "signal": alert.get("signal", {}),
                    "actionable": alert in actionable_alerts,
                }
                for alert in all_alerts
            ],
            "related_actionable_signal_count": len(actionable_alerts),
        }