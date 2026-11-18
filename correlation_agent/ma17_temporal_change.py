"""MA-17: order incident evidence and identify nearby operational changes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


class TemporalChangeCorrelator:
    """Build a time-ordered evidence timeline and surface nearby changes."""

    CHANGE_ACTIONS = {
        "deployment.started",
        "deployment.completed",
        "deployment.rollback",
        "release.started",
        "release.completed",
        "release.rollback",
    }

    def __init__(self, change_window_seconds: int = 1800, max_events: int = 500) -> None:
        self.change_window_seconds = change_window_seconds
        self.max_events = max_events

    def analyze(
        self, alert: dict[str, Any], events: list[dict[str, Any]]
    ) -> dict[str, Any]:
        alert_time = _parse_timestamp(alert.get("first_seen"))
        if alert_time is None:
            alert_time = _parse_timestamp(alert.get("signal", {}).get("observed_at"))
        timeline = []
        for event in events[: self.max_events]:
            timestamp = _parse_timestamp(event.get("@timestamp"))
            if timestamp is None:
                continue
            action = str(event.get("event.action", ""))
            item = {
                "timestamp": timestamp.isoformat(),
                "event_id": event.get("event.id"),
                "service": event.get("service.name", "unknown"),
                "action": action or None,
                "severity": event.get("log.level"),
                "message": str(event.get("message", ""))[:300],
            }
            if alert_time:
                item["seconds_from_alert"] = round((timestamp - alert_time).total_seconds(), 3)
            timeline.append((timestamp, item))
        timeline.sort(key=lambda entry: entry[0])
        changes = [
            item for timestamp, item in timeline
            if item["action"] in self.CHANGE_ACTIONS
            and alert_time is not None
            and 0 <= (alert_time - timestamp).total_seconds() <= self.change_window_seconds
        ]
        return {
            "window_seconds": self.change_window_seconds,
            "timeline": [item for _, item in timeline],
            "nearby_changes": changes,
            "alert_timestamp": alert_time.isoformat() if alert_time else None,
        }