"""MA-10: normalize event fields, timestamps, severities, and units."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class DataNormalizationMicroAgent:
    """Convert supported source variants into one monitoring event shape."""

    SEVERITIES = {
        "trace": "debug",
        "debug": "debug",
        "info": "info",
        "information": "info",
        "warn": "warning",
        "warning": "warning",
        "error": "error",
        "critical": "critical",
        "fatal": "critical",
    }

    def normalize(self, source: dict[str, Any], connector_id: str = "unknown") -> dict[str, Any]:
        event = dict(source)
        raw_timestamp = event.get("@timestamp") or event.get("timestamp") or event.get("time")
        timestamp, timestamp_valid = self._timestamp(raw_timestamp)
        service = event.get("service.name") or event.get("service")
        if isinstance(service, dict):
            service = service.get("name")
        service_name = str(service or event.get("service_name") or "unknown").strip()
        raw_level = event.get("log.level") or event.get("level") or event.get("severity") or "info"
        severity = self.SEVERITIES.get(str(raw_level).lower(), "unknown")
        message = event.get("message") or event.get("msg") or event.get("error.message") or ""
        latency_ms = self._latency_ms(event)
        status_code = self._number(event.get("http.response.status_code") or event.get("status_code"))
        outcome = str(event.get("event.outcome") or event.get("outcome") or "").lower()
        if not outcome:
            outcome = "failure" if severity in {"error", "critical"} or (status_code or 0) >= 500 else "success"
        quality_warnings = []
        if service_name == "unknown":
            quality_warnings.append("missing service identifier")
        if not timestamp_valid:
            quality_warnings.append("missing or invalid timestamp; receive time substituted")
        if severity == "unknown":
            quality_warnings.append(f"unrecognized severity: {raw_level}")
        if not str(message).strip():
            quality_warnings.append("missing message")
        normalized = {
            **event,
            "@timestamp": timestamp,
            "service.name": service_name,
            "service.environment": str(event.get("service.environment") or event.get("environment") or "unknown"),
            "log.level": severity,
            "message": str(message),
            "event.outcome": outcome,
            "monitoring.connector_id": connector_id,
            "monitoring.normalized": True,
            "monitoring.timestamp_valid": timestamp_valid,
            "monitoring.quality_warnings": quality_warnings,
        }
        if latency_ms is not None:
            normalized["monitoring.latency_ms"] = latency_ms
        if status_code is not None:
            normalized["http.response.status_code"] = status_code
        trace_id = self._identifier(event.get("trace.id") or event.get("trace_id"))
        request_id = self._identifier(event.get("request_id") or event.get("request.id"))
        if trace_id:
            normalized["trace.id"] = trace_id
        else:
            normalized.pop("trace.id", None)
        normalized.pop("trace_id", None)
        if request_id:
            normalized["request_id"] = request_id
        else:
            normalized.pop("request_id", None)
        normalized.pop("request.id", None)
        return normalized

    def _identifier(self, value: Any) -> str | None:
        if value is None:
            return None
        identifier = str(value).strip()
        if identifier.lower() in {"", "-", "none", "null", "unknown"} or set(identifier) == {"0"}:
            return None
        return identifier

    def _timestamp(self, value: Any) -> tuple[str, bool]:
        if value is not None:
            try:
                parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"), True
            except (TypeError, ValueError, OverflowError):
                pass
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"), False

    def _latency_ms(self, event: dict[str, Any]) -> float | None:
        latency = self._number(
            event.get("latency_ms")
            or event.get("duration_ms")
            or event.get("event.duration")
            or event.get("duration")
        )
        if latency is None:
            return None
        if event.get("event.duration") is not None and event.get("latency_ms") is None:
            return latency / 1_000_000
        return latency

    def _number(self, value: Any) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if number >= 0 else None
