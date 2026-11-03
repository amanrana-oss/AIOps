"""MA-14: event normalization, duplicate suppression, and incident candidates."""

from __future__ import annotations

import hashlib
import threading
import time
from collections import deque
from typing import Any


class EventDetectionMicroAgent:
    """Create bounded alert candidates while suppressing repeated signatures."""

    def __init__(
        self,
        dedupe_seconds: int = 60,
        max_alerts: int = 2000,
        max_fingerprints: int = 10_000,
    ) -> None:
        self.dedupe_seconds = dedupe_seconds
        self._alerts: deque[dict[str, Any]] = deque(maxlen=max_alerts)
        self._fingerprints: dict[str, float] = {}
        self._max_fingerprints = max_fingerprints
        self._suppressed = 0
        self._lock = threading.Lock()

    def detect(self, event: dict[str, Any], signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        created: list[dict[str, Any]] = []
        for signal in signals:
            fingerprint = hashlib.sha256(
                f"{signal.get('type')}:{signal.get('service')}:{signal.get('metric')}:"
                f"{signal.get('signature') or signal.get('error_type') or signal.get('message')}".encode()
            ).hexdigest()[:20]
            now = time.time()
            with self._lock:
                self._expire_fingerprints(now)
                previous = self._fingerprints.get(fingerprint)
                if previous is not None and now - previous < self.dedupe_seconds:
                    self._suppressed += 1
                    continue
                self._fingerprints[fingerprint] = now
                alert = {
                    "alert_id": hashlib.sha256(f"{fingerprint}:{now}".encode()).hexdigest()[:16],
                    "fingerprint": fingerprint,
                    "first_seen": event.get("@timestamp"),
                    "detected_at": now,
                    "severity": self._severity(signal),
                    "service": signal.get("service", event.get("service.name", "unknown")),
                    "signal": signal,
                    "event_evidence": {
                        "event_id": event.get("event.id"),
                        "trace_id": event.get("trace.id"),
                        "request_id": event.get("request_id"),
                        "message": str(event.get("message", ""))[:300],
                    },
                    "state": "incident_candidate",
                    "cause_confirmed": False,
                }
                self._alerts.append(alert)
                created.append(alert)
        return created

    def _expire_fingerprints(self, now: float) -> None:
        expired = [
            fingerprint
            for fingerprint, detected_at in self._fingerprints.items()
            if now - detected_at >= self.dedupe_seconds
        ]
        for fingerprint in expired:
            self._fingerprints.pop(fingerprint, None)
        if len(self._fingerprints) > self._max_fingerprints:
            oldest = sorted(self._fingerprints.items(), key=lambda item: item[1])
            for fingerprint, _ in oldest[: len(self._fingerprints) - self._max_fingerprints]:
                self._fingerprints.pop(fingerprint, None)

    def _severity(self, signal: dict[str, Any]) -> str:
        if signal.get("type") == "log_failure":
            return "high"
        if signal.get("metric") in {"memory_percent", "cpu_percent", "disk_percent", "db_pool_percent"}:
            return "high"
        return "medium"

    def alerts(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._alerts)[-limit:][::-1]

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "active_candidates": len(self._alerts),
                "suppressed_duplicates": self._suppressed,
                "dedupe_seconds": self.dedupe_seconds,
                "retained_limit": self._alerts.maxlen,
            }
