"""MA-09: reusable local/webhook source registry and bounded event queue."""

from __future__ import annotations

import asyncio
import queue
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any


class SourceCollectorMicroAgent:
    """Accept events from registered connectors without per-service agents."""

    def __init__(
        self,
        services: tuple[str, ...],
        queue_limit: int = 20_000,
        max_connectors: int = 100,
    ) -> None:
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=queue_limit)
        self._max_connectors = max_connectors
        self._lock = threading.Lock()
        self._connectors: dict[str, dict[str, Any]] = {
            "simulator": {
                "connector_id": "simulator",
                "kind": "in_process",
                "enabled": True,
                "services": list(services),
                "accepted": 0,
                "rejected": 0,
                "rate_limit_per_minute": None,
                "window_started_at": time.monotonic(),
                "window_accepted": 0,
                "last_error": None,
            },
            "http_webhook": {
                "connector_id": "http_webhook",
                "kind": "http_push",
                "enabled": True,
                "services": [],
                "accepted": 0,
                "rejected": 0,
                "rate_limit_per_minute": 6000,
                "window_started_at": time.monotonic(),
                "window_accepted": 0,
                "last_error": None,
            },
        }
        self._last_event_at: str | None = None

    def register(
        self,
        connector_id: str,
        kind: str,
        services: list[str] | None = None,
        enabled: bool = True,
        rate_limit_per_minute: int = 6000,
    ) -> None:
        if not connector_id.strip():
            raise ValueError("Connector ID must not be empty")
        if rate_limit_per_minute < 1:
            raise ValueError("rate_limit_per_minute must be at least one")
        with self._lock:
            if connector_id in self._connectors:
                raise ValueError(f"Connector ID is already registered: {connector_id!r}")
            if len(self._connectors) >= self._max_connectors:
                raise ValueError(f"Connector registry limit reached ({self._max_connectors})")
            self._connectors[connector_id] = {
                "connector_id": connector_id,
                "kind": kind,
                "enabled": enabled,
                "services": sorted(set(services or [])),
                "accepted": 0,
                "rejected": 0,
                "rate_limit_per_minute": rate_limit_per_minute,
                "window_started_at": time.monotonic(),
                "window_accepted": 0,
                "last_error": None,
            }

    def collect(self, event: dict[str, Any], connector_id: str = "simulator") -> bool:
        with self._lock:
            connector = self._connectors.get(connector_id)
            if not connector or not connector["enabled"]:
                if connector:
                    connector["rejected"] += 1
                return False
            now = time.monotonic()
            if now - connector["window_started_at"] >= 60:
                connector["window_started_at"] = now
                connector["window_accepted"] = 0
            rate_limit = connector["rate_limit_per_minute"]
            if rate_limit is not None and connector["window_accepted"] >= rate_limit:
                connector["rejected"] += 1
                connector["last_error"] = "connector rate limit exceeded"
                return False
            try:
                self._queue.put_nowait({"connector_id": connector_id, "event": event})
            except queue.Full:
                connector["rejected"] += 1
                connector["last_error"] = "monitoring event queue is full"
                return False
            connector["accepted"] += 1
            connector["window_accepted"] += 1
            connector["last_error"] = None
            self._last_event_at = str(event.get("@timestamp") or event.get("timestamp") or "")
            return True

    async def run_polling_connector(
        self,
        connector_id: str,
        fetch: Callable[[], Awaitable[dict[str, Any] | list[dict[str, Any]]]],
        interval_seconds: float,
    ) -> None:
        if interval_seconds < 1:
            raise ValueError("Polling interval must be at least one second")
        with self._lock:
            connector = self._connectors.get(connector_id)
            if not connector or connector["kind"] not in {"polling", "api_poll"}:
                raise ValueError(f"Connector {connector_id!r} is not registered as a polling connector")
        while True:
            try:
                result = await fetch()
                events = result if isinstance(result, list) else [result]
                for event in events:
                    if not self.collect(event, connector_id):
                        with self._lock:
                            self._connectors[connector_id]["last_error"] = (
                                "event rejected by connector rate limit or queue capacity"
                            )
                await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                raise
            except (OSError, TimeoutError, ValueError, TypeError) as exc:
                with self._lock:
                    self._connectors[connector_id]["last_error"] = str(exc)
                await asyncio.sleep(interval_seconds)

    async def next_event(self) -> dict[str, Any]:
        while True:
            try:
                return await asyncio.to_thread(self._queue.get, True, 1)
            except queue.Empty:
                await asyncio.sleep(0)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connectors": [
                    {
                        key: value
                        for key, value in connector.items()
                        if key not in {"window_started_at", "window_accepted"}
                    }
                    | {"health": "degraded" if connector["last_error"] else "ready"}
                    for connector in self._connectors.values()
                ],
                "queue_depth": self._queue.qsize(),
                "queue_capacity": self._queue.maxsize,
                "connector_limit": self._max_connectors,
                "last_event_at": self._last_event_at,
            }
