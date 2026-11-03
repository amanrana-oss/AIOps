"""MA-12: known error signatures, stack-trace grouping, and trace correlation."""

from __future__ import annotations

import hashlib
import re
import threading
from collections import OrderedDict, deque
from typing import Any


class LogTraceAnalyzerMicroAgent:
    """Group failures by signature and retain bounded trace event context."""

    ERROR_PATTERN = re.compile(
        r"(timeout|timed out|exception|error|refused|failed|failure|unavailable|"
        r"deadlock|outofmemory|traceback|HTTP 5\d\d)",
        re.IGNORECASE,
    )

    def __init__(
        self,
        max_events: int = 20_000,
        max_events_per_trace: int = 100,
        max_traces: int = 5_000,
        max_signatures: int = 5_000,
    ) -> None:
        self._recent: deque[dict[str, Any]] = deque(maxlen=max_events)
        self._traces: OrderedDict[str, deque[dict[str, Any]]] = OrderedDict()
        self._max_events_per_trace = max_events_per_trace
        self._max_traces = max_traces
        self._signature_counts: OrderedDict[str, int] = OrderedDict()
        self._max_signatures = max_signatures
        self._lock = threading.Lock()

    def analyze(self, event: dict[str, Any]) -> dict[str, Any] | None:
        message = str(event.get("message", ""))
        stack = str(event.get("error.stack_trace") or event.get("stack_trace") or "")
        severity = event.get("log.level")
        status = event.get("http.response.status_code")
        known_non_failure = severity in {"debug", "info", "warning"} or event.get("event.outcome") == "success"
        failed = (
            severity in {"error", "critical"}
            or event.get("event.outcome") == "failure"
            or (isinstance(status, (int, float)) and status >= 500)
            or bool(stack)
            or (not known_non_failure and bool(self.ERROR_PATTERN.search(message)))
        )
        trace_id = event.get("trace.id")
        with self._lock:
            self._recent.append(event)
            if trace_id:
                trace_key = str(trace_id)
                trace = self._traces.get(trace_key)
                if trace is None:
                    trace = deque(maxlen=self._max_events_per_trace)
                    self._traces[trace_key] = trace
                trace.append(event)
                self._traces.move_to_end(trace_key)
                if len(self._traces) > self._max_traces:
                    self._traces.popitem(last=False)
            if not failed:
                return None
            error_type = str(event.get("error.type") or event.get("error_type") or "")
            signature_source = error_type or stack or message
            signature = hashlib.sha256(
                f"{event.get('service.name', 'unknown')}:{signature_source}".encode("utf-8")
            ).hexdigest()[:16]
            self._signature_counts[signature] = self._signature_counts.get(signature, 0) + 1
            self._signature_counts.move_to_end(signature)
            if len(self._signature_counts) > self._max_signatures:
                self._signature_counts.popitem(last=False)
            return {
                "type": "log_failure",
                "service": event.get("service.name", "unknown"),
                "signature": signature,
                "error_type": error_type or None,
                "message": message[:500],
                "trace_id": trace_id,
                "request_id": event.get("request_id"),
                "stack_trace_present": bool(stack),
                "signature_occurrences": self._signature_counts[signature],
            }

    def trace(self, trace_id: str) -> dict[str, Any]:
        with self._lock:
            events = list(self._traces.get(trace_id, ()))
        return {"trace_id": trace_id, "event_count": len(events), "events": events}

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "retained_events": len(self._recent),
                "known_error_signatures": len(self._signature_counts),
                "correlated_traces": len(self._traces),
                "trace_limit": self._max_traces,
                "signature_limit": self._max_signatures,
            }
