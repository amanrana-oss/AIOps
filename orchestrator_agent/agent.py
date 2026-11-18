"""AG-01: coordinate alert evidence collection and human-reviewed incidents."""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

INCIDENT_RECORD_QUERY = "SELECT record_json FROM incidents WHERE incident_id = ?"


class OrchestratorAgent:
    """Persist bounded workflow state for investigations created from alerts."""

    DEDUPLICATION_WINDOW_SECONDS = 600
    MAX_DEDUP_SCAN = 250

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path.resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._connection() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS incidents (
                    incident_id TEXT PRIMARY KEY,
                    source_alert_id TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    record_json TEXT NOT NULL
                )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS archived_customer_investigations (
                    incident_id TEXT PRIMARY KEY,
                    archived_at TEXT NOT NULL
                )"""
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def create_investigation(
        self,
        alert: dict[str, Any],
        trace_events: list[dict[str, Any]],
        log_evidence: dict[str, Any],
        sop_evidence: dict[str, Any],
        correlation_evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        alert_id = str(alert.get("alert_id", "")).strip()
        if not alert_id:
            raise ValueError("Alert must have an alert_id")
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        signal = alert.get("signal", {})
        dedupe_key = self._deduplication_key(alert)
        detected_at = float(alert.get("detected_at", time.time()))
        trace_id = str(alert.get("event_evidence", {}).get("trace_id") or "")
        related_alerts = self._related_alerts(alert)
        services = self._affected_services(alert, trace_events)
        record = {
            "incident_id": f"INC-{uuid.uuid4().hex[:12].upper()}",
            "source_alert_id": alert_id,
            "created_at": now,
            "updated_at": now,
            "status": "pending_review",
            "summary": str(signal.get("message") or signal.get("type") or "Monitoring alert")[:500],
            "service": alert.get("service", signal.get("service", "unknown")),
            "severity": alert.get("severity", "medium"),
            "cause_confirmed": False,
            "alert": alert,
            "evidence": {
                "customer_feedback": [alert["customer_feedback"]] if alert.get("customer_feedback") else [],
                "trace_events": trace_events,
                "logs": log_evidence,
                "sops": sop_evidence,
                "correlation": correlation_evidence or {
                    "agent": "AG-16 Correlation Agent",
                    "state": "not_run",
                    "cause_confirmed": False,
                },
            },
            "deduplication": {
                "key": dedupe_key,
                "occurrences": 1,
                "alert_ids": sorted({str(item["alert_id"]) for item in related_alerts if item.get("alert_id")}),
                "trace_ids": [trace_id] if trace_id else [],
                "first_seen_epoch": detected_at,
                "last_seen_epoch": detected_at,
            },
            "impact": {
                "affected_services": services,
                "signal_count": len(related_alerts),
                "trace_count": 1 if trace_id else 0,
                "first_seen": alert.get("first_seen") or now,
                "last_seen": alert.get("first_seen") or now,
                "symptoms_active": True,
            },
            "review_note": None,
        }
        with self._lock, self._connection() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO incidents
                   (incident_id, source_alert_id, created_at, updated_at, status, record_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    record["incident_id"], alert_id, now, now,
                    record["status"], json.dumps(record, ensure_ascii=False),
                ),
            )
            row = connection.execute(
                "SELECT record_json FROM incidents WHERE source_alert_id = ?", (alert_id,)
            ).fetchone()
        return json.loads(row["record_json"])

    @staticmethod
    def _deduplication_key(alert: dict[str, Any]) -> str:
        signal = alert.get("signal", {})
        service = str(alert.get("service") or signal.get("service") or "unknown").strip().casefold()
        fingerprint = str(alert.get("fingerprint") or "").strip().casefold()
        if fingerprint:
            return f"{service}|{fingerprint}"
        signal_type = str(signal.get("type") or "alert").strip().casefold()
        signature = signal.get("signature") or signal.get("error_type") or signal.get("metric")
        if not signature:
            message = str(signal.get("message") or "").casefold()
            signature = re.sub(r"\d+", "#", re.sub(r"\s+", " ", message)).strip()[:240]
        return f"{service}|{signal_type}|{str(signature or signal_type).strip().casefold()}"

    @staticmethod
    def _related_alerts(alert: dict[str, Any]) -> list[dict[str, Any]]:
        related = alert.get("related_alerts") or [{
            "alert_id": alert.get("alert_id"),
            "service": alert.get("service"),
            "severity": alert.get("severity"),
            "signal": alert.get("signal", {}),
            "actionable": True,
        }]
        return [item for item in related if isinstance(item, dict)]

    @staticmethod
    def _affected_services(alert: dict[str, Any], events: list[dict[str, Any]]) -> list[str]:
        services = {
            str(event.get("service.name"))
            for event in events
            if event.get("service.name")
        }
        services.update(
            str(item.get("service"))
            for item in OrchestratorAgent._related_alerts(alert)
            if item.get("service")
        )
        if alert.get("service"):
            services.add(str(alert["service"]))
        return sorted(services)

    @staticmethod
    def _merge_unique_records(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for item in [*existing, *incoming]:
            if not isinstance(item, dict):
                continue
            item_id = item.get("event.id") or item.get("event_id") or json.dumps(item, sort_keys=True, ensure_ascii=False)
            merged[str(item_id)] = item
        return list(merged.values())[-500:]

    def _merge_evidence(
        self,
        record: dict[str, Any],
        trace_events: list[dict[str, Any]],
        log_evidence: dict[str, Any],
        sop_evidence: dict[str, Any],
        correlation_evidence: dict[str, Any] | None,
    ) -> None:
        evidence = record.setdefault("evidence", {})
        evidence["trace_events"] = self._merge_unique_records(evidence.get("trace_events", []), trace_events)
        for key, incoming, collection in (
            ("logs", log_evidence, "items"),
            ("sops", sop_evidence, "evidence"),
        ):
            existing_data = evidence.get(key, {})
            existing_items = existing_data.get(collection, [])
            incoming_items = incoming.get(collection, []) if isinstance(incoming, dict) else []
            evidence[key] = {**existing_data, **(incoming if isinstance(incoming, dict) else {})}
            if existing_items or incoming_items:
                evidence[key][collection] = self._merge_unique_records(existing_items, incoming_items)
        if correlation_evidence:
            evidence["correlation"] = correlation_evidence

    @staticmethod
    def _merge_alert_summary(
        record: dict[str, Any], incoming_alerts: list[dict[str, Any]], alert: dict[str, Any]
    ) -> None:
        severity_order = {"high": 3, "medium": 2, "low": 1}
        old_alert = record.setdefault("alert", {})
        all_related = OrchestratorAgent._merge_unique_records(
            OrchestratorAgent._related_alerts(old_alert), incoming_alerts
        )
        old_alert["related_alerts"] = all_related
        old_alert["related_actionable_signal_count"] = sum(
            1 for item in all_related if item.get("actionable", True)
        )
        severities = [str(alert.get("severity", "low"))]
        severities.extend(str(item.get("severity", "low")) for item in incoming_alerts)
        latest_severity = max(severities, key=lambda value: severity_order.get(value, 0))
        if severity_order.get(latest_severity, 0) > severity_order.get(record.get("severity", "low"), 0):
            record["severity"] = latest_severity
            old_alert["severity"] = latest_severity

    @staticmethod
    def _merge_impact(
        record: dict[str, Any],
        alert: dict[str, Any],
        incoming_alerts: list[dict[str, Any]],
        incoming_services: list[str],
        trace_id: str,
        detected_at: float,
    ) -> None:
        deduplication = record.get("deduplication", {})
        deduplication["occurrences"] = int(deduplication.get("occurrences", 1)) + 1
        deduplication["alert_ids"] = sorted({
            *deduplication.get("alert_ids", []),
            *(str(item["alert_id"]) for item in incoming_alerts if item.get("alert_id")),
        })
        trace_ids = set(deduplication.get("trace_ids", []))
        if trace_id:
            trace_ids.add(trace_id)
        deduplication["trace_ids"] = sorted(trace_ids)
        deduplication["last_seen_epoch"] = detected_at
        record["deduplication"] = deduplication

        impact = record.setdefault("impact", {})
        impact["affected_services"] = sorted(set(impact.get("affected_services", [])) | set(incoming_services))
        impact["signal_count"] = len(deduplication["alert_ids"])
        impact["trace_count"] = len(trace_ids)
        impact["last_seen"] = alert.get("first_seen") or datetime.fromtimestamp(detected_at, timezone.utc).isoformat()
        impact["symptoms_active"] = True

    def _merge_incident_record(
        self,
        record: dict[str, Any],
        alert: dict[str, Any],
        trace_events: list[dict[str, Any]],
        log_evidence: dict[str, Any],
        sop_evidence: dict[str, Any],
        correlation_evidence: dict[str, Any] | None,
        incoming_alerts: list[dict[str, Any]],
        incoming_services: list[str],
        trace_id: str,
        detected_at: float,
    ) -> dict[str, Any]:
        self._merge_evidence(record, trace_events, log_evidence, sop_evidence, correlation_evidence)
        if alert.get("customer_feedback"):
            feedback = record["evidence"].setdefault("customer_feedback", [])
            if not any(item.get("feedback_id") == alert["customer_feedback"]["feedback_id"] for item in feedback):
                feedback.append(alert["customer_feedback"])
        self._merge_alert_summary(record, incoming_alerts, alert)
        self._merge_impact(record, alert, incoming_alerts, incoming_services, trace_id, detected_at)
        record["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return record

    def merge_recent_investigation(
        self,
        alert: dict[str, Any],
        trace_events: list[dict[str, Any]],
        log_evidence: dict[str, Any],
        sop_evidence: dict[str, Any],
        correlation_evidence: dict[str, Any] | None = None,
        window_seconds: int = DEDUPLICATION_WINDOW_SECONDS,
    ) -> dict[str, Any] | None:
        """Merge a repeated same-signature alert into a recent open incident."""
        dedupe_key = self._deduplication_key(alert)
        detected_at = float(alert.get("detected_at", time.time()))
        trace_id = str(alert.get("event_evidence", {}).get("trace_id") or "")
        incoming_alerts = self._related_alerts(alert)
        incoming_services = self._affected_services(alert, trace_events)
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                "SELECT incident_id, record_json FROM incidents WHERE status != 'closed' ORDER BY updated_at DESC LIMIT ?",
                (self.MAX_DEDUP_SCAN,),
            ).fetchall()
            for row in rows:
                record = json.loads(row["record_json"])
                deduplication = record.get("deduplication", {})
                last_seen = float(deduplication.get("last_seen_epoch", 0))
                if deduplication.get("key") != dedupe_key or detected_at - last_seen > window_seconds or detected_at < last_seen:
                    continue
                record = self._merge_incident_record(
                    record, alert, trace_events, log_evidence, sop_evidence,
                    correlation_evidence, incoming_alerts, incoming_services,
                    trace_id, detected_at,
                )
                connection.execute(
                    "UPDATE incidents SET updated_at = ?, record_json = ? WHERE incident_id = ?",
                    (record["updated_at"], json.dumps(record, ensure_ascii=False), row["incident_id"]),
                )
                return record
        return None

    def list_incidents(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                "SELECT record_json FROM incidents ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [json.loads(row["record_json"]) for row in rows]

    def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        with self._lock, self._connection() as connection:
            row = connection.execute(
                INCIDENT_RECORD_QUERY, (incident_id,)
            ).fetchone()
        return json.loads(row["record_json"]) if row else None

    def archive_customer_investigation(self, incident_id: str) -> bool:
        incident = self.get_incident(incident_id)
        if not incident or not incident.get("alert", {}).get("customer_feedback"):
            return False
        with self._lock, self._connection() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO archived_customer_investigations VALUES (?, ?)",
                (incident_id, datetime.now(timezone.utc).isoformat(timespec="seconds")),
            )
        return True

    def delete_customer_investigation(self, incident_id: str) -> bool:
        with self._lock, self._connection() as connection:
            row = connection.execute(
                INCIDENT_RECORD_QUERY, (incident_id,)
            ).fetchone()
            if not row or not json.loads(row["record_json"]).get("alert", {}).get("customer_feedback"):
                return False
            connection.execute("DELETE FROM incidents WHERE incident_id = ?", (incident_id,))
            connection.execute("DELETE FROM archived_customer_investigations WHERE incident_id = ?", (incident_id,))
        return True

    def archived_customer_investigation_ids(self) -> set[str]:
        with self._lock, self._connection() as connection:
            rows = connection.execute("SELECT incident_id FROM archived_customer_investigations").fetchall()
        return {row["incident_id"] for row in rows}

    def is_customer_investigation_archived(self, incident_id: str) -> bool:
        with self._lock, self._connection() as connection:
            row = connection.execute(
                "SELECT 1 FROM archived_customer_investigations WHERE incident_id = ?", (incident_id,)
            ).fetchone()
        return row is not None

    def review_incident(
        self, incident_id: str, status: str, note: str | None = None
    ) -> dict[str, Any] | None:
        if status not in {"acknowledged", "closed"}:
            raise ValueError("Review status must be acknowledged or closed")
        with self._lock, self._connection() as connection:
            row = connection.execute(
                INCIDENT_RECORD_QUERY, (incident_id,)
            ).fetchone()
            if not row:
                return None
            record = json.loads(row["record_json"])
            if record["status"] == "closed" and status != "closed":
                raise ValueError("Closed incidents cannot be reopened")
            record["status"] = status
            record["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            if note is not None:
                record["review_note"] = note.strip()[:1000] or None
            connection.execute(
                """UPDATE incidents SET updated_at = ?, status = ?, record_json = ?
                   WHERE incident_id = ?""",
                (record["updated_at"], status, json.dumps(record, ensure_ascii=False), incident_id),
            )
        return record

    def save_knowledge_evidence(self, incident_id: str, knowledge: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock, self._connection() as connection:
            row = connection.execute("SELECT record_json FROM incidents WHERE incident_id=?", (incident_id,)).fetchone()
            if not row:
                return None
            record = json.loads(row["record_json"])
            record.setdefault("evidence", {})["sops"] = knowledge
            record["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            connection.execute("UPDATE incidents SET updated_at=?,record_json=? WHERE incident_id=?",
                               (record["updated_at"], json.dumps(record, ensure_ascii=False), incident_id))
        return record

    def save_poc_workflow(
        self, incident_id: str, workflow: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Persist POC agent outputs within the existing incident JSON record."""
        with self._lock, self._connection() as connection:
            row = connection.execute(
                INCIDENT_RECORD_QUERY, (incident_id,)
            ).fetchone()
            if not row:
                return None
            record = json.loads(row["record_json"])
            record["poc"] = workflow
            record["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            connection.execute(
                "UPDATE incidents SET updated_at = ?, record_json = ? WHERE incident_id = ?",
                (record["updated_at"], json.dumps(record, ensure_ascii=False), incident_id),
            )
        return record