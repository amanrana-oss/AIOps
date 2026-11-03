"""MA-05: bounded OpenSearch indexing, JSONL backfill, and operational log search."""

from __future__ import annotations

import asyncio
import json
import os
import queue
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import requests

from .common import KnowledgeBaseError, MAX_BACKFILL_BYTES, MAX_BACKFILL_RECORDS, utc_now


class OpenSearchLogConnector:
    """OpenSearch REST connector for indexing and searching application logs."""

    def __init__(self) -> None:
        self.base_url = os.getenv("OPENSEARCH_URL", "").rstrip("/")
        self.username = os.getenv("OPENSEARCH_USERNAME")
        self.password = os.getenv("OPENSEARCH_PASSWORD")
        self.index_prefix = os.getenv("OPENSEARCH_INDEX_PREFIX", "aiops-logs")
        self.verify_tls = os.getenv("OPENSEARCH_VERIFY_TLS", "true").lower() not in {"0", "false", "no"}
        self.timeout_seconds = float(os.getenv("OPENSEARCH_TIMEOUT_SECONDS", "5"))
        self._auth = (self.username, self.password) if self.username else None
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=10_000)
        self._lock = threading.Lock()
        self.queued = 0
        self.indexed = 0
        self.failed = 0
        self.dropped = 0
        self.last_error: str | None = None

    @property
    def configured(self) -> bool:
        return bool(self.base_url)

    def enqueue(self, record: dict[str, Any]) -> bool:
        if not self.configured:
            return False
        try:
            self._queue.put_nowait(record)
            with self._lock:
                self.queued += 1
            return True
        except queue.Full:
            with self._lock:
                self.dropped += 1
                self.last_error = "OpenSearch queue full; event was not queued"
            return False

    def backfill_jsonl(self, paths: Iterable[Path]) -> dict[str, Any]:
        if not self.configured:
            raise KnowledgeBaseError("OpenSearch is not configured; set OPENSEARCH_URL before backfilling.")
        try:
            files = [path for path in paths if path.is_file()]
            total_bytes = sum(path.stat().st_size for path in files)
        except OSError as exc:
            raise KnowledgeBaseError(f"Unable to inspect JSONL log files: {exc}") from exc
        if total_bytes > MAX_BACKFILL_BYTES:
            raise KnowledgeBaseError(
                f"JSONL backfill is {total_bytes} bytes; the safety limit is {MAX_BACKFILL_BYTES} bytes."
            )
        queued = 0
        invalid_lines = 0
        queue_full = False
        for path in files:
            try:
                with path.open("r", encoding="utf-8") as stream:
                    for line in stream:
                        if not line.strip():
                            continue
                        if queued >= MAX_BACKFILL_RECORDS:
                            return {
                                "files_read": len(files),
                                "records_queued": queued,
                                "invalid_lines": invalid_lines,
                                "record_limit_reached": True,
                                "queue_full": False,
                            }
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            invalid_lines += 1
                            continue
                        if not isinstance(record, dict) or not record.get("@timestamp") or not record.get("event.id"):
                            invalid_lines += 1
                            continue
                        if not self.enqueue(record):
                            queue_full = True
                            return {
                                "files_read": len(files),
                                "records_queued": queued,
                                "invalid_lines": invalid_lines,
                                "record_limit_reached": False,
                                "queue_full": queue_full,
                            }
                        queued += 1
            except OSError as exc:
                raise KnowledgeBaseError(f"Unable to read JSONL log file {path.name}: {exc}") from exc
        return {
            "files_read": len(files),
            "records_queued": queued,
            "invalid_lines": invalid_lines,
            "record_limit_reached": False,
            "queue_full": queue_full,
        }

    def _take_batch(self, batch_size: int = 300) -> list[dict[str, Any]]:
        try:
            first = self._queue.get(timeout=1)
        except queue.Empty:
            return []
        batch = [first]
        while len(batch) < batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _bulk_index(self, batch: list[dict[str, Any]]) -> int:
        lines: list[str] = []
        for record in batch:
            timestamp = record.get("@timestamp", utc_now())
            date = timestamp[:10].replace("-", ".")
            index_name = f"{self.index_prefix}-{date}"
            event_id = record.get("event.id")
            lines.append(json.dumps({"index": {"_index": index_name, "_id": event_id}}))
            lines.append(json.dumps(record, ensure_ascii=False))
        response = requests.post(
            f"{self.base_url}/_bulk",
            data="\n".join(lines) + "\n",
            headers={"Content-Type": "application/x-ndjson"},
            auth=self._auth,
            verify=self.verify_tls,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        result = response.json()
        if result.get("errors"):
            failures = [
                item.get("index", {}).get("error")
                for item in result.get("items", [])
                if item.get("index", {}).get("status", 500) >= 300
            ]
            raise KnowledgeBaseError(
                f"OpenSearch bulk request partially failed for {len(failures)} of {len(batch)} records"
            )
        return len(batch)

    async def run(self) -> None:
        while True:
            if not self.configured:
                await asyncio.sleep(5)
                continue
            batch = await asyncio.to_thread(self._take_batch)
            if not batch:
                continue
            try:
                indexed = await asyncio.to_thread(self._bulk_index, batch)
                with self._lock:
                    self.indexed += indexed
                    self.last_error = None
            except (requests.RequestException, KnowledgeBaseError, ValueError) as exc:
                with self._lock:
                    self.failed += len(batch)
                    self.last_error = str(exc)

    async def search(
        self,
        query: str,
        limit: int = 20,
        service: str | None = None,
        since: str | None = None,
        until: str | None = None,
        host: str | None = None,
        trace_id: str | None = None,
        error_type: str | None = None,
        severity: str | None = None,
        exact: bool = False,
    ) -> list[dict[str, Any]]:
        if not self.configured:
            raise KnowledgeBaseError("OpenSearch is not configured; set OPENSEARCH_URL.")
        filters: list[dict[str, Any]] = []
        if service:
            filters.append({"term": {"service.name.keyword": service}})
        if since:
            time_range: dict[str, str] = {"gte": since}
            if until:
                time_range["lte"] = until
            filters.append({"range": {"@timestamp": time_range}})
        elif until:
            filters.append({"range": {"@timestamp": {"lte": until}}})
        if host:
            filters.append({"term": {"host.name.keyword": host}})
        if trace_id:
            filters.append({"term": {"trace.id.keyword": trace_id}})
        if error_type:
            filters.append({"term": {"error.type.keyword": error_type}})
        if severity:
            filters.append({"term": {"log.level.keyword": severity.lower()}})
        body = {
            "size": limit,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": (
                        [{"match_phrase": {"message": query}}]
                        if exact and query.strip()
                        else [{"match": {"message": query}}]
                        if query.strip()
                        else [{"match_all": {}}]
                    ),
                    "filter": filters,
                }
            },
            "aggs": {
                "by_service": {"terms": {"field": "service.name.keyword", "size": 50}},
                "by_severity": {"terms": {"field": "log.level.keyword", "size": 10}},
            },
        }
        try:
            response = await asyncio.to_thread(
                requests.post,
                f"{self.base_url}/{self.index_prefix}-*/_search",
                json=body,
                auth=self._auth,
                verify=self.verify_tls,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise KnowledgeBaseError(f"OpenSearch log search failed: {exc}") from exc
        return [
            {"id": hit.get("_id"), "index": hit.get("_index"), **hit.get("_source", {})}
            for hit in payload.get("hits", {}).get("hits", [])
        ]

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "configured": self.configured,
                "index_prefix": self.index_prefix,
                "queued": self.queued,
                "indexed": self.indexed,
                "failed": self.failed,
                "dropped": self.dropped,
                "queue_depth": self._queue.qsize(),
                "last_error": self.last_error,
            }


class LogKnowledgeRetrievalMicroAgent:
    """MA-05: search indexed OpenSearch events, or explicitly marked local logs."""

    def __init__(self, connector: OpenSearchLogConnector) -> None:
        self.connector = connector

    async def search(
        self,
        query: str,
        local_records: Iterable[dict[str, Any]] = (),
        limit: int = 20,
        service: str | None = None,
        since: str | None = None,
        until: str | None = None,
        host: str | None = None,
        trace_id: str | None = None,
        error_type: str | None = None,
        severity: str | None = None,
        exact: bool = False,
    ) -> dict[str, Any]:
        if self.connector.configured:
            return {
                "source": "opensearch",
                "items": await self.connector.search(
                    query, limit, service, since, until, host, trace_id, error_type, severity, exact
                ),
            }
        needle = query.casefold().strip()
        items = []
        for record in reversed(list(local_records)):
            if service and record.get("service.name") != service:
                continue
            if host and record.get("host.name") != host:
                continue
            if trace_id and record.get("trace.id") != trace_id:
                continue
            if error_type and record.get("error.type") != error_type:
                continue
            if severity and str(record.get("log.level", "")).lower() != severity.lower():
                continue
            if since and record.get("@timestamp", "") < since:
                continue
            if until and record.get("@timestamp", "") > until:
                continue
            record_text = str(record.get("message", "")).casefold()
            if needle and (
                (record_text != needle if exact else needle not in json.dumps(record, ensure_ascii=False).casefold())
            ):
                continue
            items.append(record)
            if len(items) >= limit:
                break
        service_counts: dict[str, int] = {}
        severity_counts: dict[str, int] = {}
        for item in items:
            service_name = str(item.get("service.name", "unknown"))
            log_level = str(item.get("log.level", "unknown"))
            service_counts[service_name] = service_counts.get(service_name, 0) + 1
            severity_counts[log_level] = severity_counts.get(log_level, 0) + 1
        return {
            "source": "local_buffer",
            "items": items,
            "aggregations": {"by_service": service_counts, "by_severity": severity_counts},
        }
