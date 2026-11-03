"""AG-02 Knowledge Base Agent and its bounded, auditable microagents."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import queue
import re
import threading
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

LOGGER = logging.getLogger("dummy-app.knowledge")
TOKEN_PATTERN = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{1,}")
SUPPORTED_EXTENSIONS = {".txt", ".md", ".rst", ".json", ".yaml", ".yml", ".csv", ".pdf", ".docx"}
EMBEDDING_DIMENSIONS = 384
MAX_DOCUMENT_BYTES = 20 * 1024 * 1024
MAX_CHUNKS_PER_DOCUMENT = 10_000


class KnowledgeBaseError(RuntimeError):
    """A configured knowledge store or ingestion operation failed."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _hash_embedding(text: str) -> list[float]:
    """Create deterministic local lexical vectors without downloading a model."""
    tokens = [token.lower() for token in TOKEN_PATTERN.findall(text)]
    features = tokens + [f"{left}_{right}" for left, right in zip(tokens, tokens[1:])]
    vector = [0.0] * EMBEDDING_DIMENSIONS
    for feature in features:
        digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest()
        number = int.from_bytes(digest, "big")
        index = number % EMBEDDING_DIMENSIONS
        sign = 1.0 if (number >> 8) & 1 else -1.0
        vector[index] += sign
    magnitude = math.sqrt(sum(value * value for value in vector))
    if magnitude:
        vector = [value / magnitude for value in vector]
    return vector


class DocumentIngestionMicroAgent:
    """MA-03: read, normalize, chunk, and prepare local knowledge documents."""

    def __init__(self, source_root: Path, chunk_words: int = 220, overlap_words: int = 35) -> None:
        self.source_root = source_root.resolve()
        self.chunk_words = chunk_words
        self.overlap_words = overlap_words

    def _extract(self, path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix in {".txt", ".md", ".rst", ".yaml", ".yml", ".csv"}:
            return path.read_text(encoding="utf-8-sig")
        if suffix == ".json":
            value = json.loads(path.read_text(encoding="utf-8-sig"))
            return json.dumps(value, ensure_ascii=False, indent=2)
        if suffix == ".pdf":
            from pypdf import PdfReader

            return "\n\n".join(page.extract_text() or "" for page in PdfReader(str(path)).pages)
        if suffix == ".docx":
            import docx2txt

            return docx2txt.process(str(path))
        raise KnowledgeBaseError(f"Unsupported document type: {suffix or '(no extension)'}")

    def _chunks(self, text: str) -> list[str]:
        words = text.split()
        if not words:
            return []
        stride = max(1, self.chunk_words - self.overlap_words)
        chunks = [" ".join(words[start:start + self.chunk_words])
                  for start in range(0, len(words), stride)]
        return [chunk for chunk in chunks if chunk.strip()]

    def prepare(self) -> dict[str, Any]:
        self.source_root.mkdir(parents=True, exist_ok=True)
        documents: list[dict[str, Any]] = []
        skipped: list[dict[str, str]] = []
        file_count = 0
        for path in sorted(self.source_root.rglob("*")):
            if not path.is_file() or path.name.startswith("."):
                continue
            if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                skipped.append({"file": str(path.relative_to(self.source_root)), "reason": "unsupported file type"})
                continue
            try:
                if path.stat().st_size > MAX_DOCUMENT_BYTES:
                    skipped.append({"file": str(path.relative_to(self.source_root)), "reason": "file exceeds 20 MB"})
                    continue
                text = self._extract(path).strip()
                if not text:
                    skipped.append({"file": str(path.relative_to(self.source_root)), "reason": "no extractable text"})
                    continue
                relative_path = path.relative_to(self.source_root).as_posix()
                content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
                chunks = self._chunks(text)
                if len(chunks) > MAX_CHUNKS_PER_DOCUMENT:
                    skipped.append({"file": relative_path, "reason": "document has too many chunks"})
                    continue
                file_count += 1
                review_status = (
                    "approved" if path.parent.name == "incident-learning" else "unreviewed"
                )
                for chunk_index, chunk in enumerate(chunks):
                    chunk_id = hashlib.sha256(
                        f"{relative_path}:{content_hash}:{chunk_index}".encode("utf-8")
                    ).hexdigest()
                    documents.append({
                        "id": chunk_id,
                        "text": chunk,
                        "metadata": {
                            "source_path": relative_path,
                            "source_name": path.name,
                            "title": path.stem,
                            "source_sha256": content_hash,
                            "chunk_index": chunk_index,
                            "document_type": path.suffix.lower().lstrip("."),
                            "ingested_at": _utc_now(),
                            "review_status": review_status,
                        },
                    })
            except (OSError, UnicodeError, json.JSONDecodeError, ImportError, KnowledgeBaseError) as exc:
                skipped.append({"file": str(path.relative_to(self.source_root)), "reason": str(exc)})
        return {"documents": documents, "files_read": file_count, "skipped": skipped}


class ChromaDocumentStore:
    """Persistent Chroma collection using explicitly supplied offline vectors."""

    def __init__(self, persist_path: Path, collection_name: str = "enterprise_sops") -> None:
        self.persist_path = persist_path.resolve()
        self.collection_name = collection_name
        self.client: Any = None
        self.collection: Any = None
        self.initialization_error: str | None = None
        try:
            import chromadb

            self.persist_path.mkdir(parents=True, exist_ok=True)
            self.client = chromadb.PersistentClient(path=str(self.persist_path))
            self.collection = self.client.get_or_create_collection(
                name=collection_name,
                metadata={"hnsw:space": "cosine"},
            )
        except (ImportError, OSError, ValueError, RuntimeError) as exc:
            self.initialization_error = str(exc)

    @property
    def available(self) -> bool:
        return self.collection is not None

    def upsert(self, items: list[dict[str, Any]]) -> int:
        if not self.available:
            raise KnowledgeBaseError(
                "Chroma is unavailable. Install requirements.txt and restart; "
                f"initialization detail: {self.initialization_error or 'unknown error'}"
            )
        if not items:
            return 0
        by_source: dict[str, list[dict[str, Any]]] = {}
        for item in items:
            by_source.setdefault(item["metadata"]["source_path"], []).append(item)
        for source_path, source_items in by_source.items():
            previous = self.collection.get(
                where={"source_path": source_path},
                include=["metadatas"],
            )["ids"]
            self.collection.upsert(
                ids=[item["id"] for item in source_items],
                documents=[item["text"] for item in source_items],
                metadatas=[item["metadata"] for item in source_items],
                embeddings=[_hash_embedding(item["text"]) for item in source_items],
            )
            current_ids = {item["id"] for item in source_items}
            stale_ids = [item_id for item_id in previous if item_id not in current_ids]
            if stale_ids:
                self.collection.delete(ids=stale_ids)
        return len(items)

    def search(self, query: str, limit: int = 6) -> list[dict[str, Any]]:
        if not self.available:
            raise KnowledgeBaseError(
                "Chroma is unavailable; document retrieval cannot run. "
                f"Initialization detail: {self.initialization_error or 'unknown error'}"
            )
        if self.collection.count() == 0:
            return []
        result = self.collection.query(
            query_embeddings=[_hash_embedding(query)],
            n_results=min(limit, self.collection.count()),
            include=["documents", "metadatas", "distances"],
        )
        hits = []
        for index, text in enumerate(result["documents"][0]):
            metadata = result["metadatas"][0][index]
            distance = result["distances"][0][index]
            hits.append({
                "id": result["ids"][0][index],
                "text": text,
                "metadata": metadata,
                "score": round(max(0.0, 1.0 - float(distance)), 4),
            })
        return hits

    def status(self) -> dict[str, Any]:
        count = self.collection.count() if self.available else 0
        return {
            "configured": True,
            "available": self.available,
            "collection": self.collection_name,
            "persist_path": str(self.persist_path),
            "chunks": count,
            "error": self.initialization_error,
        }


class DocumentRetrievalMicroAgent:
    """MA-04: retrieve cited SOP/document chunks from Chroma."""

    def __init__(self, store: ChromaDocumentStore) -> None:
        self.store = store

    def search(self, query: str, limit: int = 6) -> list[dict[str, Any]]:
        return self.store.search(query, limit)


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

    def enqueue(self, record: dict[str, Any]) -> None:
        if not self.configured:
            return
        try:
            self._queue.put_nowait(record)
            with self._lock:
                self.queued += 1
        except queue.Full:
            with self._lock:
                self.dropped += 1
                self.last_error = "OpenSearch queue full; event was not queued"

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
            timestamp = record.get("@timestamp", _utc_now())
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
    ) -> list[dict[str, Any]]:
        if not self.configured:
            raise KnowledgeBaseError("OpenSearch is not configured; set OPENSEARCH_URL.")
        filters: list[dict[str, Any]] = []
        if service:
            filters.append({"term": {"service.name.keyword": service}})
        if since:
            filters.append({"range": {"@timestamp": {"gte": since}}})
        body = {
            "size": limit,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [{"match": {"message": query}}] if query.strip() else [{"match_all": {}}],
                    "filter": filters,
                }
            },
        }
        response = await asyncio.to_thread(
            requests.post,
            f"{self.base_url}/{self.index_prefix}-*/_search",
            json=body,
            auth=self._auth,
            verify=self.verify_tls,
            timeout=self.timeout_seconds,
        )
        try:
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
    ) -> dict[str, Any]:
        if self.connector.configured:
            return {
                "source": "opensearch",
                "items": await self.connector.search(query, limit, service, since),
            }
        needle = query.casefold().strip()
        items = []
        for record in reversed(list(local_records)):
            if service and record.get("service.name") != service:
                continue
            if since and record.get("@timestamp", "") < since:
                continue
            if needle and needle not in json.dumps(record, ensure_ascii=False).casefold():
                continue
            items.append(record)
            if len(items) >= limit:
                break
        return {"source": "local_buffer", "items": items}


class KnowledgeGraphMicroAgent:
    """MA-06: explicit deferred boundary for the graph phase."""

    def status(self) -> dict[str, Any]:
        return {
            "implemented": False,
            "state": "deferred",
            "reason": "Graph database and relationship schema are scheduled for a later phase.",
        }


class KnowledgeGovernanceMicroAgent:
    """MA-07: preserve provenance, highlight stale evidence, and flag possible conflicts."""

    POSITIVE_DIRECTIVES = ("must ", "required", "always ", "shall ")
    NEGATIVE_DIRECTIVES = ("must not ", "do not ", "never ", "shall not ")

    def assess(self, hits: list[dict[str, Any]]) -> dict[str, Any]:
        now = datetime.now(timezone.utc).date()
        evidence = []
        for hit in hits:
            metadata = hit.get("metadata", {})
            item = {**hit, "citation": {
                "source": metadata.get("source_path", "unknown"),
                "chunk": metadata.get("chunk_index", 0),
                "sha256": metadata.get("source_sha256", ""),
            }}
            item["review_required"] = metadata.get("review_status", "unreviewed") != "approved"
            expiry = metadata.get("effective_until")
            if expiry:
                try:
                    item["stale"] = datetime.fromisoformat(str(expiry)).date() < now
                except ValueError:
                    item["stale"] = True
                    item["metadata_warning"] = "effective_until is not a valid ISO date"
            else:
                item["stale"] = False
            evidence.append(item)

        conflicts = []
        for index, left in enumerate(evidence):
            left_text = left.get("text", "").lower()
            for right in evidence[index + 1:]:
                left_title = left.get("metadata", {}).get("title")
                right_title = right.get("metadata", {}).get("title")
                if not left_title or left_title != right_title:
                    continue
                right_text = right.get("text", "").lower()
                left_polarity = self._polarity(left_text)
                right_polarity = self._polarity(right_text)
                if left_polarity and right_polarity and left_polarity != right_polarity:
                    conflicts.append({
                        "type": "possible_conflict",
                        "sources": [left["citation"], right["citation"]],
                        "note": "Opposing directive wording found in similarly titled documents; human review required.",
                    })
        return {
            "evidence": evidence,
            "possible_conflicts": conflicts,
            "disclaimer": "Retrieved text is evidence, not an automated policy decision. Verify the cited source and revision.",
        }

    def _polarity(self, text: str) -> str | None:
        positive = any(phrase in text for phrase in self.POSITIVE_DIRECTIVES)
        negative = any(phrase in text for phrase in self.NEGATIVE_DIRECTIVES)
        if positive == negative:
            return None
        return "negative" if negative else "positive"

    def approved_incident_learning(
        self,
        incident_id: str,
        summary: str,
        evidence: list[str],
        learning_root: Path,
    ) -> dict[str, Any]:
        if not incident_id.strip() or not summary.strip() or not evidence:
            raise KnowledgeBaseError("incident_id, summary, and at least one evidence reference are required")
        safe_id = re.sub(r"[^a-zA-Z0-9._-]", "-", incident_id.strip())[:100]
        if not safe_id or safe_id in {".", ".."}:
            raise KnowledgeBaseError("incident_id must contain a usable letter or number")
        learning_root.mkdir(parents=True, exist_ok=True)
        destination = learning_root / f"{safe_id}.json"
        if destination.exists():
            raise KnowledgeBaseError(f"incident learning record already exists: {safe_id}")
        record = {
            "incident_id": incident_id.strip(),
            "summary": summary.strip(),
            "evidence": evidence,
            "recorded_at": _utc_now(),
            "review_status": "approved",
            "provenance": "human-reviewed incident learning",
        }
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(destination)
        return {"status": "approved_pending_index", "file": destination.name, "record": record}


class KnowledgeBaseAgent:
    """AG-02: coordinate ingestion, document/log retrieval, graph boundary, and governance."""

    def __init__(self, data_root: Path, opensearch: OpenSearchLogConnector) -> None:
        self.data_root = data_root.resolve()
        self.ingestor = DocumentIngestionMicroAgent(self.data_root / "sops")
        self.store = ChromaDocumentStore(self.data_root / "chroma")
        self.document_retrieval = DocumentRetrievalMicroAgent(self.store)
        self.log_retrieval = LogKnowledgeRetrievalMicroAgent(opensearch)
        self.graph = KnowledgeGraphMicroAgent()
        self.governance = KnowledgeGovernanceMicroAgent()
        self._ingest_lock = asyncio.Lock()
        self.last_ingest: dict[str, Any] | None = None

    async def ingest_documents(self) -> dict[str, Any]:
        async with self._ingest_lock:
            prepared = await asyncio.to_thread(self.ingestor.prepare)
            indexed = await asyncio.to_thread(self.store.upsert, prepared["documents"])
            result = {
                "files_read": prepared["files_read"],
                "chunks_indexed": indexed,
                "skipped": prepared["skipped"],
                "store": "chroma",
            }
            self.last_ingest = {**result, "completed_at": _utc_now()}
            return result

    def search_documents(self, query: str, limit: int = 6) -> dict[str, Any]:
        if not query.strip():
            raise KnowledgeBaseError("query must not be empty")
        hits = self.document_retrieval.search(query, limit)
        return self.governance.assess(hits)

    def status(self, opensearch: OpenSearchLogConnector) -> dict[str, Any]:
        return {
            "agent": "AG-02 Knowledge Base Agent",
            "microagents": {
                "MA-03_document_ingestion_indexing": {
                    "state": "ready",
                    "source_directory": str(self.ingestor.source_root),
                    "supported_types": sorted(SUPPORTED_EXTENSIONS),
                    "last_ingest": self.last_ingest,
                },
                "MA-04_document_knowledge_retrieval": self.store.status(),
                "MA-05_log_knowledge_retrieval": opensearch.status(),
                "MA-06_knowledge_graph_relationship_manager": self.graph.status(),
                "MA-07_governance_conflict_incident_learning": {
                    "state": "ready",
                    "human_review_required": True,
                },
            },
            "llm_required": False,
            "embedding_method": "deterministic local lexical vectors (no model download)",
        }
