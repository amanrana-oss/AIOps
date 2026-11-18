"""MA-04: persistent Chroma document storage and cited SOP retrieval."""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .common import KnowledgeBaseError
from .relevance import rank_passages


def hybrid_rank(semantic_hits: list[dict[str, Any]], lexical_hits: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    fused: dict[str, dict[str, Any]] = {}
    for ranking in (semantic_hits, lexical_hits):
        for rank, hit in enumerate(ranking, start=1):
            identifier = str(hit["id"])
            current = fused.setdefault(identifier, {**hit, "score": 0.0, "retrieval_method": "hybrid_chroma_bm25_rrf"})
            current["score"] += 1 / (60 + rank)
            if hit.get("retrieval_method", "").startswith("chroma"):
                current["semantic_score"] = hit.get("score")
            else:
                current["lexical_score"] = hit.get("score")
            for key in ("text", "metadata"):
                if key not in current and key in hit:
                    current[key] = hit[key]
    return sorted(fused.values(), key=lambda item: (-item["score"], str(item["id"])))[:limit]


class ChromaDocumentStore:
    """Persistent Chroma collection using explicitly supplied offline vectors."""

    embedding_model = "all-MiniLM-L6-v2"

    def __init__(self, persist_path: Path, collection_name: str = "enterprise_sops") -> None:
        self.persist_path = persist_path.resolve()
        self.collection_name = collection_name
        self.client: Any = None
        self.collection: Any = None
        self.embedding_function: Any = None
        self.initialization_error: str | None = None
        self.fallback_path = self.persist_path / "lexical_fallback.sqlite3"
        self.fallback_available = False
        try:
            import chromadb
            from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

            self.persist_path.mkdir(parents=True, exist_ok=True)
            self.embedding_function = DefaultEmbeddingFunction()
            self.client = chromadb.PersistentClient(path=str(self.persist_path))
            self.collection = self.client.get_or_create_collection(
                name=collection_name,
                metadata={"hnsw:space": "cosine"},
                embedding_function=self.embedding_function,
            )
        except (ImportError, OSError, ValueError, RuntimeError) as exc:
            self.initialization_error = str(exc)
            try:
                self.persist_path.mkdir(parents=True, exist_ok=True)
                with self._fallback_connection() as connection:
                    connection.execute(
                        """CREATE TABLE IF NOT EXISTS document_chunks (
                            id TEXT PRIMARY KEY,
                            source_path TEXT NOT NULL,
                            text TEXT NOT NULL,
                            metadata_json TEXT NOT NULL
                        )"""
                    )
                    connection.execute(
                        "CREATE INDEX IF NOT EXISTS idx_document_chunks_source ON document_chunks(source_path)"
                    )
                self.fallback_available = True
            except (OSError, sqlite3.Error) as fallback_error:
                self.initialization_error = f"{self.initialization_error}; SQLite fallback: {fallback_error}"

    @contextmanager
    def _fallback_connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.fallback_path)
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @property
    def available(self) -> bool:
        return self.collection is not None or self.fallback_available

    def upsert(self, items: list[dict[str, Any]]) -> int:
        if not self.available:
            raise KnowledgeBaseError(
                "Chroma is unavailable. Install requirements.txt and restart; "
                f"initialization detail: {self.initialization_error or 'unknown error'}"
            )
        if not items:
            return 0
        if self.collection is None:
            try:
                by_source: dict[str, list[dict[str, Any]]] = {}
                for item in items:
                    by_source.setdefault(item["metadata"]["source_path"], []).append(item)
                with self._fallback_connection() as connection:
                    for source_path, source_items in by_source.items():
                        connection.executemany(
                            "INSERT OR REPLACE INTO document_chunks (id, source_path, text, metadata_json) VALUES (?, ?, ?, ?)",
                            [
                                (item["id"], source_path, item["text"], json.dumps(item["metadata"]))
                                for item in source_items
                            ],
                        )
                        current_ids = [item["id"] for item in source_items]
                        placeholders = ",".join("?" for _ in current_ids)
                        connection.execute(
                            f"DELETE FROM document_chunks WHERE source_path = ? AND id NOT IN ({placeholders})",
                            [source_path, *current_ids],
                        )
            except sqlite3.Error as exc:
                raise KnowledgeBaseError(f"SQLite document index update failed: {exc}") from exc
            return len(items)
        by_source: dict[str, list[dict[str, Any]]] = {}
        for item in items:
            by_source.setdefault(item["metadata"]["source_path"], []).append(item)
        collection_metadata = self.collection.metadata or {}
        semantic_index = collection_metadata.get("embedding_model") == self.embedding_model
        for source_path, source_items in by_source.items():
            previous = self.collection.get(
                where={"source_path": source_path},
                include=["metadatas"],
            )
            previous_ids = set(previous["ids"])
            current_ids = {item["id"] for item in source_items}
            previous_metadata = dict(zip(previous["ids"], previous["metadatas"]))
            source_unchanged = semantic_index and current_ids == previous_ids and all(
                previous_metadata.get(item["id"], {}).get("source_sha256") == item["metadata"].get("source_sha256")
                for item in source_items
            )
            if source_unchanged:
                continue
            for offset in range(0, len(source_items), 32):
                batch = source_items[offset:offset + 32]
                self.collection.upsert(
                    ids=[item["id"] for item in batch],
                    documents=[item["text"] for item in batch],
                    metadatas=[item["metadata"] for item in batch],
                )
            stale_ids = [item_id for item_id in previous_ids if item_id not in current_ids]
            if stale_ids:
                self.collection.delete(ids=stale_ids)
        self.collection.modify(metadata={**collection_metadata, "hnsw:space": "cosine",
                                         "embedding_model": self.embedding_model})
        return len(items)

    def search(self, query: str, limit: int = 6) -> list[dict[str, Any]]:
        if not self.available:
            raise KnowledgeBaseError(
                "Chroma is unavailable; document retrieval cannot run. "
                f"Initialization detail: {self.initialization_error or 'unknown error'}"
            )
        if self.collection is None:
            query_tokens = set(re.findall(r"[a-zA-Z0-9][a-zA-Z0-9_.-]+", query.casefold()))
            if not query_tokens:
                return []
            try:
                with self._fallback_connection() as connection:
                    rows = connection.execute(
                        "SELECT id, text, metadata_json FROM document_chunks"
                    ).fetchall()
            except sqlite3.Error as exc:
                raise KnowledgeBaseError(f"SQLite document search failed: {exc}") from exc
            passages = [{"id": item_id, "text": text, "metadata": json.loads(metadata_json)} for item_id, text, metadata_json in rows]
            return rank_passages(query, passages, limit)
        if self.collection.count() == 0:
            return []
        if (self.collection.metadata or {}).get("embedding_model") != self.embedding_model:
            raise KnowledgeBaseError("Chroma collection has not been indexed with all-MiniLM-L6-v2 yet; ingest the knowledge base before searching")
        semantic_limit = min(max(limit * 4, 12), self.collection.count())
        result = self.collection.query(
            query_texts=[query],
            n_results=semantic_limit,
            include=["documents", "metadatas", "distances"],
        )
        semantic_hits = []
        for index, text in enumerate(result["documents"][0]):
            metadata = result["metadatas"][0][index] or {}
            distance = float(result["distances"][0][index])
            semantic_hits.append({
                "id": result["ids"][0][index], "text": text, "metadata": metadata,
                "score": round(max(0.0, 1.0 - distance), 4),
                "retrieval_method": "chroma_all-MiniLM-L6-v2_cosine",
            })
        lexical_result = self.collection.get(include=["documents", "metadatas"])
        lexical_passages = [
            {"id": identifier, "text": text, "metadata": metadata or {}}
            for identifier, text, metadata in zip(
                lexical_result["ids"], lexical_result["documents"], lexical_result["metadatas"]
            )
        ]
        lexical_hits = rank_passages(query, lexical_passages, limit=semantic_limit)
        return hybrid_rank(semantic_hits, lexical_hits, limit)

    def status(self) -> dict[str, Any]:
        if self.collection is not None:
            count = self.collection.count()
            backend = "chroma"
        elif self.fallback_available:
            with self._fallback_connection() as connection:
                count = connection.execute("SELECT COUNT(*) FROM document_chunks").fetchone()[0]
            backend = "sqlite_lexical_fallback"
        else:
            count = 0
            backend = "unavailable"
        return {
            "configured": self.collection is not None,
            "available": self.available,
            "backend": backend,
            "collection": self.collection_name,
            "persist_path": str(self.persist_path),
            "embedding_model": "all-MiniLM-L6-v2 + BM25 lexical hybrid" if self.collection is not None else "lexical BM25 fallback",
            "chunks": count,
            "error": self.initialization_error,
        }


class DocumentRetrievalMicroAgent:
    """MA-04: retrieve cited SOP/document chunks from Chroma."""

    def __init__(self, store: ChromaDocumentStore) -> None:
        self.store = store

    def search(self, query: str, limit: int = 6) -> list[dict[str, Any]]:
        return self.store.search(query, limit)
