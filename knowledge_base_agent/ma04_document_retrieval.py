"""MA-04: persistent Chroma document storage and cited SOP retrieval."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .common import KnowledgeBaseError, hash_embedding


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
                embeddings=[hash_embedding(item["text"]) for item in source_items],
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
            query_embeddings=[hash_embedding(query)],
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
