"""AG-02 coordinator for the independently maintained knowledge microagents."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import Any

from .common import KnowledgeBaseError, SUPPORTED_EXTENSIONS, utc_now
from .ma03_document_ingestion import DocumentIngestionMicroAgent
from .ma04_document_retrieval import ChromaDocumentStore, DocumentRetrievalMicroAgent
from .ma05_log_retrieval import LogKnowledgeRetrievalMicroAgent, OpenSearchLogConnector
from .ma06_knowledge_graph import KnowledgeGraphMicroAgent
from .ma07_governance import KnowledgeGovernanceMicroAgent


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
            manual_ingestor = DocumentIngestionMicroAgent(self.data_root / "real-data")
            manuals = await asyncio.to_thread(manual_ingestor.prepare, {".pdf", ".docx"}, 200 * 1024 * 1024)
            for document in manuals["documents"]:
                document["metadata"]["source_path"] = "real-data/" + document["metadata"]["source_path"]
                document["metadata"]["knowledge_kind"] = "uploaded_reference_document"
                document["id"] = hashlib.sha256(("real-data/" + document["id"]).encode()).hexdigest()
            prepared["documents"].extend(manuals["documents"])
            prepared["files_read"] += manuals["files_read"]
            prepared["skipped"].extend(manuals["skipped"])
            trackers = await asyncio.to_thread(self.ingestor.prepare_issue_trackers, self.data_root / "real-data")
            prepared["documents"].extend(trackers["documents"])
            prepared["files_read"] += trackers["files_read"]
            prepared["skipped"].extend(trackers["skipped"])
            indexed = await asyncio.to_thread(self.store.upsert, prepared["documents"])
            result = {
                "files_read": prepared["files_read"],
                "chunks_indexed": indexed,
                "skipped": prepared["skipped"],
                "store": self.store.status()["backend"],
            }
            self.last_ingest = {**result, "completed_at": utc_now()}
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
            "embedding_method": "Hybrid retrieval: local Chroma all-MiniLM-L6-v2 semantic vectors fused with BM25 lexical ranking; lexical-only fallback if Chroma is unavailable",
        }
