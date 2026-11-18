import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from knowledge_base_agent.ma04_document_retrieval import ChromaDocumentStore
from knowledge_base_agent.ma06_knowledge_graph import KnowledgeGraphMicroAgent


class LocalDocumentFallbackTests(unittest.TestCase):
    def test_sqlite_fallback_persists_and_returns_source_metadata(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            with patch.dict(sys.modules, {"chromadb": None}):
                store = ChromaDocumentStore(Path(temporary_directory))

            item = {
                "id": "chunk-1",
                "text": "Database connection pool pressure can cause request timeouts.",
                "metadata": {"source_path": "demo.md", "source_name": "demo.md", "chunk_index": 0},
            }
            self.assertEqual(store.status()["backend"], "sqlite_lexical_fallback")
            self.assertEqual(store.upsert([item]), 1)

            results = store.search("database pool timeout")

            self.assertEqual(results[0]["metadata"]["source_path"], "demo.md")
            self.assertIn("connection pool", results[0]["text"])
            self.assertEqual(store.status()["chunks"], 1)

    def test_local_graph_returns_known_neighbors_and_observed_edges(self):
        graph = KnowledgeGraphMicroAgent()

        result = graph.analyze(
            "vehicle-telemetry-api",
            {"vehicle-telemetry-api", "postgresql", "redis"},
        )

        self.assertIn("postgresql", result["neighbors"])
        self.assertEqual(result["observed_edges"], [
            {"upstream": "vehicle-telemetry-api", "downstream": "postgresql"},
            {"upstream": "vehicle-telemetry-api", "downstream": "redis"},
        ])
        self.assertFalse(result["cause_confirmed"])


if __name__ == "__main__":
    unittest.main()