"""MA-06: graph boundary; implementation intentionally deferred by project scope."""

from __future__ import annotations

from typing import Any


class KnowledgeGraphMicroAgent:
    """MA-06: explicit deferred boundary for the graph phase."""

    def status(self) -> dict[str, Any]:
        return {
            "implemented": False,
            "state": "deferred",
            "reason": "Graph database and relationship schema are scheduled for a later phase.",
        }
