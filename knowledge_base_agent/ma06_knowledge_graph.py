"""MA-06: bounded, read-only service relationship graph for POC correlation."""

from __future__ import annotations

from typing import Any

from correlation_agent.ma18_dependency_analyzer import SERVICE_ALIASES, SERVICE_DEPENDENCIES


class KnowledgeGraphMicroAgent:
    """Expose local service edges and one-hop neighborhoods without a graph server."""

    def analyze(self, service: str, observed_services: set[str] | None = None) -> dict[str, Any]:
        center = SERVICE_ALIASES.get(service, service)
        observed = {SERVICE_ALIASES.get(item, item) for item in (observed_services or set())}
        edges = [
            {"upstream": source, "downstream": target}
            for source, targets in SERVICE_DEPENDENCIES.items()
            for target in targets
            if not observed or (source in observed and target in observed)
        ]
        neighbors = sorted(
            set(SERVICE_DEPENDENCIES.get(center, ()))
            | {source for source, targets in SERVICE_DEPENDENCIES.items() if center in targets}
        )
        return {
            "service": center,
            "neighbors": neighbors,
            "observed_edges": edges,
            "storage": "in-memory static POC graph",
            "cause_confirmed": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "implemented": True,
            "state": "ready",
            "storage": "in-memory static POC graph; no graph database configured",
            "edge_count": sum(len(targets) for targets in SERVICE_DEPENDENCIES.values()),
        }
