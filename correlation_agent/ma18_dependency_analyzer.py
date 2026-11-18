"""MA-18: relate observed services using the simulator's explicit dependency map."""

from __future__ import annotations

from typing import Any


SERVICE_DEPENDENCIES = {
    "api-gateway": ("authentication-service", "vehicle-telemetry-api", "order-management-service"),
    "vehicle-telemetry-api": ("postgresql", "redis", "kafka"),
    "order-management-service": ("postgresql", "kafka"),
    "kafka": ("notification-service",),
}

SERVICE_ALIASES = {"api": "api-gateway", "database": "postgresql"}
SERVICE_NAME_FIELD = "service.name"


class DependencyAnalyzer:
    """Report dependency edges supported by services present in the evidence."""

    def analyze(
        self, alert_service: str, events: list[dict[str, Any]]
    ) -> dict[str, Any]:
        observed = {SERVICE_ALIASES.get(alert_service, alert_service)}
        observed.update(
            SERVICE_ALIASES.get(str(event.get(SERVICE_NAME_FIELD, "")), str(event.get(SERVICE_NAME_FIELD, "")))
            for event in events
            if event.get(SERVICE_NAME_FIELD)
        )
        links = []
        for source, targets in SERVICE_DEPENDENCIES.items():
            for target in targets:
                if source in observed and target in observed:
                    links.append({"upstream": source, "downstream": target})
        center = SERVICE_ALIASES.get(alert_service, alert_service)
        related = sorted(
            set(SERVICE_DEPENDENCIES.get(center, ()))
            | {source for source, targets in SERVICE_DEPENDENCIES.items() if center in targets}
        )
        return {
            "alert_service": center,
            "observed_services": sorted(observed),
            "observed_dependency_links": links,
            "adjacent_services": related,
            "graph_source": "static simulator dependency map; no graph database configured",
        }