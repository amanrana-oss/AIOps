"""AG-20: explainable root-cause hypothesis generation and evidence validation."""

from __future__ import annotations

from typing import Any

from .ma21_hypothesis_generator import RootCauseHypothesisGenerator
from .ma22_hypothesis_validator import HypothesisEvidenceValidator


class RootCauseAgent:
    """Rank evidence-backed hypotheses without claiming a confirmed cause."""

    def __init__(self) -> None:
        self.generator = RootCauseHypothesisGenerator()
        self.validator = HypothesisEvidenceValidator()

    def analyze(self, incident: dict[str, Any]) -> dict[str, Any]:
        evidence = incident.get("evidence", {})
        hypotheses = self.validator.validate(incident, self.generator.generate(incident))
        return {
            "agent": "AG-20 RCA Agent",
            "microagents": ["MA-21 hypothesis generator", "MA-22 evidence validator"],
            "hypotheses": hypotheses,
            "evidence_event_count": len(evidence.get("trace_events", [])),
            "cause_confirmed": False,
            "confidence_semantics": "Evidence coverage indicator only; not a probability.",
            "disclaimer": "Candidate explanations require operator validation; this POC does not confirm root cause.",
        }