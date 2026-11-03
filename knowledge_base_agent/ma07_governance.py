"""MA-07: evidence provenance, conflict flags, and pending incident-learning review."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .common import KnowledgeBaseError, utc_now


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
                left_metadata = left.get("metadata", {})
                right_metadata = right.get("metadata", {})
                left_topic = left_metadata.get("topic") or left_metadata.get("title")
                right_topic = right_metadata.get("topic") or right_metadata.get("title")
                if not left_topic or left_topic != right_topic:
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
        negative = any(phrase in text for phrase in self.NEGATIVE_DIRECTIVES)
        positive_text = text
        for phrase in self.NEGATIVE_DIRECTIVES:
            positive_text = positive_text.replace(phrase, " ")
        positive = any(phrase in positive_text for phrase in self.POSITIVE_DIRECTIVES)
        if positive == negative:
            return None
        return "negative" if negative else "positive"

    def submit_incident_learning(
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
            "recorded_at": utc_now(),
            "review_status": "pending_review",
            "provenance": "simulator incident-learning submission",
        }
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(destination)
        return {"status": "pending_human_review", "file": destination.name, "record": record}
