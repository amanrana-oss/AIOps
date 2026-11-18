"""Deterministic relevance ranking for existing persisted knowledge chunks."""

from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any


STOP_WORDS = set("a an the my your our it its is are was were be been being i me we you they this that these those why what how when where any many so do does did doing done not no working work properly problem issue issues please help getting get have has had can cannot could would should to of for from with and or but on in at as by".split())
EXPANSIONS = {
    "app": ("application", "mobile"), "application": ("app", "mobile"),
    "sync": ("syncing", "synchronization", "data"), "syncing": ("sync", "data"),
    "pin": ("passcode",), "passcode": ("pin",),
    "tyre": ("tire",), "tire": ("tyre",), "psi": ("pressure",),
    "bluetooth": ("pairing",), "otp": ("verification",),
}


def tokens(text: str) -> list[str]:
    return re.findall(r"[^\W_]+", text.casefold(), flags=re.UNICODE)


def query_terms(query: str) -> tuple[set[str], set[str]]:
    primary = {term for term in tokens(query) if term not in STOP_WORDS and len(term) > 1}
    expanded = {related for term in primary for related in EXPANSIONS.get(term, ())} - primary
    return primary, expanded


def rank_passages(query: str, passages: list[dict[str, Any]], limit: int = 6) -> list[dict[str, Any]]:
    primary, expanded = query_terms(query)
    if not primary or not passages:
        return []
    documents = [Counter(tokens(str(item.get("text", "")))) for item in passages]
    frequencies = Counter(term for document in documents for term in document)
    average_length = sum(sum(document.values()) for document in documents) / len(documents) or 1
    scored = []
    for passage, document in zip(passages, documents):
        matches = primary.intersection(document)
        if not matches:
            continue
        length = sum(document.values())
        score = 0.0
        for term in primary | expanded:
            count = document.get(term, 0)
            if not count:
                continue
            inverse = math.log(1 + (len(documents) - frequencies[term] + 0.5) / (frequencies[term] + 0.5))
            score += (1 if term in primary else 0.35) * inverse * count * 2.2 / (count + 1.2 * (0.25 + 0.75 * length / average_length))
        metadata = passage.get("metadata", {})
        title_terms = set(tokens(str(metadata.get("title", ""))))
        score += len(primary & title_terms) * 0.6
        lowered = str(passage.get("text", "")).casefold()
        if re.search(r"\b(not|fail\w*|won't|isn't|problem|issue|crash\w*)\b", query.casefold()):
            if any(marker in lowered for marker in ("check the following", "troubleshooting", "not showing live data", "does not connect")):
                score += 2.0
        if metadata.get("document_type") == "pdf":
            score += 0.15
        scored.append({**passage, "score": round(score, 4), "matched_terms": sorted(matches),
                       "retrieval_method": "bm25_lexical_with_query_expansion"})
    return sorted(scored, key=lambda item: (-item["score"], str(item.get("id", ""))))[:limit]