"""MA-15: synthetic customer feedback generation and local heuristic analysis."""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


class FeedbackAnalyzerMicroAgent:
    """Scrub basic PII, label supplied reviews, and summarize feedback themes."""

    POSITIVE_TERMS = {
        "comfortable", "comfort", "smooth", "reliable", "love", "great", "excellent",
        "enjoy", "quality", "powerful", "stylish", "happy", "satisfied", "fun", "classic",
    }
    NEGATIVE_TERMS = {
        "problem", "issue", "vibration", "service", "delay", "expensive", "heavy",
        "uncomfortable", "breakdown", "fault", "difficult", "noise", "poor", "repair",
    }
    TOPICS = {
        "comfort": {"comfortable", "comfort", "seat", "riding position"},
        "performance": {"engine", "power", "pickup", "performance", "smooth", "vibration"},
        "service": {"service", "dealer", "maintenance", "repair", "spare"},
        "reliability": {"reliable", "reliability", "breakdown", "fault", "starting"},
        "design": {"design", "style", "classic", "look", "paint"},
        "value": {"price", "value", "cost", "expensive", "mileage", "fuel"},
    }
    EMAIL_PATTERN = re.compile(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b")
    PHONE_PATTERN = re.compile(r"(?<!\w)(?:\+?\d[\d ()-]{7,}\d)(?!\w)")

    def __init__(self, dataset_path: Path) -> None:
        self.dataset_path = dataset_path.resolve()
        self._summary_cache: dict[str, Any] | None = None
        self._summary_mtime_ns: int | None = None

    def analyze_text(
        self,
        text: str,
        rating: int | None = None,
        synthetic: bool = False,
    ) -> dict[str, Any]:
        clean_text = self.scrub_pii(text)
        tokens = re.findall(r"[a-z']+", clean_text.lower())
        positive = sum(token in self.POSITIVE_TERMS for token in tokens)
        negative = sum(token in self.NEGATIVE_TERMS for token in tokens)
        if rating is not None:
            sentiment = "positive" if rating >= 4 else "negative" if rating <= 2 else "neutral"
        else:
            sentiment = "positive" if positive > negative else "negative" if negative > positive else "neutral"
        lowered = clean_text.lower()
        topics = [
            topic for topic, terms in self.TOPICS.items()
            if any(term in lowered for term in terms)
        ]
        return {
            "sentiment": sentiment,
            "sentiment_method": "rating_label" if rating is not None else "keyword_heuristic",
            "positive_cue_count": positive,
            "negative_cue_count": negative,
            "topics": topics or ["general"],
            "text": clean_text,
            "synthetic": synthetic,
        }

    def scrub_pii(self, text: str) -> str:
        no_emails = self.EMAIL_PATTERN.sub("[EMAIL_REDACTED]", text)
        return self.PHONE_PATTERN.sub("[PHONE_REDACTED]", no_emails)

    def summarize_dataset(self, limit: int = 5000) -> dict[str, Any]:
        if not self.dataset_path.is_file():
            return {"available": False, "path": str(self.dataset_path), "review_count": 0}
        modified_at = self.dataset_path.stat().st_mtime_ns
        if self._summary_cache is not None and self._summary_mtime_ns == modified_at and limit >= 5000:
            return dict(self._summary_cache)
        sentiment_counts: Counter[str] = Counter()
        topic_counts: Counter[str] = Counter()
        rating_counts: Counter[int] = Counter()
        count = 0
        with self.dataset_path.open("r", encoding="utf-8") as stream:
            for line in stream:
                if count >= limit:
                    break
                if not line.strip():
                    continue
                review = json.loads(line)
                analysis = self.analyze_text(
                    review["text"],
                    int(review["rating"]),
                    synthetic=review.get("synthetic") is True,
                )
                count += 1
                sentiment_counts[analysis["sentiment"]] += 1
                rating_counts[int(review["rating"])] += 1
                topic_counts.update(analysis["topics"])
        summary = {
            "available": True,
            "product": "Royal Enfield",
            "dataset": "synthetic_demo_reviews",
            "synthetic": True,
            "model_used": False,
            "review_count": count,
            "sentiment_counts": dict(sentiment_counts),
            "rating_counts": dict(sorted(rating_counts.items())),
            "topic_counts": dict(topic_counts.most_common()),
            "path": str(self.dataset_path),
            "warning": "Generated demo data only; these are not real customer reviews.",
        }
        if limit >= 5000:
            self._summary_cache = summary
            self._summary_mtime_ns = modified_at
        return summary
