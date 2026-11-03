"""Generate a reproducible 1,000-row Royal Enfield demo-review JSONL dataset."""

from __future__ import annotations

import json
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any


OPENERS = (
    "I have ridden this motorcycle for several weeks.",
    "After a few commutes and weekend rides,",
    "This is my first month with the bike, and",
    "On my regular city and highway routes,",
    "After taking a couple of longer trips,",
    "I use this motorcycle for everyday travel,",
    "Having compared it with other bikes,",
    "My experience so far has been mixed, but",
    "For relaxed weekend rides,",
    "During my daily commute,",
)

EXPERIENCES = (
    "the engine feels smooth and the classic design gets noticed.",
    "the riding position is comfortable, though the bike feels heavy in traffic.",
    "the brakes feel predictable and the controls are easy to understand.",
    "the service visit took longer than expected, but the staff explained the work.",
    "fuel economy is lower than I hoped for the price.",
    "the seat could be more comfortable on a long ride.",
    "the bike has a steady feel on open roads and a relaxed character.",
    "I noticed some vibration at higher speeds and would like a follow-up check.",
    "the finish looks good, but I had to wait for a spare part.",
    "the motorcycle is enjoyable when I take my time and ride smoothly.",
)

ENDINGS = (
    "Overall, it suits the kind of riding I do.",
    "I would like to see the service experience improve.",
    "It has strengths and a few practical compromises.",
    "My rating reflects this specific demo experience.",
    "I am still learning what works best for this bike.",
    "This review is generated sample data for testing.",
    "The result may differ for another rider or model.",
    "I would check the latest specifications before buying.",
    "I am not describing a verified customer or real purchase.",
    "It gives the demo dashboard a realistic-looking test comment.",
)

MODELS = ("Classic 350", "Hunter 350", "Meteor 350", "Himalayan", "Bullet 350")
RATING_WEIGHTS = (8, 12, 20, 32, 28)
START_DATE = date(2025, 1, 1)


def build_reviews(count: int = 1000, seed: int = 20261028) -> list[dict[str, Any]]:
    if count < 0:
        raise ValueError("count must not be negative")
    rng = random.Random(seed)
    reviews: list[dict[str, Any]] = []
    for index in range(1, count + 1):
        rating = rng.choices((1, 2, 3, 4, 5), weights=RATING_WEIGHTS, k=1)[0]
        text = " ".join((
            OPENERS[(index - 1) % len(OPENERS)],
            EXPERIENCES[((index - 1) // len(OPENERS)) % len(EXPERIENCES)],
            ENDINGS[((index - 1) // (len(OPENERS) * len(EXPERIENCES))) % len(ENDINGS)],
        ))
        reviews.append({
            "review_id": f"SYN-RE-{index:05d}",
            "product": "Royal Enfield",
            "model": rng.choice(MODELS),
            "rating": rating,
            "text": text,
            "created_at": (START_DATE + timedelta(days=rng.randrange(365))).isoformat(),
            "source": "synthetic_demo",
            "synthetic": True,
            "ground_truth_sentiment": "positive" if rating >= 4 else "negative" if rating <= 2 else "neutral",
        })
    return reviews


def write_reviews(path: Path, count: int = 1000, seed: int = 20261028) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    reviews = build_reviews(count, seed)
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        for review in reviews:
            stream.write(json.dumps(review, ensure_ascii=False) + "\n")
    temporary.replace(path)
    return len(reviews)


def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    dataset = project_root / "data" / "feedback" / "royal_enfield_reviews.jsonl"
    count = write_reviews(dataset)
    print(f"Wrote {count} synthetic Royal Enfield reviews to {dataset}")


if __name__ == "__main__":
    main()
