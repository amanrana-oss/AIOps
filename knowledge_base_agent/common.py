"""Shared types and deterministic helpers for AG-02 microagents."""

from __future__ import annotations

import hashlib
import math
import re
from datetime import datetime, timezone

TOKEN_PATTERN = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{1,}")
SUPPORTED_EXTENSIONS = {".txt", ".md", ".rst", ".json", ".yaml", ".yml", ".csv", ".pdf", ".docx"}
EMBEDDING_DIMENSIONS = 384
MAX_DOCUMENT_BYTES = 20 * 1024 * 1024
MAX_METADATA_BYTES = 64 * 1024
MAX_CHUNKS_PER_DOCUMENT = 10_000
MAX_BACKFILL_BYTES = 50 * 1024 * 1024
MAX_BACKFILL_RECORDS = 100_000


class KnowledgeBaseError(RuntimeError):
    """A configured knowledge store or ingestion operation failed."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def hash_embedding(text: str) -> list[float]:
    """Create deterministic local lexical vectors without downloading a model."""
    tokens = [token.lower() for token in TOKEN_PATTERN.findall(text)]
    features = tokens + [f"{left}_{right}" for left, right in zip(tokens, tokens[1:])]
    vector = [0.0] * EMBEDDING_DIMENSIONS
    for feature in features:
        digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest()
        number = int.from_bytes(digest, "big")
        index = number % EMBEDDING_DIMENSIONS
        sign = 1.0 if (number >> 8) & 1 else -1.0
        vector[index] += sign
    magnitude = math.sqrt(sum(value * value for value in vector))
    if magnitude:
        vector = [value / magnitude for value in vector]
    return vector
