"""MA-03: source-document extraction, chunking, and metadata preparation."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .common import (
    MAX_CHUNKS_PER_DOCUMENT, MAX_DOCUMENT_BYTES, MAX_METADATA_BYTES, SUPPORTED_EXTENSIONS,
    KnowledgeBaseError, utc_now,
)


class DocumentIngestionMicroAgent:
    """MA-03: read, normalize, chunk, and prepare local knowledge documents."""

    def __init__(self, source_root: Path, chunk_words: int = 220, overlap_words: int = 35) -> None:
        self.source_root = source_root.resolve()
        self.chunk_words = chunk_words
        self.overlap_words = overlap_words

    def _extract(self, path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix in {".txt", ".md", ".rst", ".yaml", ".yml", ".csv"}:
            return path.read_text(encoding="utf-8-sig")
        if suffix == ".json":
            value = json.loads(path.read_text(encoding="utf-8-sig"))
            return json.dumps(value, ensure_ascii=False, indent=2)
        if suffix == ".pdf":
            from pypdf import PdfReader

            return "\n\n".join(page.extract_text() or "" for page in PdfReader(str(path)).pages)
        if suffix == ".docx":
            import docx2txt

            return docx2txt.process(str(path))
        raise KnowledgeBaseError(f"Unsupported document type: {suffix or '(no extension)'}")

    def _chunks(self, text: str) -> list[str]:
        stride = max(1, self.chunk_words - self.overlap_words)
        window: list[str] = []
        chunks: list[str] = []
        for match in re.finditer(r"\S+", text):
            window.append(match.group())
            if len(window) == self.chunk_words:
                chunks.append(" ".join(window))
                if len(chunks) > MAX_CHUNKS_PER_DOCUMENT:
                    raise KnowledgeBaseError("document has too many chunks")
                window = window[stride:]
        if window and (not chunks or len(window) > self.overlap_words):
            chunks.append(" ".join(window))
            if len(chunks) > MAX_CHUNKS_PER_DOCUMENT:
                raise KnowledgeBaseError("document has too many chunks")
        return chunks

    def _document_metadata(self, path: Path) -> dict[str, str]:
        sidecar = path.with_name(f"{path.name}.meta.json")
        if not sidecar.exists():
            return {}
        if sidecar.stat().st_size > MAX_METADATA_BYTES:
            raise KnowledgeBaseError("metadata sidecar exceeds 64 KB")
        values = json.loads(sidecar.read_text(encoding="utf-8-sig"))
        if not isinstance(values, dict):
            raise KnowledgeBaseError("metadata sidecar must contain a JSON object")
        allowed = {"effective_until", "revision", "topic"}
        metadata: dict[str, str] = {}
        for key in allowed:
            value = values.get(key)
            if value is not None:
                if not isinstance(value, (str, int, float, bool)):
                    raise KnowledgeBaseError(f"metadata field {key} must be a scalar value")
                metadata[key] = str(value)
        return metadata

    def prepare(self) -> dict[str, Any]:
        self.source_root.mkdir(parents=True, exist_ok=True)
        documents: list[dict[str, Any]] = []
        skipped: list[dict[str, str]] = []
        file_count = 0
        for path in sorted(self.source_root.rglob("*")):
            if not path.is_file() or path.name.startswith("."):
                continue
            if any(part.startswith(".") for part in path.relative_to(self.source_root).parts):
                continue
            if path.name.endswith(".meta.json"):
                continue
            if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                skipped.append({"file": str(path.relative_to(self.source_root)), "reason": "unsupported file type"})
                continue
            try:
                if path.stat().st_size > MAX_DOCUMENT_BYTES:
                    skipped.append({"file": str(path.relative_to(self.source_root)), "reason": "file exceeds 20 MB"})
                    continue
                text = self._extract(path).strip()
                if not text:
                    skipped.append({"file": str(path.relative_to(self.source_root)), "reason": "no extractable text"})
                    continue
                document_metadata = self._document_metadata(path)
                relative_path = path.relative_to(self.source_root).as_posix()
                content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
                chunks = self._chunks(text)
                if len(chunks) > MAX_CHUNKS_PER_DOCUMENT:
                    skipped.append({"file": relative_path, "reason": "document has too many chunks"})
                    continue
                file_count += 1
                review_status = (
                    "approved"
                    if path.parent.name == "approved"
                    and path.parent.parent.name == "incident-learning"
                    else "unreviewed"
                )
                for chunk_index, chunk in enumerate(chunks):
                    chunk_id = hashlib.sha256(
                        f"{relative_path}:{content_hash}:{chunk_index}".encode("utf-8")
                    ).hexdigest()
                    documents.append({
                        "id": chunk_id,
                        "text": chunk,
                        "metadata": {
                            **document_metadata,
                            "source_path": relative_path,
                            "source_name": path.name,
                            "title": path.stem,
                            "source_sha256": content_hash,
                            "chunk_index": chunk_index,
                            "document_type": path.suffix.lower().lstrip("."),
                            "ingested_at": utc_now(),
                            "review_status": review_status,
                        },
                    })
            except (OSError, UnicodeError, json.JSONDecodeError, ImportError, KnowledgeBaseError) as exc:
                skipped.append({"file": str(path.relative_to(self.source_root)), "reason": str(exc)})
        return {"documents": documents, "files_read": file_count, "skipped": skipped}
