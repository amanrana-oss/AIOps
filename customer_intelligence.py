"""Local customer feedback intake; external sources remain unconfigured."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterator, Literal
from urllib.parse import parse_qs, urlparse

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator
from customer_ai import customer_ai


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class FeedbackInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    source: Literal["manual_import", "synthetic", "google_play", "app_store", "bikewale", "bikedekho", "zigwheels"] = "manual_import"
    external_id: str = Field(min_length=1, max_length=200)
    original_content: str = Field(min_length=3, max_length=10000)
    rating: float | None = Field(default=None, ge=1, le=5)
    product: str = Field(default="Flying Flea", min_length=1, max_length=100)
    feedback_scope: Literal["mobile_app", "product_experience"] = "mobile_app"
    app_id: str | None = Field(default=None, max_length=100)
    source_url: str | None = Field(default=None, max_length=1000)
    created_at: str | None = Field(default=None, max_length=100)
    country: str | None = Field(default=None, max_length=10)
    experience_type: Literal["unverified", "owner_report", "public_opinion", "prelaunch_opinion"] = "unverified"
    published_date_display: str | None = Field(default=None, max_length=100)
    quality_note: str | None = Field(default=None, max_length=1000)

    @model_validator(mode="after")
    def validate_ev_source(self) -> FeedbackInput:
        website_paths = {
            "bikewale": ("www.bikewale.com", "/royalenfield-bikes/flying-flea/reviews/"),
            "bikedekho": ("www.bikedekho.com", "/royal-enfield/flying-flea-c6/reviews"),
            "zigwheels": ("www.zigwheels.com", "/royal-enfield-bikes/flying-flea-c6/user-reviews"),
        }
        if self.source in website_paths:
            host, path = website_paths[self.source]
            parsed = urlparse(self.source_url or "")
            if (parsed.scheme != "https" or parsed.netloc != host
                    or not (parsed.path.rstrip("/") == path.rstrip("/") or parsed.path.startswith(path.rstrip("/") + "/"))
                    or self.product != "Flying Flea" or self.feedback_scope != "product_experience" or self.app_id is not None):
                raise ValueError("Website feedback requires the allowed Flying Flea EV product review page and product_experience scope")
        identifiers = {"google_play": "com.flyingflea.ffapp", "app_store": "6745692820"}
        urls = {
            "google_play": "https://play.google.com/store/apps/details?id=com.flyingflea.ffapp",
            "app_store": "https://apps.apple.com/in/app/flying-flea-app/id6745692820",
        }
        if self.source in identifiers:
            if self.app_id != identifiers[self.source] or self.product != "Flying Flea":
                raise ValueError("Only the verified Flying Flea EV customer app is allowed; general Royal Enfield and mechanic apps are excluded")
            parsed = urlparse(self.source_url or "")
            expected = urlparse(urls[self.source])
            valid_url = parsed.scheme == "https" and parsed.netloc == expected.netloc and parsed.path == expected.path
            if self.source == "google_play":
                valid_url = valid_url and parse_qs(parsed.query).get("id") == [identifiers[self.source]]
            if not valid_url:
                raise ValueError("A source URL for the verified Flying Flea app is required")
        return self


class FeedbackBatch(BaseModel):
    records: list[FeedbackInput] = Field(min_length=1, max_length=200)


class ApprovalDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    note: str = Field(default="", max_length=1000)


class SatisfactionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    resolved: bool
    followup: str = Field(default="", max_length=5000)


class FeedbackAnalysis(BaseModel):
    sentiment: Literal["positive", "negative", "neutral", "mixed", "insufficient_evidence"]
    topic: str
    language: str
    requires_investigation: bool
    analysis_status: str = "requires_review"
    model_version: str = "deterministic_keyword_fallback_v1"
    cause_confirmed: Literal[False] = False
    evidence: list[str]
    intent: str = "unknown"
    ai_error: str | None = None


TOPICS = {
    "mobile_app": ("mobile app", "application", " app "),
    "battery_status": ("battery", "percentage", "charge level"),
    "charging": ("charging", "charger"),
    "vehicle_range": ("real-world range", "riding range", "km/charge", "range"),
    "login_authentication": ("login", "log in", "otp", "sign in", "passcode", "password", "bike pin", "primary pin", "digital pin", "change pin", "pin change"),
    "connectivity": ("connect", "bluetooth", "pairing"),
    "service_experience": ("dealer", "service", "booking"),
    "product_design": ("design", "comfort", "seat"),
    "safety_concern": ("fire", "smoke", "brake", "unsafe"),
}
NEGATIVE = ("not", "stuck", "fail", "problem", "issue", "poor", "broken", "crash", "delay", "stop", "hang", "disconnect", "unavailable", "unsafe", "freez", "unresponsive", "malfunction", "unresolved")
POSITIVE = ("great", "excellent", "love", "good", "smooth", "happy", "awesome", "amazing", "fantastic", "premium")


def classify_feedback(record: FeedbackInput, scrub: Callable[[str], str]) -> FeedbackAnalysis:
    text = scrub(record.original_content).casefold().replace("\u2019", "'")
    feature_request = is_feature_request(text)
    pin_request = bool(re.search(r"\bpin\b", text)) and bool(re.search(r"\b(change|changing|reset|forgot|unlock)\b", text))
    failure = bool(re.search(r"\b(couldn't|couldnt|can't|cant|cannot|doesn't|doesnt|isn't|isnt|won't|wont|didn't|didnt|unable)\b", text))
    question = bool(re.search(r"\b(how|what|where|when|why|can|could|do|does|is|are|which|who)\b", text) and ("?" in text or re.search(r"^\s*(how|what|where|when|why|can|could|do|does|is|are|which|who)\b", text)))
    negative = any(re.search(r"\b" + re.escape(term) + (r"\b" if term == "not" else r"\w*\b"), text) for term in NEGATIVE) or (record.rating is not None and record.rating <= 2)
    negative = negative or failure
    positive = any(term in text for term in POSITIVE) or (record.rating is not None and record.rating >= 4)
    topic = next((name for name, terms in TOPICS.items() if any(term in text for term in terms)), "unknown")
    if re.search(r"\b(app|application)\b", text) and topic == "unknown":
        topic = "mobile_app"
    if feature_request:
        topic = "mobile_app"
    if pin_request:
        topic = "login_authentication"
    if any(term in text for term in TOPICS["safety_concern"]):
        topic = "safety_concern"
        negative = True
    non_ascii = any(ord(character) > 127 for character in text)
    language = "unknown_or_mixed" if non_ascii else "unverified_latin_script"
    sentiment = "insufficient_evidence"
    if negative and positive:
        sentiment = "mixed"
    elif negative:
        sentiment = "negative"
    elif positive:
        sentiment = "positive"
    opinion_only = record.experience_type in {"public_opinion", "prelaunch_opinion"}
    intent = "feature_request" if feature_request else "complaint" if negative else "question" if question or pin_request else "praise" if positive else "unknown"
    return FeedbackAnalysis(sentiment=sentiment, topic=topic, language=language,
                            requires_investigation=(negative or question or pin_request or feature_request) and (topic != "unknown" or record.source == "manual_import" or question) and not opinion_only,
                            intent=intent,
                            evidence=["Keyword/rating fallback only; multilingual understanding and technical causality are not verified."])


def is_feature_request(text: str) -> bool:
    normalized = text.casefold().replace("\u2019", "'")
    unavailable_action = re.search(
        r"\b(?:can't|cannot|unable to|doesn't let(?: you)?|does not let(?: you)?)\b.{0,80}\b(?:adjust|change|create|add|control|customi[sz]e)\b",
        normalized,
    )
    feature_area = re.search(r"\b(?:mode|profile|feature|setting|function|option)s?\b", normalized)
    return bool(unavailable_action and feature_area)


class FeedbackStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS feedback_records (
                    internal_id TEXT PRIMARY KEY, source TEXT NOT NULL, external_id TEXT NOT NULL,
                    digest TEXT NOT NULL, original_json TEXT NOT NULL, collected_at TEXT NOT NULL,
                    UNIQUE(source, external_id), UNIQUE(source, digest));
                CREATE TABLE IF NOT EXISTS feedback_analysis (
                    internal_id TEXT PRIMARY KEY, analysis_json TEXT NOT NULL, incident_id TEXT,
                    FOREIGN KEY(internal_id) REFERENCES feedback_records(internal_id));
                CREATE TABLE IF NOT EXISTS feedback_responses (
                    internal_id TEXT PRIMARY KEY, response_json TEXT NOT NULL,
                    satisfaction INTEGER, followup TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL,
                    FOREIGN KEY(internal_id) REFERENCES feedback_records(internal_id));
                CREATE TABLE IF NOT EXISTS feedback_jobs (
                    job_id TEXT PRIMARY KEY, status TEXT NOT NULL, payload_json TEXT NOT NULL,
                    result_json TEXT, error TEXT, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS feedback_tickets (
                    approval_id TEXT PRIMARY KEY, status TEXT NOT NULL, snapshot_json TEXT NOT NULL,
                    ticket_json TEXT, approver TEXT, decided_at TEXT, note TEXT);
                CREATE TABLE IF NOT EXISTS feedback_audit_events (
                    event_id TEXT PRIMARY KEY, approval_id TEXT NOT NULL, action TEXT NOT NULL,
                    operator TEXT NOT NULL, recorded_at TEXT NOT NULL);
            """)
            connection.execute("UPDATE feedback_jobs SET status='queued', error='Recovered after process interruption' WHERE status='running'")

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def submit(self, batch: FeedbackBatch) -> str:
        identifier = f"CXJOB-{uuid.uuid4().hex[:16]}"
        with self.connection() as connection:
            count = connection.execute("SELECT count(*) FROM feedback_jobs WHERE status IN ('queued','running')").fetchone()[0]
            if count >= 20:
                raise HTTPException(429, "Feedback queue is full; retry later")
            connection.execute("INSERT INTO feedback_jobs VALUES (?,?,?,NULL,NULL,?)",
                               (identifier, "queued", batch.model_dump_json(), now()))
        return identifier

    def job(self, identifier: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute("SELECT job_id,status,result_json,error,created_at FROM feedback_jobs WHERE job_id=?", (identifier,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["result"] = json.loads(result.pop("result_json") or "null")
        return result

    def retain(self, record: FeedbackInput) -> tuple[str, bool]:
        digest = hashlib.sha256(" ".join(record.original_content.casefold().split()).encode()).hexdigest()
        identifier = f"FB-{uuid.uuid4().hex[:16]}"
        with self.connection() as connection:
            row = connection.execute("SELECT internal_id FROM feedback_records WHERE source=? AND (external_id=? OR digest=?)", (record.source, record.external_id, digest)).fetchone()
            if row:
                return row[0], False
            connection.execute("INSERT INTO feedback_records VALUES (?,?,?,?,?,?)",
                               (identifier, record.source, record.external_id, digest, record.model_dump_json(), now()))
        return identifier, True

    def save_analysis(self, identifier: str, analysis: FeedbackAnalysis, incident_id: str | None) -> None:
        with self.connection() as connection:
            connection.execute("INSERT OR REPLACE INTO feedback_analysis VALUES (?,?,?)", (identifier, analysis.model_dump_json(), incident_id))

    @staticmethod
    def _remove_feedback_from_jobs(connection: sqlite3.Connection, feedback_ids: set[str],
                                   source_keys: set[tuple[str, str]]) -> None:
        jobs = connection.execute("SELECT job_id,payload_json,result_json FROM feedback_jobs").fetchall()
        for job in jobs:
            payload = json.loads(job["payload_json"] or "{}")
            records = payload.get("records", [])
            retained_records = [record for record in records
                                if (record.get("source"), record.get("external_id")) not in source_keys]
            result = json.loads(job["result_json"]) if job["result_json"] else None
            if isinstance(result, dict):
                result["records"] = [item for item in result.get("records", [])
                                     if item.get("feedback_id") not in feedback_ids]
            if len(retained_records) == len(records):
                continue
            if retained_records:
                payload["records"] = retained_records
                connection.execute(
                    "UPDATE feedback_jobs SET payload_json=?,result_json=? WHERE job_id=?",
                    (json.dumps(payload, ensure_ascii=False),
                     json.dumps(result, ensure_ascii=False) if result is not None else None, job["job_id"]),
                )
            else:
                connection.execute("DELETE FROM feedback_jobs WHERE job_id=?", (job["job_id"],))

    def delete_investigation_data(self, incident_id: str, linked_feedback_ids: set[str]) -> None:
        with self.connection() as connection:
            feedback_rows = connection.execute(
                "SELECT internal_id,source,external_id FROM feedback_records"
            ).fetchall()
            linked_feedback_ids.update(row[0] for row in connection.execute(
                "SELECT internal_id FROM feedback_analysis WHERE incident_id=?", (incident_id,)
            ).fetchall())
            linked_rows = [row for row in feedback_rows if row["internal_id"] in linked_feedback_ids]
            source_keys = {(row["source"], row["external_id"]) for row in linked_rows}
            self._remove_feedback_from_jobs(connection, linked_feedback_ids, source_keys)
            for feedback_id in linked_feedback_ids:
                connection.execute("DELETE FROM feedback_responses WHERE internal_id=?", (feedback_id,))
                connection.execute("DELETE FROM feedback_analysis WHERE internal_id=?", (feedback_id,))
                connection.execute("DELETE FROM feedback_records WHERE internal_id=?", (feedback_id,))
            connection.execute("DELETE FROM feedback_audit_events WHERE approval_id=?", (incident_id,))
            connection.execute("DELETE FROM feedback_tickets WHERE approval_id=?", (incident_id,))

    def analysis(self, identifier: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute("SELECT analysis_json,incident_id FROM feedback_analysis WHERE internal_id=?", (identifier,)).fetchone()
        return {"analysis": json.loads(row[0]), "incident_id": row[1]} if row else None

    def save_response(self, identifier: str, response: dict[str, Any],
                      resolved: bool | None = None, followup: str | None = None) -> None:
        with self.connection() as connection:
            previous = connection.execute("SELECT satisfaction,followup FROM feedback_responses WHERE internal_id=?", (identifier,)).fetchone()
            satisfaction = int(resolved) if resolved is not None else (previous[0] if previous else None)
            saved_followup = followup if followup is not None else (previous[1] if previous else "")
            connection.execute("INSERT OR REPLACE INTO feedback_responses VALUES (?,?,?,?,?)",
                               (identifier, json.dumps(response), satisfaction, saved_followup, now()))

    def response(self, identifier: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute("SELECT response_json,satisfaction,followup,updated_at FROM feedback_responses WHERE internal_id=?", (identifier,)).fetchone()
        if not row:
            return None
        return {"response": json.loads(row[0]), "satisfied": None if row[1] is None else bool(row[1]),
                "followup": row[2], "updated_at": row[3]}

    def list_records(self, limit: int, offset: int, dataset: str = "all") -> dict[str, Any]:
        where = {"all": "", "real": " WHERE source != 'synthetic'", "synthetic": " WHERE source = 'synthetic'"}[dataset]
        with self.connection() as connection:
            total = connection.execute("SELECT count(*) FROM feedback_records" + where).fetchone()[0]
            rows = connection.execute("SELECT * FROM feedback_records" + where + " ORDER BY collected_at DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
        return {"total": total, "items": [{**json.loads(row["original_json"]), "internal_id": row["internal_id"],
                                          "collected_at": row["collected_at"], **(self.analysis(row["internal_id"]) or {}),
                                          "incident_id": (self.analysis(row["internal_id"]) or {}).get("incident_id"),
                                          **({"customer_response": self.response(row["internal_id"])} if self.response(row["internal_id"]) else {})} for row in rows]}

    def propose(self, incident: dict[str, Any]) -> None:
        with self.connection() as connection:
            connection.execute("INSERT OR IGNORE INTO feedback_tickets VALUES (?, 'PENDING_APPROVAL', ?, NULL, NULL, NULL, NULL)",
                               (incident["incident_id"], json.dumps(incident)))

    def proposals(self) -> dict[str, Any]:
        with self.connection() as connection:
            rows = connection.execute("SELECT * FROM feedback_tickets ORDER BY rowid DESC LIMIT 100").fetchall()
        return {"items": [{"approval_id": row["approval_id"], "status": row["status"],
                           "approved_snapshot": json.loads(row["snapshot_json"]),
                           "ticket": json.loads(row["ticket_json"] or "null"), "approver": row["approver"],
                           "decided_at": row["decided_at"], "note": row["note"]} for row in rows]}

    def decide(self, identifier: str, approved: bool, operator: str, note: str,
               ticket_builder: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]) -> dict[str, Any]:
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM feedback_tickets WHERE approval_id=?", (identifier,)).fetchone()
            if not row:
                raise HTTPException(404, "Ticket proposal not found")
            if row["status"] == "CREATED" and approved:
                return {"status": "CREATED", "ticket": json.loads(row["ticket_json"]), "idempotent": True}
            if row["status"] != "PENDING_APPROVAL":
                raise HTTPException(409, "Proposal already decided")
            if not approved and not note:
                raise HTTPException(422, "Rejection reason is required")
            snapshot = json.loads(row["snapshot_json"])
            ticket = ticket_builder(snapshot, snapshot["poc"]["rca"]) if approved else None
            status = "CREATED" if approved else "REJECTED"
            connection.execute("UPDATE feedback_tickets SET status=?,ticket_json=?,approver=?,decided_at=?,note=? WHERE approval_id=?",
                               (status, json.dumps(ticket) if ticket else None, operator, now(), note, identifier))
            for action in (["APPROVED", "CREATED"] if approved else ["REJECTED"]):
                connection.execute("INSERT INTO feedback_audit_events VALUES (?,?,?,?,?)", (uuid.uuid4().hex, identifier, action, operator, now()))
            return {"status": status, "ticket": ticket, "idempotent": False}


class CustomerIntelligence:
    def __init__(self, store: FeedbackStore, scrub: Callable[[str], str],
                 investigate: Callable[[str, FeedbackInput, FeedbackAnalysis], Awaitable[dict[str, Any]]],
                 on_decision: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
                 answer_question: Callable[[FeedbackInput], Awaitable[dict[str, Any]]] | None = None) -> None:
        self.store = store
        self.scrub = scrub
        self.investigate = investigate
        self.on_decision = on_decision
        self.answer_question = answer_question

    async def process(self, batch: FeedbackBatch) -> dict[str, Any]:
        results = []
        for record in batch.records:
            identifier, created = self.store.retain(record)
            previous = self.store.analysis(identifier)
            analysis = classify_feedback(record, self.scrub)
            if customer_ai.status()["enabled"]:
                try:
                    understood = await customer_ai.understand(self.scrub(record.original_content), record.rating)
                    opinion = record.experience_type in {"public_opinion", "prelaunch_opinion"}
                    analysis = FeedbackAnalysis(sentiment=understood.sentiment, topic=understood.topic,
                                                language=understood.language, intent=understood.intent,
                                                requires_investigation=(understood.requires_investigation or understood.intent == "question" or (record.source == "manual_import" and understood.sentiment in {"negative", "mixed"})) and not opinion and understood.intent != "irrelevant",
                                                evidence=understood.evidence, analysis_status="ai_analyzed_requires_review",
                                                model_version="huggingface:" + customer_ai.status()["model"])
                except Exception as exc:
                    analysis.ai_error = str(exc) if isinstance(exc, ValueError) else "AI analysis timed out or failed"
                    analysis.analysis_status = "ai_failed_fallback_requires_review"
            if previous and (previous.get("incident_id") or not analysis.requires_investigation):
                results.append({"feedback_id": identifier, "duplicate": True, **previous})
                continue
            incident = await self.investigate(identifier, record, analysis) if analysis.requires_investigation else None
            incident_id = incident.get("incident_id") if incident else None
            self.store.save_analysis(identifier, analysis, incident_id)
            results.append({"feedback_id": identifier, "duplicate": not created,
                            "analysis": analysis.model_dump(), "incident_id": incident_id})
        return {"records": results}

    async def run(self) -> None:
        while True:
            with self.store.connection() as connection:
                row = connection.execute("SELECT job_id,payload_json FROM feedback_jobs WHERE status='queued' ORDER BY created_at LIMIT 1").fetchone()
                if row:
                    connection.execute("UPDATE feedback_jobs SET status='running' WHERE job_id=?", (row[0],))
            if not row:
                await asyncio.sleep(0.5)
                continue
            try:
                result = await asyncio.wait_for(self.process(FeedbackBatch.model_validate_json(row[1])), timeout=180)
                with self.store.connection() as connection:
                    connection.execute("UPDATE feedback_jobs SET status='completed',result_json=?,error=NULL WHERE job_id=?", (json.dumps(result), row[0]))
            except asyncio.CancelledError:
                with self.store.connection() as connection:
                    connection.execute("UPDATE feedback_jobs SET status='queued' WHERE job_id=?", (row[0],))
                raise
            except Exception as exc:
                with self.store.connection() as connection:
                    connection.execute("UPDATE feedback_jobs SET status='failed',error=? WHERE job_id=?", (f"Processing failed ({type(exc).__name__}); retry after reviewing configuration", row[0]))

    def router(self) -> APIRouter:
        router = APIRouter()

        @router.get("/api/customer-intelligence/ai")
        async def ai_status() -> dict[str, Any]:
            return customer_ai.status()

        @router.get("/api/customer-intelligence/approvals")
        async def proposals() -> dict[str, Any]:
            return self.store.proposals()

        @router.post("/api/customer-intelligence/approvals/{approval_id}/{decision}")
        async def decide(approval_id: str, decision: Literal["approve", "reject"], payload: ApprovalDecision, request: Request) -> dict[str, Any]:
            token = os.environ.get("CX_OPERATOR_TOKEN", "")
            if not token:
                raise HTTPException(503, "Customer approvals are disabled until CX_OPERATOR_TOKEN is configured on the server")
            supplied = request.headers.get("authorization", "")
            if not secrets.compare_digest(supplied.encode(), f"Bearer {token}".encode()):
                raise HTTPException(403, "Authorized operator credentials required")
            from poc_agents import TicketingAgent

            result = self.store.decide(approval_id, decision == "approve", os.environ.get("CX_OPERATOR_ID", "local-cx-operator"),
                                       payload.note, TicketingAgent().create_draft)
            if self.on_decision:
                await self.on_decision(approval_id, result)
            return result

        @router.post("/api/feedback/import", status_code=202)
        async def import_feedback(batch: FeedbackBatch) -> dict[str, Any]:
            return {"job_id": self.store.submit(batch), "status": "queued"}

        @router.post("/api/feedback/{feedback_id}/reanalyze", status_code=202)
        async def reanalyze_feedback(feedback_id: str) -> dict[str, Any]:
            with self.store.connection() as connection:
                row = connection.execute("SELECT original_json FROM feedback_records WHERE internal_id=?", (feedback_id,)).fetchone()
            if not row:
                raise HTTPException(404, "Feedback not found")
            previous = self.store.analysis(feedback_id)
            if previous and previous.get("incident_id"):
                raise HTTPException(409, "Feedback already has an investigation; rerun that investigation instead")
            batch = FeedbackBatch(records=[FeedbackInput.model_validate_json(row["original_json"])])
            return {"job_id": self.store.submit(batch), "status": "queued", "feedback_id": feedback_id}

        @router.post("/api/customer-intelligence/feedback/{feedback_id}/satisfaction")
        async def satisfaction(feedback_id: str, payload: SatisfactionInput) -> dict[str, Any]:
            with self.store.connection() as connection:
                row = connection.execute("SELECT original_json FROM feedback_records WHERE internal_id=?", (feedback_id,)).fetchone()
            if not row:
                raise HTTPException(404, "Feedback not found")
            existing = self.store.response(feedback_id)
            if not existing:
                raise HTTPException(409, "No customer answer is available to rate")
            if not payload.resolved and len(payload.followup) < 3:
                raise HTTPException(422, "Please add details about what remains unresolved")
            self.store.save_response(feedback_id, existing["response"], payload.resolved, payload.followup)
            result: dict[str, Any] = {"feedback_id": feedback_id, "satisfied": payload.resolved}
            if not payload.resolved:
                original = FeedbackInput.model_validate_json(row["original_json"])
                followup = FeedbackInput(source="manual_import", external_id=f"followup-{uuid.uuid4().hex}",
                                         original_content=f"Unresolved customer issue: {original.original_content}\nAdditional details: {payload.followup}",
                                         product=original.product, feedback_scope=original.feedback_scope)
                result["job_id"] = self.store.submit(FeedbackBatch(records=[followup]))
                result["status"] = "followup_queued"
            else:
                result["status"] = "resolved"
            return result

        @router.post("/api/customer-intelligence/investigations/{incident_id}/satisfaction")
        async def investigation_satisfaction(incident_id: str, payload: SatisfactionInput) -> dict[str, Any]:
            with self.store.connection() as connection:
                rows = connection.execute("SELECT internal_id,original_json FROM feedback_records").fetchall()
            match = next(((row[0], FeedbackInput.model_validate_json(row[1])) for row in rows
                          if (self.store.analysis(row[0]) or {}).get("incident_id") == incident_id), None)
            if not match:
                raise HTTPException(404, "Feedback linked to this investigation was not found")
            feedback_id, original = match
            if not payload.resolved and len(payload.followup) < 3:
                raise HTTPException(422, "Please add details about what remains unresolved")
            self.store.save_response(feedback_id, {"status": "investigation_response", "incident_id": incident_id},
                                     payload.resolved, payload.followup)
            result: dict[str, Any] = {"feedback_id": feedback_id, "incident_id": incident_id,
                                      "satisfied": payload.resolved}
            if not payload.resolved:
                followup = FeedbackInput(source="manual_import", external_id=f"followup-{uuid.uuid4().hex}",
                                         original_content=f"Unresolved customer issue: {original.original_content}\nAdditional details: {payload.followup}",
                                         product=original.product, feedback_scope=original.feedback_scope)
                result["job_id"] = self.store.submit(FeedbackBatch(records=[followup]))
                result["status"] = "followup_queued"
            else:
                result["status"] = "resolved"
            return result

        @router.get("/api/customer-intelligence/jobs/{job_id}")
        async def get_job(job_id: str) -> dict[str, Any]:
            result = self.store.job(job_id)
            if result is None:
                raise HTTPException(404, "Feedback job not found")
            return result

        @router.get("/api/feedback")
        async def feedback(limit: int = Query(25, ge=1, le=100), offset: int = Query(0, ge=0), dataset: Literal["all", "real", "synthetic"] = "all") -> dict[str, Any]:
            return self.store.list_records(limit, offset, dataset)

        @router.get("/api/feedback/sources")
        async def sources() -> dict[str, Any]:
            with self.store.connection() as connection:
                counts = dict(connection.execute("SELECT source,count(*) FROM feedback_records GROUP BY source").fetchall())
            return {"items": [{"source": source, "status": "public_snapshot_imported" if counts.get(source) else {"bikedekho": "permission_required", "zigwheels": "permission_required"}.get(source, "unconfigured"),
                               "stored_reviews": counts.get(source, 0), "live_collection": False,
                               "app_id": {"google_play": "com.flyingflea.ffapp", "app_store": "6745692820"}.get(source)}
                              for source in ("google_play", "app_store", "bikewale", "bikedekho", "zigwheels", "instagram", "facebook", "youtube", "x")]}

        @router.get("/api/customer-intelligence/overview")
        async def overview(dataset: Literal["all", "real", "synthetic"] = "all") -> dict[str, Any]:
            where = {"all": "", "real": " WHERE r.source != 'synthetic'", "synthetic": " WHERE r.source = 'synthetic'"}[dataset]
            with self.store.connection() as connection:
                rows = connection.execute("SELECT r.original_json,a.analysis_json,a.incident_id FROM feedback_records r LEFT JOIN feedback_analysis a USING(internal_id)" + where).fetchall()
                synthetic = connection.execute("SELECT count(*) FROM feedback_records WHERE source='synthetic'").fetchone()[0]
            sentiment_counts: dict[str, int] = {}
            ratings = []
            for row in rows:
                analysis = json.loads(row["analysis_json"] or "{}")
                sentiment = analysis.get("sentiment", "pending")
                sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
                rating = json.loads(row["original_json"]).get("rating")
                if rating is not None:
                    ratings.append(rating)
            return {"total_feedback": len(rows), "synthetic_feedback": synthetic,
                    "investigations": len({row["incident_id"] for row in rows if row["incident_id"]}),
                    "sentiment_counts": sentiment_counts, "average_rating": round(sum(ratings)/len(ratings), 2) if ratings else None,
                    "dataset": dataset, "live_sources": 0, "analysis_method": "review-required keyword/rating fallback; no semantic model"}

        @router.post("/api/customer-intelligence/demo", status_code=202)
        async def demo() -> dict[str, Any]:
            samples = [
                ("Battery percentage is stuck and not refreshing", 1),
                ("Login fails and OTP does not arrive", 1),
                ("Bluetooth pairing fails to connect", 2),
                ("Charging is not starting", 1),
                ("Dealer service booking is delayed", 2),
                ("Love the design, great experience", 5),
            ]
            batch = FeedbackBatch(records=[FeedbackInput(source="synthetic", external_id=f"demo-{index}", original_content=text, rating=rating)
                                           for index, (text, rating) in enumerate(samples)])
            return {"job_id": self.store.submit(batch), "status": "queued", "synthetic": True}

        return router