"""SQLite persistence for routine definitions, registries, execution runs and task outputs."""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
import uuid

from .catalog import AGENT_CATALOG, MICRO_AGENT_CATALOG, TOOL_CATALOG, default_routines


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def decode(value: str, fallback: Any = None) -> Any:
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return fallback


class RoutineStore:
    """Versionless local store; structured payloads allow compatible additive evolution."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path.resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._lock, self._connection() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS routines (
                    routine_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    definition_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS studio_agents (
                    agent_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS studio_micro_agents (
                    micro_agent_id TEXT PRIMARY KEY,
                    parent_agent_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    profile_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS studio_tools (
                    tool_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS routine_runs (
                    run_id TEXT PRIMARY KEY,
                    routine_id TEXT NOT NULL,
                    routine_name TEXT NOT NULL,
                    incident_id TEXT,
                    trigger_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    selected_reason TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    output_json TEXT NOT NULL DEFAULT '{}',
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    duration_ms REAL
                );
                CREATE INDEX IF NOT EXISTS idx_routine_runs_started ON routine_runs(started_at DESC);
                CREATE INDEX IF NOT EXISTS idx_routine_runs_incident ON routine_runs(incident_id, started_at DESC);
                CREATE TABLE IF NOT EXISTS routine_tasks (
                    task_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES routine_runs(run_id) ON DELETE CASCADE,
                    node_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    task_type TEXT NOT NULL,
                    agent_id TEXT,
                    micro_agent_id TEXT,
                    tool_id TEXT,
                    status TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    output_json TEXT NOT NULL DEFAULT '{}',
                    evidence_json TEXT NOT NULL DEFAULT '[]',
                    error_text TEXT,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT,
                    completed_at TEXT,
                    duration_ms REAL,
                    tokens_used INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_routine_tasks_run ON routine_tasks(run_id, ordinal);
                CREATE TABLE IF NOT EXISTS agent_memory (
                    memory_id TEXT PRIMARY KEY,
                    scope TEXT NOT NULL,
                    owner_id TEXT,
                    incident_id TEXT,
                    content TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_agent_memory_scope ON agent_memory(scope, created_at DESC);
                CREATE TABLE IF NOT EXISTS agent_evaluations (
                    evaluation_id TEXT PRIMARY KEY,
                    suite TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS studio_settings (
                    setting_key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS registry_executions (
                    execution_id TEXT PRIMARY KEY,
                    registry_kind TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    output_json TEXT NOT NULL DEFAULT '{}',
                    error_text TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    duration_ms REAL,
                    tokens_used INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_registry_exec_owner ON registry_executions(registry_kind,owner_id,started_at DESC);
            """)
            now = utc_now()
            for routine in default_routines():
                connection.execute(
                    "INSERT OR IGNORE INTO routines(routine_id,name,enabled,definition_json,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                    (routine["routine_id"], routine["name"], int(routine.get("enabled", True)), encode(routine), now, now),
                )
            for agent in AGENT_CATALOG:
                connection.execute(
                    "INSERT OR IGNORE INTO studio_agents(agent_id,name,config_json,updated_at) VALUES(?,?,?,?)",
                    (agent["agent_id"], agent["name"], encode(agent), now),
                )
            for profile in MICRO_AGENT_CATALOG:
                connection.execute(
                    "INSERT OR IGNORE INTO studio_micro_agents(micro_agent_id,parent_agent_id,name,profile_json,updated_at) VALUES(?,?,?,?,?)",
                    (profile["micro_agent_id"], profile["parent_agent_id"], profile["name"], encode(profile), now),
                )
            for tool in TOOL_CATALOG:
                connection.execute(
                    "INSERT OR IGNORE INTO studio_tools(tool_id,name,definition_json,updated_at) VALUES(?,?,?,?)",
                    (tool["tool_id"], tool["name"], encode(tool), now),
                )

    @staticmethod
    def _routine_row(row: sqlite3.Row) -> dict[str, Any]:
        return decode(row["definition_json"], {})

    def list_routines(self) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute("SELECT definition_json FROM routines ORDER BY name COLLATE NOCASE").fetchall()
        return [decode(row["definition_json"], {}) for row in rows]

    def get_routine(self, routine_id: str) -> dict[str, Any] | None:
        with self._connection() as connection:
            row = connection.execute("SELECT definition_json FROM routines WHERE routine_id=?", (routine_id,)).fetchone()
        return self._routine_row(row) if row else None

    def save_routine(self, definition: dict[str, Any]) -> dict[str, Any]:
        routine_id = str(definition.get("routine_id") or f"routine.{uuid.uuid4().hex[:12]}")
        name = str(definition.get("name") or "").strip()
        if not name:
            raise ValueError("Routine name is required")
        nodes = definition.get("nodes")
        if not isinstance(nodes, list):
            raise ValueError("Routine nodes must be an array")
        node_ids = [str(node.get("id", "")) for node in nodes if isinstance(node, dict)]
        if len(node_ids) != len(nodes) or any(not node_id for node_id in node_ids):
            raise ValueError("Each routine node needs an id")
        if len(set(node_ids)) != len(node_ids):
            raise ValueError("Routine node ids must be unique")
        allowed_types = {"agent", "micro_agent", "tool", "decision", "approval"}
        for node in nodes:
            if node.get("type") not in allowed_types:
                raise ValueError(f"Unsupported routine node type: {node.get('type')}")
            node["timeout_seconds"] = max(0, min(600, int(node.get("timeout_seconds", 30))))
            node["retries"] = max(0, min(5, int(node.get("retries", 0))))
        definition = {**definition, "routine_id": routine_id, "name": name, "enabled": bool(definition.get("enabled", True)), "nodes": nodes}
        now = utc_now()
        with self._connection() as connection:
            row = connection.execute("SELECT created_at FROM routines WHERE routine_id=?", (routine_id,)).fetchone()
            created_at = row["created_at"] if row else now
            connection.execute(
                "INSERT INTO routines(routine_id,name,enabled,definition_json,created_at,updated_at) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(routine_id) DO UPDATE SET name=excluded.name,enabled=excluded.enabled,definition_json=excluded.definition_json,updated_at=excluded.updated_at",
                (routine_id, name, int(definition["enabled"]), encode(definition), created_at, now),
            )
        return definition

    def duplicate_routine(self, routine_id: str) -> dict[str, Any]:
        source = self.get_routine(routine_id)
        if not source:
            raise KeyError("Routine not found")
        duplicate = decode(encode(source), {})
        duplicate["routine_id"] = f"routine.{uuid.uuid4().hex[:12]}"
        duplicate["name"] = f"{source['name']} copy"
        duplicate["enabled"] = False
        for index, node in enumerate(duplicate["nodes"]):
            node["id"] = f"{node.get('id', 'node')}-{index + 1}"
        return self.save_routine(duplicate)

    def set_routine_enabled(self, routine_id: str, enabled: bool) -> dict[str, Any] | None:
        routine = self.get_routine(routine_id)
        if not routine:
            return None
        routine["enabled"] = bool(enabled)
        return self.save_routine(routine)

    @staticmethod
    def _score_routine(routine: dict[str, Any], alert: dict[str, Any]) -> tuple[int, str]:
        trigger = routine.get("trigger", {})
        signal = alert.get("signal", {})
        service = str(alert.get("service") or signal.get("service") or "").casefold()
        signal_type = str(signal.get("type") or "").casefold()
        text = " ".join(str(signal.get(key, "")) for key in ("message", "error_type", "metric", "signature")).casefold()
        related_count = int(alert.get("related_actionable_signal_count", 1) or 1)
        service_matches = service in {str(value).casefold() for value in trigger.get("services", [])}
        keywords = [str(value).casefold() for value in trigger.get("keywords", [])]
        keyword_matches = [keyword for keyword in keywords if keyword in text]
        signal_matches = signal_type in {str(value).casefold() for value in trigger.get("signal_types", [])}
        minimum = int(trigger.get("minimum_related_signals", 1))
        related_matches = minimum > 1 and related_count >= minimum
        generic_signal_match = signal_matches and not keywords and not trigger.get("services")
        if not (service_matches or keyword_matches or related_matches or generic_signal_match):
            return 0, ""
        score = int(trigger.get("priority", 0))
        reasons = []
        if service_matches:
            score += 100
            reasons.append(f"service {service}")
        if keyword_matches and (not trigger.get("signal_types") or signal_matches):
            score += 110 + max(map(len, keyword_matches))
            reasons.append(f"keywords {', '.join(sorted(keyword_matches, key=len, reverse=True)[:3])}")
        if related_matches:
            score += 120 + related_count
            reasons.append(f"{related_count} related signals")
        if generic_signal_match:
            score += 10
            reasons.append(f"signal type {signal_type}")
        return score, "; ".join(reasons)

    def select_routine(self, alert: dict[str, Any]) -> tuple[dict[str, Any], str]:
        candidates = [routine for routine in self.list_routines() if routine.get("enabled", True)]
        ranked = [
            (score, routine, reason)
            for routine in candidates
            for score, reason in [self._score_routine(routine, alert)]
            if score > 0
        ]
        if ranked:
            ranked.sort(key=lambda item: (item[0], item[1]["routine_id"]), reverse=True)
            _, selected, reason = ranked[0]
            return selected, f"Matched {reason}"
        fallback = self.get_routine("routine.api_failure") or (candidates[0] if candidates else None)
        if fallback is None:
            raise ValueError("No enabled incident routine is available")
        return fallback, "Default fallback: generic actionable failure"

    def create_run(self, routine: dict[str, Any], trigger: dict[str, Any], context: dict[str, Any], selected_reason: str, incident_id: str | None = None) -> dict[str, Any]:
        run_id = f"run-{uuid.uuid4().hex[:16]}"
        row = {"run_id":run_id,"routine_id":routine["routine_id"],"routine_name":routine["name"],"incident_id":incident_id,"trigger":trigger,"status":"running","selected_reason":selected_reason,"input":context,"output":{},"started_at":utc_now(),"completed_at":None,"duration_ms":None}
        with self._connection() as connection:
            connection.execute("INSERT INTO routine_runs(run_id,routine_id,routine_name,incident_id,trigger_json,status,selected_reason,input_json,output_json,started_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(run_id,row["routine_id"],row["routine_name"],incident_id,encode(trigger),row["status"],selected_reason,encode(context),"{}",row["started_at"]))
        return row

    def create_task(self, run_id: str, node: dict[str, Any], ordinal: int, task_input: dict[str, Any]) -> str:
        task_id = f"task-{uuid.uuid4().hex[:16]}"
        with self._connection() as connection:
            connection.execute("INSERT INTO routine_tasks(task_id,run_id,node_id,ordinal,task_type,agent_id,micro_agent_id,tool_id,status,input_json) VALUES(?,?,?,?,?,?,?,?,?,?)",(task_id,run_id,str(node["id"]),ordinal,str(node["type"]),node.get("agent_id"),node.get("micro_agent_id"),node.get("tool_id"),"queued",encode(task_input)))
        return task_id

    def update_task(self, task_id: str, *, status: str, task_input: dict[str, Any] | None = None, output: Any = None, evidence: Any = None, error: str | None = None, attempt: int = 0, started_at: str | None = None, completed_at: str | None = None, duration_ms: float | None = None, tokens_used: int | None = None) -> None:
        with self._connection() as connection:
            connection.execute("UPDATE routine_tasks SET status=?,input_json=COALESCE(?,input_json),output_json=?,evidence_json=?,error_text=?,attempt=?,started_at=COALESCE(?,started_at),completed_at=?,duration_ms=?,tokens_used=? WHERE task_id=?",(status,encode(task_input) if task_input is not None else None,encode(output if output is not None else {}),encode(evidence if evidence is not None else []),error,attempt,started_at,completed_at,duration_ms,tokens_used,task_id))

    def finish_run(self, run_id: str, *, status: str, output: dict[str, Any], completed_at: str | None = None, duration_ms: float | None = None) -> None:
        completed_at = completed_at or utc_now()
        with self._connection() as connection:
            connection.execute("UPDATE routine_runs SET status=?,output_json=?,completed_at=?,duration_ms=? WHERE run_id=?",(status,encode(output),completed_at,duration_ms,run_id))

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM routine_runs WHERE run_id=?",(run_id,)).fetchone()
            tasks = connection.execute("SELECT * FROM routine_tasks WHERE run_id=? ORDER BY ordinal",(run_id,)).fetchall()
        if not row:
            return None
        result = dict(row)
        result["trigger"] = decode(result.pop("trigger_json"), {})
        result["input"] = decode(result.pop("input_json"), {})
        result["output"] = decode(result.pop("output_json"), {})
        result["tasks"] = []
        for task in tasks:
            item = dict(task)
            item["input"] = decode(item.pop("input_json"), {})
            item["output"] = decode(item.pop("output_json"), {})
            item["evidence"] = decode(item.pop("evidence_json"), [])
            result["tasks"].append(item)
        return result

    def list_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute("SELECT run_id FROM routine_runs ORDER BY started_at DESC LIMIT ?",(max(1,min(500,limit)),)).fetchall()
        return [run for row in rows if (run := self.get_run(row["run_id"])) is not None]

    def latest_incident_run(self, incident_id: str) -> dict[str, Any] | None:
        with self._connection() as connection:
            row = connection.execute("SELECT run_id FROM routine_runs WHERE incident_id=? ORDER BY started_at DESC LIMIT 1", (incident_id,)).fetchone()
        return self.get_run(row["run_id"]) if row else None

    def delete_incident_data(self, incident_id: str) -> None:
        with self._connection() as connection:
            run_ids = [row[0] for row in connection.execute(
                "SELECT run_id FROM routine_runs WHERE incident_id=?", (incident_id,)
            ).fetchall()]
            for run_id in run_ids:
                connection.execute("DELETE FROM routine_tasks WHERE run_id=?", (run_id,))
            connection.execute("DELETE FROM routine_runs WHERE incident_id=?", (incident_id,))
            connection.execute("DELETE FROM agent_memory WHERE incident_id=?", (incident_id,))

    def list_registry(self, table: str) -> list[dict[str, Any]]:
        allowed = {"agents":"studio_agents","micro_agents":"studio_micro_agents","tools":"studio_tools"}
        if table not in allowed:
            raise ValueError("Unknown registry")
        column = {"agents":"config_json","micro_agents":"profile_json","tools":"definition_json"}[table]
        with self._connection() as connection:
            rows = connection.execute(f"SELECT {column} AS payload FROM {allowed[table]} ORDER BY 1").fetchall()
        return [decode(row["payload"], {}) for row in rows]

    def update_agent(self, agent_id: str, patch: dict[str, Any]) -> dict[str, Any] | None:
        with self._connection() as connection:
            row = connection.execute("SELECT config_json FROM studio_agents WHERE agent_id=?",(agent_id,)).fetchone()
            if not row:
                return None
            current = decode(row["config_json"], {})
            current.update({key:value for key,value in patch.items() if key in {"instructions","model","memory","status","tool_permissions","temperature","system_instructions","task_instructions"}})
            connection.execute("UPDATE studio_agents SET config_json=?,updated_at=? WHERE agent_id=?",(encode(current),utc_now(),agent_id))
        return current

    def save_memory(self, scope: str, content: str, *, owner_id: str | None = None, incident_id: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        if scope not in {"incident_working","historical_incident","knowledge","agent_execution"}:
            raise ValueError("Unsupported memory scope")
        row={"memory_id":f"mem-{uuid.uuid4().hex[:16]}","scope":scope,"owner_id":owner_id,"incident_id":incident_id,"content":content[:10000],"metadata":metadata or {},"created_at":utc_now()}
        with self._connection() as connection:
            connection.execute("INSERT INTO agent_memory(memory_id,scope,owner_id,incident_id,content,metadata_json,created_at) VALUES(?,?,?,?,?,?,?)",(row["memory_id"],scope,owner_id,incident_id,row["content"],encode(row["metadata"]),row["created_at"]))
        return row

    def search_memory(self, query: str = "", *, scope: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        clauses=[]; values=[]
        if scope:
            clauses.append("scope=?"); values.append(scope)
        terms=[term.casefold() for term in query.split() if term]
        with self._connection() as connection:
            rows=connection.execute("SELECT * FROM agent_memory"+(" WHERE "+" AND ".join(clauses) if clauses else "")+" ORDER BY created_at DESC LIMIT 500",values).fetchall()
        matches=[]
        for row in rows:
            text=row["content"].casefold()
            score=sum(term in text for term in terms)
            if not terms or score:
                item=dict(row); item["metadata"]=decode(item.pop("metadata_json"),{}); item["relevance"] = score
                matches.append(item)
        matches.sort(key=lambda item:(item["relevance"],item["created_at"]),reverse=True)
        return matches[:max(1,min(limit,100))]

    def get_setting(self, key: str, default: Any = None) -> Any:
        with self._connection() as connection:
            row=connection.execute("SELECT value_json FROM studio_settings WHERE setting_key=?",(key,)).fetchone()
        return decode(row["value_json"],default) if row else default

    def set_setting(self, key: str, value: Any) -> Any:
        with self._connection() as connection:
            connection.execute("INSERT INTO studio_settings(setting_key,value_json,updated_at) VALUES(?,?,?) ON CONFLICT(setting_key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",(key,encode(value),utc_now()))
        return value

    def create_evaluation(self, suite: str, result: dict[str, Any], status: str = "completed") -> dict[str, Any]:
        row={"evaluation_id":f"eval-{uuid.uuid4().hex[:16]}","suite":suite,"status":status,"result":result,"created_at":utc_now()}
        with self._connection() as connection:
            connection.execute("INSERT INTO agent_evaluations(evaluation_id,suite,status,result_json,created_at) VALUES(?,?,?,?,?)",(row["evaluation_id"],suite,status,encode(result),row["created_at"]))
        return row

    def list_evaluations(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows=connection.execute("SELECT * FROM agent_evaluations ORDER BY created_at DESC LIMIT ?",(max(1,min(200,limit)),)).fetchall()
        return [{"evaluation_id":row["evaluation_id"],"suite":row["suite"],"status":row["status"],"result":decode(row["result_json"],{}),"created_at":row["created_at"]} for row in rows]

    def record_registry_execution(self, registry_kind: str, owner_id: str, task_input: dict[str, Any], *, status: str, output: Any = None, error: str | None = None, started_at: str | None = None, duration_ms: float | None = None, tokens_used: int | None = None) -> dict[str, Any]:
        row={"execution_id":f"exec-{uuid.uuid4().hex[:16]}","registry_kind":registry_kind,"owner_id":owner_id,"status":status,"input":task_input,"output":output if output is not None else {},"error":error,"started_at":started_at or utc_now(),"completed_at":utc_now(),"duration_ms":duration_ms,"tokens_used":tokens_used}
        with self._connection() as connection:
            connection.execute("INSERT INTO registry_executions(execution_id,registry_kind,owner_id,status,input_json,output_json,error_text,started_at,completed_at,duration_ms,tokens_used) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(row["execution_id"],registry_kind,owner_id,status,encode(task_input),encode(row["output"]),error,row["started_at"],row["completed_at"],duration_ms,tokens_used))
        return row

    def list_registry_executions(self, registry_kind: str | None = None, owner_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        clauses=[]; values=[]
        if registry_kind:
            clauses.append("registry_kind=?"); values.append(registry_kind)
        if owner_id:
            clauses.append("owner_id=?"); values.append(owner_id)
        query="SELECT * FROM registry_executions"+(" WHERE "+" AND ".join(clauses) if clauses else "")+" ORDER BY started_at DESC LIMIT ?"
        values.append(max(1,min(500,limit)))
        with self._connection() as connection:
            rows=connection.execute(query,values).fetchall()
        return [{"execution_id":row["execution_id"],"registry_kind":row["registry_kind"],"owner_id":row["owner_id"],"status":row["status"],"input":decode(row["input_json"],{}),"output":decode(row["output_json"],{}),"error":row["error_text"],"started_at":row["started_at"],"completed_at":row["completed_at"],"duration_ms":row["duration_ms"],"tokens_used":row["tokens_used"]} for row in rows]
