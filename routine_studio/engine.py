"""Standardized, persisted routine/task execution contract for local agents."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable

from .store import RoutineStore, utc_now

CONTRACT_VERSION = "1.0"
TaskHandler = Callable[[dict[str, Any], dict[str, Any]], Any | Awaitable[Any]]


class ExecutionContractError(ValueError):
    """Raised when an agent task violates the shared execution contract."""


@dataclass
class TaskResult:
    contract_version: str
    task_id: str
    status: str
    output: dict[str, Any]
    evidence: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    attempts: int = 1
    duration_ms: float = 0.0
    tokens_used: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_task_result(value: Any, task_id: str) -> TaskResult:
    if isinstance(value, TaskResult):
        result = value
    elif isinstance(value, dict):
        output = value.get("output", value)
        evidence = value.get("evidence", [])
        status = value.get("status", "completed")
        if not isinstance(output, dict):
            raise ExecutionContractError("Task output must be a JSON object")
        if not isinstance(evidence, list) or any(not isinstance(item, dict) for item in evidence):
            raise ExecutionContractError("Task evidence must be an array of JSON objects")
        result = TaskResult(
            contract_version=str(value.get("contract_version", CONTRACT_VERSION)),
            task_id=str(value.get("task_id", task_id)),
            status=str(status),
            output=output,
            evidence=evidence,
            error=value.get("error"),
            attempts=int(value.get("attempts", 1)),
            duration_ms=float(value.get("duration_ms", 0)),
            tokens_used=int(value["tokens_used"]) if value.get("tokens_used") is not None else None,
        )
    else:
        raise ExecutionContractError("Agent handler must return a TaskResult or JSON object")
    if result.contract_version != CONTRACT_VERSION:
        raise ExecutionContractError(f"Unsupported task contract version: {result.contract_version}")
    if result.task_id != task_id:
        raise ExecutionContractError("Task result id does not match the requested task")
    if result.status not in {"completed", "failed", "waiting_approval", "waiting_operator"}:
        raise ExecutionContractError(f"Unsupported task result status: {result.status}")
    try:
        json.dumps(result.to_dict(), ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise ExecutionContractError(f"Task result is not JSON serializable: {exc}") from exc
    return result


def _get_path(context: dict[str, Any], path: str) -> Any:
    value: Any = context
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def evaluate_condition(condition: dict[str, Any] | None, context: dict[str, Any]) -> bool:
    if not condition:
        return True
    actual = _get_path(context, str(condition.get("field", "")))
    expected = condition.get("value")
    operator = condition.get("operator", "equals")
    if operator == "equals":
        return actual == expected
    if operator == "not_equals":
        return actual != expected
    if operator == "contains":
        return str(expected).casefold() in str(actual or "").casefold()
    if operator == "in":
        return actual in (expected if isinstance(expected, list) else [])
    if operator == "exists":
        return actual is not None
    raise ExecutionContractError(f"Unsupported condition operator: {operator}")


class RoutineExecutor:
    """Execute ordered routine nodes and persist every task input/output locally."""

    def __init__(self, store: RoutineStore) -> None:
        self.store = store

    async def execute(
        self,
        routine: dict[str, Any],
        initial_context: dict[str, Any],
        handlers: dict[str, TaskHandler],
        *,
        trigger: dict[str, Any] | None = None,
        selected_reason: str = "Operator requested routine run",
        incident_id: str | None = None,
    ) -> dict[str, Any]:
        routine = self.store.save_routine(routine)
        run = self.store.create_run(routine, trigger or {}, initial_context, selected_reason, incident_id)
        return await self._execute_nodes(routine, run, initial_context, handlers, 0)

    async def resume(
        self,
        run_id: str,
        handlers: dict[str, TaskHandler],
        *,
        approved: bool,
        operator_id: str = "local-operator",
    ) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if not run:
            raise KeyError("Routine run not found")
        if run["status"] not in {"waiting_approval", "waiting_operator"}:
            raise ValueError("Routine run is not waiting for an operator")
        routine = self.store.get_routine(run["routine_id"])
        if not routine:
            raise KeyError("Routine definition no longer exists")
        waiting = next((task for task in run["tasks"] if task["status"] in {"waiting_approval", "waiting_operator"}), None)
        if not waiting:
            raise ValueError("Approval checkpoint task is missing")
        decision = {"approved":bool(approved),"operator_id":operator_id,"decided_at":utc_now()}
        self.store.update_task(waiting["task_id"], status="completed" if approved else "rejected", output={"approval":decision}, evidence=[] , completed_at=utc_now())
        if not approved:
            self.store.finish_run(run_id,status="rejected",output={"approval":decision})
            return self.store.get_run(run_id) or run
        context = dict(run["input"])
        for task in run["tasks"]:
            if task["status"] == "completed":
                context.setdefault("tasks", {})[task["node_id"]] = task["output"]
        context.setdefault("tasks", {})[waiting["node_id"]] = {"approval":decision}
        return await self._execute_nodes(routine, run, context, handlers, waiting["ordinal"] + 1)

    async def _invoke(self, handler: TaskHandler, context: dict[str, Any], node: dict[str, Any]) -> Any:
        value = handler(context, node)
        return await value if inspect.isawaitable(value) else value

    async def _execute_one(
        self, run_id: str, node: dict[str, Any], ordinal: int, context: dict[str, Any], handlers: dict[str, TaskHandler]
    ) -> tuple[dict[str, Any], TaskResult]:
        task_id = self.store.create_task(run_id,node,ordinal,context)
        started = time.perf_counter()
        started_at = utc_now()
        self.store.update_task(task_id,status="running",task_input=context,started_at=started_at)
        attempts = 0
        last_error = "Task handler is not registered"
        handler = handlers.get(str(node.get("operation") or node.get("tool_id") or node["type"]))
        if node["type"] == "decision":
            matched = evaluate_condition(node.get("condition"),context)
            result = TaskResult(CONTRACT_VERSION,task_id,"completed",{"condition_matched":matched,"selected_branch":"true" if matched else "false"})
        elif node["type"] == "approval":
            result = TaskResult(CONTRACT_VERSION,task_id,"waiting_approval",{"checkpoint":node.get("label",node["id"]),"requires_operator":True})
        elif node.get("requires_operator"):
            result = TaskResult(CONTRACT_VERSION,task_id,"waiting_operator",{"checkpoint":node.get("label",node["id"]),"requires_operator":True})
        elif handler is None:
            result = TaskResult(CONTRACT_VERSION,task_id,"failed",{},error=last_error)
        else:
            max_attempts = max(1,min(6,int(node.get("retries",0))+1))
            timeout = max(0.001,min(600.0,float(node.get("timeout_seconds",30))))
            result = TaskResult(CONTRACT_VERSION,task_id,"failed",{},error=last_error)
            for attempts in range(1,max_attempts+1):
                try:
                    value = await asyncio.wait_for(self._invoke(handler,context,node),timeout=timeout)
                    result = validate_task_result(value,task_id)
                    result.attempts = attempts
                    break
                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    result = TaskResult(CONTRACT_VERSION,task_id,"failed",{},error=last_error,attempts=attempts)
        duration = round((time.perf_counter()-started)*1000,3)
        result.duration_ms = duration
        completed_at = utc_now() if result.status not in {"waiting_approval", "waiting_operator"} else None
        self.store.update_task(task_id,status=result.status,output=result.output,evidence=result.evidence,error=result.error,attempt=result.attempts,completed_at=completed_at,duration_ms=duration,tokens_used=result.tokens_used)
        return {"task_id":task_id,"node":node},result

    def _record_skipped(self, run_id: str, node: dict[str, Any], ordinal: int, context: dict[str, Any]) -> None:
        task_id = self.store.create_task(run_id, node, ordinal, context)
        self.store.update_task(task_id, status="skipped", output={"reason":"Condition did not match"}, completed_at=utc_now())

    @staticmethod
    def _parallel_batch(nodes: list[dict[str, Any]], ordinal: int) -> list[dict[str, Any]]:
        node = nodes[ordinal]
        group = node.get("parallel_group")
        if not group or node["type"] in {"decision", "approval"}:
            return [node]
        batch = [node]
        cursor = ordinal + 1
        while cursor < len(nodes) and nodes[cursor].get("parallel_group") == group and nodes[cursor].get("type") not in {"decision", "approval"}:
            batch.append(nodes[cursor])
            cursor += 1
        return batch

    async def _execute_batch(
        self,
        run: dict[str, Any],
        batch: list[dict[str, Any]],
        ordinal: int,
        context: dict[str, Any],
        handlers: dict[str, TaskHandler],
    ) -> str | None:
        results = await asyncio.gather(*[
            self._execute_one(run["run_id"], node, ordinal + offset, context, handlers)
            for offset, node in enumerate(batch)
        ])
        for record, result in results:
            context["tasks"][str(record["node"]["id"])] = result.output
            if result.status in {"waiting_approval", "waiting_operator"}:
                return result.status
            if result.status == "failed":
                return "failed"
        return None

    async def _execute_nodes(
        self,
        routine: dict[str, Any],
        run: dict[str, Any],
        context: dict[str, Any],
        handlers: dict[str, TaskHandler],
        start_ordinal: int,
    ) -> dict[str, Any]:
        nodes = routine.get("nodes", [])
        output_context = dict(context)
        output_context.setdefault("tasks", {})
        ordinal = start_ordinal
        while ordinal < len(nodes):
            node = nodes[ordinal]
            if not evaluate_condition(node.get("condition"), output_context):
                self._record_skipped(run["run_id"],node,ordinal,output_context)
                ordinal += 1
                continue
            batch = self._parallel_batch(nodes,ordinal)
            terminal_status = await self._execute_batch(run,batch,ordinal,output_context,handlers)
            if terminal_status:
                self.store.finish_run(run["run_id"],status=terminal_status,output=output_context)
                return self.store.get_run(run["run_id"]) or run
            ordinal += len(batch)
        self.store.finish_run(run["run_id"], status="completed", output=output_context)
        return self.store.get_run(run["run_id"]) or run
