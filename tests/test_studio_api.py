import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import app as application
from routine_studio import RoutineExecutor, RoutineStore


class StudioApiTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = RoutineStore(Path(self.temp_dir.name) / "studio.sqlite3")
        self.store_patch = patch.object(application, "routine_store", self.store)
        self.executor_patch = patch.object(application, "routine_executor", RoutineExecutor(self.store))
        self.store_patch.start()
        self.executor_patch.start()
        self.client = TestClient(application.app)

    def tearDown(self):
        self.client.close()
        self.executor_patch.stop()
        self.store_patch.stop()
        self.temp_dir.cleanup()

    def test_metrics_tool_resolves_async_handler_and_records_history(self):
        response = self.client.post("/api/studio/tools/tool.get_metrics/execute", json={"input": {}})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "completed")
        self.assertIn("simulator", response.json()["output"])
        self.assertEqual(len(self.store.list_registry_executions("tool")), 1)

    def test_normalization_micro_agent_and_unknown_agent(self):
        response = self.client.post("/api/studio/micro-agents/MA-10/execute", json={
            "input": {"event": {"service.name": "api-gateway", "message": "API test", "log.level": "info"}}
        })
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["output"]["service.name"], "api-gateway")
        missing = self.client.post("/api/studio/micro-agents/MA-unknown/execute", json={"input": {}})
        self.assertEqual(missing.status_code, 404)

    def test_simulation_tool_requires_confirmation(self):
        response = self.client.post("/api/studio/tools/tool.simulate_recovery/execute", json={"input": {}})
        self.assertEqual(response.status_code, 409)

    def test_routine_update_duplicate_toggle_and_execution_history(self):
        routine = self.store.get_routine("routine.api_failure")
        routine["description"] = "API regression test"
        updated = self.client.put("/api/studio/routines/routine.api_failure", json=routine)
        self.assertEqual(updated.status_code, 200, updated.text)
        self.assertEqual(self.store.get_routine(routine["routine_id"])["description"], "API regression test")
        duplicate = self.client.post("/api/studio/routines/routine.api_failure/duplicate").json()
        self.assertFalse(duplicate["enabled"])
        disabled = self.client.post(f"/api/studio/routines/{duplicate['routine_id']}/runs", json={})
        self.assertEqual(disabled.status_code, 409)
        routine["nodes"] = [{"id": "metrics", "type": "tool", "operation": "get_metrics"}]
        self.client.put("/api/studio/routines/routine.api_failure", json=routine)
        run = self.client.post("/api/studio/routines/routine.api_failure/runs", json={})
        self.assertEqual(run.status_code, 200, run.text)
        self.assertEqual(run.json()["status"], "completed")
        detail = self.client.get(f"/api/studio/runs/{run.json()['run_id']}")
        self.assertEqual(len(detail.json()["tasks"]), 1)

    def test_evaluation_persists_measured_results(self):
        response = self.client.post("/api/studio/evaluations/run")
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()["result"]
        self.assertEqual(result["passed"], 6)
        self.assertEqual(result["metrics"]["task_completion"]["total"], 1)
        self.assertEqual(result["metrics"]["tool_execution_success"]["passed"], 1)
        self.assertEqual(result["metrics"]["llm_response_validity"]["status"], "not_run")
        self.assertEqual(len(self.store.list_evaluations()), 1)


if __name__ == "__main__":
    unittest.main()