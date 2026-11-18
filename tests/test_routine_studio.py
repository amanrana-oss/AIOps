import asyncio
import tempfile
import unittest
from pathlib import Path

from routine_studio import ExecutionContractError, RoutineExecutor, RoutineStore, validate_task_result


class RoutineStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = RoutineStore(Path(self.temp_dir.name) / "studio.sqlite3")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_seed_routines_and_registries_are_persisted(self):
        routines = self.store.list_routines()
        self.assertEqual(len(routines), 7)
        self.assertTrue(self.store.get_routine("routine.database_failure")["enabled"])
        self.assertGreaterEqual(len(self.store.list_registry("agents")), 6)
        self.assertGreaterEqual(len(self.store.list_registry("micro_agents")), 20)
        self.assertGreaterEqual(len(self.store.list_registry("tools")), 8)

    def test_routine_crud_duplicate_and_toggle(self):
        routine = self.store.get_routine("routine.api_failure")
        routine["name"] = "Custom API routine"
        saved = self.store.save_routine(routine)
        self.assertEqual(self.store.get_routine(saved["routine_id"])["name"], "Custom API routine")
        disabled = self.store.set_routine_enabled(saved["routine_id"], False)
        self.assertFalse(disabled["enabled"])
        duplicate = self.store.duplicate_routine(saved["routine_id"])
        self.assertNotEqual(duplicate["routine_id"], saved["routine_id"])
        self.assertFalse(duplicate["enabled"])
        self.assertEqual(duplicate["name"], "Custom API routine copy")

    def test_deterministic_selector_uses_service_signal_and_fallback(self):
        routine, reason = self.store.select_routine({"service":"postgresql","signal":{"type":"log_failure","message":"query timed out"}})
        self.assertEqual(routine["routine_id"], "routine.database_failure")
        self.assertIn("service postgresql", reason)
        routine, _ = self.store.select_routine({"service":"custom-api","signal":{"type":"baseline_anomaly","message":"latency degraded"}})
        self.assertEqual(routine["routine_id"], "routine.performance_degradation")
        routine, reason = self.store.select_routine({"service":"unknown","signal":{"type":"log_failure","message":"unclassified issue"}})
        self.assertEqual(routine["routine_id"], "routine.api_failure")
        self.assertIn("fallback", reason)

    def test_seeded_routine_selector_distinguishes_all_incident_classes(self):
        fixtures = [
            ({"service":"postgresql","signal":{"type":"log_failure","message":"query timeout"}},"routine.database_failure"),
            ({"service":"api-gateway","signal":{"type":"log_failure","message":"HTTP 503"}},"routine.api_failure"),
            ({"service":"vehicle-telemetry-api","signal":{"type":"threshold_anomaly","metric":"latency_ms","message":"latency threshold exceeded"}},"routine.performance_degradation"),
            ({"service":"kubernetes","signal":{"type":"threshold_anomaly","message":"memory pressure"}},"routine.infrastructure_failure"),
            ({"service":"api-gateway","signal":{"type":"log_failure","message":"deployment regression after release"}},"routine.deployment_regression"),
            ({"service":"api-gateway","related_actionable_signal_count":3,"signal":{"type":"log_failure","message":"cascade upstream failure"}},"routine.cascading_failure"),
        ]

        for alert, expected in fixtures:
            with self.subTest(expected=expected):
                selected, _ = self.store.select_routine(alert)
                self.assertEqual(selected["routine_id"], expected)


class RoutineExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = RoutineStore(Path(self.temp_dir.name) / "studio.sqlite3")
        self.executor = RoutineExecutor(self.store)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_task_contract_rejects_non_object_and_non_serializable_output(self):
        with self.assertRaises(ExecutionContractError):
            validate_task_result({"output": [1, 2]}, "task-a")
        with self.assertRaises(ExecutionContractError):
            validate_task_result({"output": {"bad": object()}}, "task-a")

    def test_sequential_routine_persists_inputs_outputs_and_history(self):
        routine = {"routine_id":"test.sequential","name":"Sequential","enabled":True,"nodes":[
            {"id":"first","type":"agent","operation":"first","timeout_seconds":2,"retries":0},
            {"id":"second","type":"micro_agent","operation":"second","timeout_seconds":2,"retries":0},
        ]}
        async def first(context, node):
            return {"output":{"value":context["seed"]+1},"evidence":[{"id":"event-1"}]}
        async def second(context, node):
            return {"output":{"value":context["tasks"]["first"]["value"]+1}}

        run = asyncio.run(self.executor.execute(routine,{"seed":1},{"first":first,"second":second}))

        self.assertEqual(run["status"],"completed")
        self.assertEqual(run["tasks"][0]["output"],{"value":2})
        self.assertEqual(run["tasks"][0]["evidence"],[{"id":"event-1"}])
        self.assertEqual(run["tasks"][1]["output"],{"value":3})
        self.assertEqual(self.store.list_runs()[0]["run_id"],run["run_id"])

    def test_retries_then_completes_and_condition_can_skip_node(self):
        routine = {"routine_id":"test.retry","name":"Retry","enabled":True,"nodes":[
            {"id":"flaky","type":"agent","operation":"flaky","timeout_seconds":2,"retries":1},
            {"id":"conditional","type":"tool","operation":"never","condition":{"field":"tasks.flaky.ok","operator":"equals","value":False}},
        ]}
        attempts = {"count":0}
        async def flaky(context,node):
            attempts["count"]+=1
            if attempts["count"]==1:
                raise RuntimeError("transient")
            return {"output":{"ok":True}}
        async def never(context,node):
            raise AssertionError("Condition should skip this node")

        run = asyncio.run(self.executor.execute(routine,{}, {"flaky":flaky,"never":never}))

        self.assertEqual(run["status"],"completed")
        self.assertEqual(run["tasks"][0]["attempt"],2)
        self.assertEqual(run["tasks"][1]["status"],"skipped")

    def test_approval_pauses_and_resume_continues(self):
        routine = {"routine_id":"test.approval","name":"Approval","enabled":True,"nodes":[
            {"id":"plan","type":"agent","operation":"plan","timeout_seconds":2},
            {"id":"approval","type":"approval","operation":"approval_checkpoint"},
            {"id":"finish","type":"tool","operation":"finish","timeout_seconds":2},
        ]}
        async def plan(context,node):
            return {"output":{"ready":True}}
        async def finish(context,node):
            return {"output":{"approved":context["tasks"]["approval"]["approval"]["approved"]}}
        handlers={"plan":plan,"finish":finish}

        waiting=asyncio.run(self.executor.execute(routine,{},handlers))
        self.assertEqual(waiting["status"],"waiting_approval")
        completed=asyncio.run(self.executor.resume(waiting["run_id"],handlers,approved=True))
        self.assertEqual(completed["status"],"completed")
        self.assertEqual(completed["tasks"][-1]["output"],{"approved":True})

    def test_parallel_group_runs_both_tasks(self):
        routine={"routine_id":"test.parallel","name":"Parallel","enabled":True,"nodes":[
            {"id":"left","type":"tool","operation":"left","parallel_group":"g","timeout_seconds":2},
            {"id":"right","type":"tool","operation":"right","parallel_group":"g","timeout_seconds":2},
        ]}
        async def handler(context,node):
            return {"output":{"node":node["id"]}}
        run=asyncio.run(self.executor.execute(routine,{}, {"left":handler,"right":handler}))
        self.assertEqual(run["status"],"completed")
        self.assertEqual([task["output"]["node"] for task in run["tasks"]],["left","right"])


if __name__ == "__main__":
    unittest.main()
