import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from customer_intelligence import CustomerIntelligence, FeedbackBatch, FeedbackInput, FeedbackStore, classify_feedback
from monitoring_agent.ma14_event_detection import EventDetectionMicroAgent
from orchestrator_agent import OrchestratorAgent
from routine_studio import RoutineExecutor, RoutineStore

from knowledge_base_agent.ma03_document_ingestion import DocumentIngestionMicroAgent


class TrackerKnowledgeTests(unittest.TestCase):
    def test_user_supplied_bikedekho_text_is_opinion_only_and_deduplicated(self):
        path = Path(__file__).parents[1] / "data" / "feedback" / "flying_flea_bikedekho_user_supplied.json"
        batch = FeedbackBatch.model_validate_json(path.read_text(encoding="utf-8"))
        self.assertEqual(len(batch.records), 31)
        with tempfile.TemporaryDirectory() as directory:
            store = FeedbackStore(Path(directory) / "feedback.sqlite3")
            for record in batch.records:
                self.assertIn(record.experience_type, {"public_opinion", "prelaunch_opinion"})
                self.assertFalse(classify_feedback(record, lambda text: text).requires_investigation)
                self.assertIn("User-supplied", record.quality_note)
                self.assertNotIn("Royal Enfield 350", record.original_content)
                self.assertTrue(store.retain(record)[1])
                self.assertFalse(store.retain(record)[1])
            self.assertEqual(store.list_records(100, 0)["total"], 31)
        self.assertEqual(next(record for record in batch.records if record.created_at == "2024-11-09").rating, 2.2)

    def test_selected_bikewale_snapshot_contains_two_range_reports(self):
        path = Path(__file__).parents[1] / "data" / "feedback" / "flying_flea_bikewale_public.json"
        batch = FeedbackBatch.model_validate_json(path.read_text(encoding="utf-8"))
        self.assertEqual(len(batch.records), 6)
        self.assertEqual(len({record.external_id for record in batch.records}), 6)
        complaints = []
        for record in batch.records:
            self.assertIsNone(record.rating)
            self.assertTrue(record.quality_note)
            analysis = classify_feedback(record, lambda text: text)
            if analysis.requires_investigation:
                complaints.append(record.external_id)
                self.assertEqual(analysis.topic, "vehicle_range")
        self.assertEqual(set(complaints), {"1197540", "1149892"})

    def test_product_site_scope_and_fractional_ratings(self):
        from pydantic import ValidationError

        record = FeedbackInput(source="bikedekho", external_id="opinion-1", original_content="Battery is not efficient; design needs improvement",
                               source_url="https://www.bikedekho.com/royal-enfield/flying-flea-c6/reviews", feedback_scope="product_experience",
                               rating=2.2, experience_type="prelaunch_opinion")
        self.assertEqual(record.rating, 2.2)
        self.assertFalse(classify_feedback(record, lambda text: text).requires_investigation)
        with self.assertRaises(ValidationError):
            FeedbackInput(source="bikewale", external_id="ice", original_content="Classic 350 review",
                          source_url="https://www.bikewale.com/royalenfield-bikes/classic-350/reviews/", feedback_scope="product_experience")

    def test_observed_flying_flea_reviews_are_real_positive_and_not_incidents(self):
        path = Path(__file__).parents[1] / "data" / "feedback" / "flying_flea_google_play_public.json"
        batch = FeedbackBatch.model_validate_json(path.read_text(encoding="utf-8"))
        self.assertEqual(len(batch.records), 3)
        self.assertEqual(len({record.external_id for record in batch.records}), 3)
        for record in batch.records:
            self.assertEqual(record.source, "google_play")
            analysis = classify_feedback(record, lambda text: text)
            self.assertEqual(analysis.sentiment, "positive")
            self.assertFalse(analysis.requires_investigation)
            self.assertNotIn("user_name", record.model_dump())

    def test_neutral_support_question_is_classified_for_answering(self):
        record = FeedbackInput(external_id="pin-help", original_content="How do I change my PIN?")
        analysis = classify_feedback(record, lambda text: text)
        self.assertEqual(analysis.sentiment, "insufficient_evidence")
        self.assertEqual(analysis.intent, "question")
        self.assertTrue(analysis.requires_investigation)

    def test_stopped_working_complaint_is_classified_for_investigation(self):
        analysis = classify_feedback(
            FeedbackInput(external_id="stopped-app", original_content="My app stopped working"),
            lambda text: text,
        )
        self.assertEqual(analysis.sentiment, "negative")
        self.assertTrue(analysis.requires_investigation)

    def test_app_store_missing_features_are_not_misclassified_as_charging_failure(self):
        analysis = classify_feedback(FeedbackInput(
            source="app_store", external_id="feature-request",
            original_content="Good UI, lacks functionality. I can't adjust modes, create profiles, or change charging mode.",
            source_url="https://apps.apple.com/in/app/flying-flea-app/id6745692820?see-all=reviews&platform=iphone",
            app_id="6745692820", rating=2,
        ), lambda text: text)
        self.assertEqual(analysis.topic, "mobile_app")
        self.assertEqual(analysis.intent, "feature_request")
        self.assertTrue(analysis.requires_investigation)

    def test_only_verified_ev_store_reviews_are_accepted(self):
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            FeedbackInput(source="app_store", app_id="1472963660", external_id="ice", original_content="General Royal Enfield review")
        review = FeedbackInput(source="google_play", app_id="com.flyingflea.ffapp", external_id="real-review",
                               original_content="Great EV app", source_url="https://play.google.com/store/apps/details?id=com.flyingflea.ffapp")
        self.assertEqual(review.product, "Flying Flea")
        with self.assertRaises(ValidationError):
            FeedbackInput(source="google_play", app_id="com.flyingflea.ffapp", external_id="bad-url",
                          original_content="Not the EV app", source_url="https://play.google.com/store/apps/details?id=com.flyingflea.ffapp.ice")

    def test_tracker_rows_have_citations_without_identifier_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "issues.csv").write_text(
                'Issue Description,Reported by,VIN,Resolution Details\nBattery not refreshing,Private Name,PRIVATEVIN,Investigating\n',
                encoding="utf-8",
            )
            result = DocumentIngestionMicroAgent(root).prepare_issue_trackers(root)
            self.assertEqual(result["files_read"], 1)
            self.assertEqual(len(result["documents"]), 1)
            document = result["documents"][0]
            self.assertNotIn("Private Name", document["text"])
            self.assertNotIn("PRIVATEVIN", document["text"])
            self.assertEqual(document["metadata"]["source_row"], 2)
            self.assertEqual(document["metadata"]["review_status"], "unreviewed")


class CustomerPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import app as application

        self.application = application
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.store = FeedbackStore(root / "feedback.sqlite3")
        self.routines = RoutineStore(root / "routines.sqlite3")
        self.orchestrator = OrchestratorAgent(root / "incidents.sqlite3")
        self.service = CustomerIntelligence(self.store, application.monitoring_agent.feedback.scrub_pii, application._investigate_customer_feedback, application._customer_ticket_decided)
        self.patches = [
            patch.object(application.knowledge_base, "ingest_documents", return_value={"files_read": 0, "chunks_indexed": 0, "skipped": []}),
            patch.object(application, "customer_intelligence", self.service),
            patch.object(application, "orchestrator_agent", self.orchestrator),
            patch.object(application, "routine_store", self.routines),
            patch.object(application, "routine_executor", RoutineExecutor(self.routines)),
            patch.object(application.monitoring_agent, "events", EventDetectionMicroAgent()),
            patch.object(application.knowledge_base, "search_documents", return_value={"evidence": [{"id": "tracker-row-2", "text": "Battery status stale", "citation": {"source": "tracker.csv", "row": 2}, "review_required": True}]}),
            patch.object(application, "_ollama_hypothesis", return_value=(None, None, None)),
        ]
        for replacement in self.patches:
            replacement.start()

    async def asyncTearDown(self):
        for replacement in reversed(self.patches):
            replacement.stop()
        self.directory.cleanup()

    async def test_complaint_runs_every_specialist_without_creating_ticket(self):
        batch = FeedbackBatch(records=[FeedbackInput(source="synthetic", external_id="battery-1", original_content="Battery percentage is stuck and not refreshing", rating=1)])
        result = await self.service.process(batch)
        incident_id = result["records"][0]["incident_id"]
        incident = self.orchestrator.get_incident(incident_id)
        self.assertTrue(incident["evidence"]["customer_feedback"])
        self.assertEqual(incident["evidence"]["trace_events"], [])
        self.assertIsNone(incident["poc"]["ticket"])
        self.assertFalse(incident["poc"]["rca"]["cause_confirmed"])
        hypothesis = incident["poc"]["rca"]["hypotheses"][0]
        self.assertIn("tracker-row-2", hypothesis["valid_evidence_refs"])
        run = self.routines.get_run(incident["poc"]["routine_execution"]["run_id"])
        self.assertEqual(run["status"], "waiting_approval")
        self.assertEqual([task["node_id"] for task in run["tasks"]], ["monitor", "collect", "correlate", "hypothesis", "response", "preview", "approval"])
        self.assertTrue(all(task["status"] == "completed" for task in run["tasks"][:-1]))
        self.assertEqual(incident["poc"]["resolution"]["execution_mode"], "manual_review_only")
        self.assertEqual(self.store.proposals()["items"][0]["status"], "PENDING_APPROVAL")
        repeated = await self.service.process(batch)
        self.assertTrue(repeated["records"][0]["duplicate"])
        self.assertEqual(len(self.routines.list_runs()), 1)

    async def test_positive_review_does_not_create_incident(self):
        result = await self.service.process(FeedbackBatch(records=[FeedbackInput(external_id="positive", original_content="Love the design, great experience", rating=5)]))
        self.assertIsNone(result["records"][0]["incident_id"])
        self.assertEqual(len(self.orchestrator.list_incidents()), 0)

    async def test_unknown_manual_fault_runs_pipeline_without_inventing_solution(self):
        with patch.object(self.application.knowledge_base, "search_documents", return_value={"evidence": []}) as search:
            result = await self.service.process(FeedbackBatch(records=[FeedbackInput(external_id="unknown-fault", original_content="My bike is not working")]))
        record = result["records"][0]
        self.assertEqual(record["analysis"]["sentiment"], "negative")
        self.assertEqual(record["analysis"]["topic"], "unknown")
        incident = self.orchestrator.get_incident(record["incident_id"])
        search.assert_called()
        self.assertEqual(incident["poc"]["resolution"]["actions"][0]["answer_status"], "insufficient_knowledge")
        self.assertEqual(incident["poc"]["resolution"]["actions"][0]["knowledge_guidance"], [])
        run = self.routines.get_run(incident["poc"]["routine_execution"]["run_id"])
        self.assertEqual(run["status"], "waiting_approval")
        self.assertTrue(all(task["status"] == "completed" for task in run["tasks"][:-1]))
        self.assertIsNone(incident["poc"]["ticket"])
        self.assertEqual(self.store.proposals()["items"][0]["status"], "PENDING_APPROVAL")

    async def test_huggingface_understood_question_runs_customer_agent_routine(self):
        from customer_ai import Understanding, GroundedAnswer, AnswerStep, customer_ai

        understood = Understanding(sentiment="neutral", topic="battery_status", intent="question", language="English", requires_investigation=True, evidence=["Customer requests help with a frozen reading"])
        answer = GroundedAnswer(status="supported", summary="Review the documented procedure", steps=[AnswerStep(text="Check the cited reference with support.", evidence_ids=["manual-row-2"])])
        with patch.object(self.application.knowledge_base, "search_documents", return_value={"evidence": [
            {"id": "manual-row-2", "text": "Manual procedure for checking this reading.",
             "metadata": {"document_type": "pdf"}, "citation": {"source": "manual.pdf", "chunk": 2},
             "review_required": True},
        ]}), patch.object(customer_ai, "status", return_value={"enabled": True, "configured": True, "model": "mock-hf-model"}), patch.object(customer_ai, "understand", return_value=understood), patch.object(customer_ai, "answer", return_value=answer):
            result = await self.service.process(FeedbackBatch(records=[FeedbackInput(external_id="hf-question", original_content="Could you explain this frozen reading?")]))
        record = result["records"][0]
        self.assertEqual(record["analysis"]["intent"], "question")
        self.assertEqual(record["analysis"]["model_version"], "huggingface:mock-hf-model")
        incident = self.orchestrator.get_incident(record["incident_id"])
        self.assertEqual(incident["poc"]["resolution"]["ai_status"], "completed")
        self.assertIsNone(incident["poc"]["ticket"])
        self.assertEqual(self.store.proposals()["items"], [])

    async def test_supported_manual_question_does_not_create_ticket_proposal(self):
        manual = {"id": "manual-pin", "text": "Select Account, then Change Bike PIN. An OTP will be sent to your registered mobile number.",
                  "metadata": {"document_type": "pdf"}, "citation": {"source": "owner-manual.pdf", "chunk": 84},
                  "review_required": True}
        with patch.object(self.application.knowledge_base, "search_documents", return_value={"evidence": [manual]}):
            result = await self.service.process(FeedbackBatch(records=[FeedbackInput(
                external_id="manual-question", original_content="How do I change my PIN?"
            )]))
        incident_id = result["records"][0]["incident_id"]
        incident = self.orchestrator.get_incident(incident_id)
        self.assertEqual(incident["poc"]["resolution"]["actions"][0]["answer_status"], "documented_guidance_requires_review")
        run = self.routines.get_run(incident["poc"]["routine_execution"]["run_id"])
        self.assertEqual(run["status"], "completed")
        self.assertEqual(self.store.proposals()["items"], [])

    async def test_customer_question_adapter_returns_cited_evidence_without_model(self):
        from customer_ai import customer_ai

        passage = {"id": "manual-pin", "text": "Open Account, choose Change Bike PIN, and follow the OTP prompt.",
                   "citation": {"source": "Cluster & ETM Document.docx", "chunk": 14}, "score": 4.2}
        with patch.object(self.application.knowledge_base, "search_documents", return_value={"evidence": [passage]}), \
                patch.object(customer_ai, "status", return_value={"enabled": False}):
            answer = await self.application._answer_customer_question(
                FeedbackInput(external_id="direct-pin-question", original_content="How do I change my PIN?")
            )
        self.assertEqual(answer["status"], "evidence_only")
        self.assertEqual(answer["evidence"][0]["citation"]["source"], "Cluster & ETM Document.docx")
        self.assertFalse(answer["steps"])

    async def test_question_satisfaction_is_saved_and_unresolved_feedback_queued(self):
        record = FeedbackInput(external_id="question-satisfaction", original_content="How do I change my PIN?")
        identifier, _ = self.store.retain(record)
        self.store.save_response(identifier, {"status": "evidence_only", "summary": "Read the cited instructions.", "steps": [], "evidence": []})
        api = FastAPI()
        api.include_router(self.service.router())
        with TestClient(api) as client:
            resolved = client.post(f"/api/customer-intelligence/feedback/{identifier}/satisfaction", json={"resolved": True})
            self.assertEqual(resolved.status_code, 200, resolved.text)
            self.assertTrue(self.store.response(identifier)["satisfied"])
            unresolved = client.post(f"/api/customer-intelligence/feedback/{identifier}/satisfaction", json={"resolved": False, "followup": "The account screen still rejects my PIN"})
            self.assertEqual(unresolved.status_code, 200, unresolved.text)
            self.assertEqual(unresolved.json()["status"], "followup_queued")
            self.assertEqual(self.store.job(unresolved.json()["job_id"])["status"], "queued")

    async def test_investigation_satisfaction_and_followup(self):
        result = await self.service.process(FeedbackBatch(records=[FeedbackInput(
            external_id="satisfied-incident", original_content="Battery is stuck and not updating", rating=1)]))
        incident_id = result["records"][0]["incident_id"]
        feedback_id = result["records"][0]["feedback_id"]
        api = FastAPI()
        api.include_router(self.service.router())
        with TestClient(api) as client:
            response = client.post(f"/api/customer-intelligence/investigations/{incident_id}/satisfaction",
                                   json={"resolved": False, "followup": "The reading is still frozen"})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(self.store.response(feedback_id)["followup"], "The reading is still frozen")
            self.assertEqual(self.store.job(response.json()["job_id"])["status"], "queued")

    async def test_ai_failure_is_not_reported_as_success(self):
        from customer_ai import CustomerAIError, customer_ai

        with patch.object(customer_ai, "status", return_value={"enabled": True, "configured": False, "model": ""}), patch.object(customer_ai, "understand", side_effect=CustomerAIError("Model unavailable")):
            result = await self.service.process(FeedbackBatch(records=[FeedbackInput(external_id="hf-fail", original_content="Great design", rating=5)]))
        self.assertEqual(result["records"][0]["analysis"]["analysis_status"], "ai_failed_fallback_requires_review")
        self.assertEqual(result["records"][0]["analysis"]["ai_error"], "Model unavailable")

    async def test_reanalyze_saved_unknown_pin_feedback(self):
        from customer_intelligence import FeedbackAnalysis

        record = FeedbackInput(external_id="saved-pin", original_content="how to change the bike pin? i have so many times but i couldnt change it")
        identifier, _ = self.store.retain(record)
        self.store.save_analysis(identifier, FeedbackAnalysis(sentiment="insufficient_evidence", topic="unknown", language="unverified", requires_investigation=False, evidence=[]), None)
        api = FastAPI()
        api.include_router(self.service.router())
        with TestClient(api) as client:
            self.assertEqual(client.post(f"/api/feedback/{identifier}/reanalyze").status_code, 202)
            self.assertEqual(client.post("/api/feedback/missing/reanalyze").status_code, 404)
        result = await self.service.process(FeedbackBatch(records=[record]))
        self.assertEqual(result["records"][0]["feedback_id"], identifier)
        self.assertEqual(result["records"][0]["analysis"]["sentiment"], "negative")
        self.assertIsNotNone(result["records"][0]["incident_id"])
        self.assertEqual(self.store.list_records(10, 0)["total"], 1)

    async def test_visual_investigation_api_lists_reruns_and_preserves_ticket_gate(self):
        result = await self.service.process(FeedbackBatch(records=[FeedbackInput(external_id="visual-flow", original_content="Battery is stuck and not updating", rating=1)]))
        identifier = result["records"][0]["incident_id"]
        with TestClient(self.application.app) as client:
            listing = client.get("/api/customer-intelligence/investigations?dataset=real")
            self.assertEqual(listing.status_code, 200)
            self.assertEqual(listing.json()["items"][0]["incident_id"], identifier)
            previous = client.get(f"/api/customer-intelligence/investigations/{identifier}").json()["run"]["run_id"]
            with patch.object(self.application.knowledge_base, "search_documents", return_value={"evidence": [{"id": "fresh-sop", "text": "Freshly retrieved reference", "review_required": True}]}):
                rerun = client.post(f"/api/customer-intelligence/investigations/{identifier}/run")
            self.assertEqual(rerun.status_code, 202, rerun.text)
            detail = client.get(f"/api/customer-intelligence/investigations/{identifier}").json()
            self.assertNotEqual(detail["run"]["run_id"], previous)
            self.assertEqual(detail["run"]["status"], "waiting_approval")
            self.assertIsNone(detail["incident"]["poc"]["ticket"])
            self.assertFalse(detail["running"])
            self.assertEqual(detail["incident"]["evidence"]["sops"]["evidence"][0]["id"], "fresh-sop")
            self.assertEqual(client.post("/api/customer-intelligence/investigations/missing/run").status_code, 404)

    async def test_deleting_customer_investigation_removes_linked_data_and_allows_resubmission(self):
        record = FeedbackInput(external_id="archive-flow", original_content="Battery is stuck and not updating", rating=1)
        result = await self.service.process(FeedbackBatch(records=[record]))
        incident_id = result["records"][0]["incident_id"]
        feedback_id = result["records"][0]["feedback_id"]
        with TestClient(self.application.app) as client:
            removed = client.delete(f"/api/customer-intelligence/investigations/{incident_id}")
            self.assertEqual(removed.status_code, 200, removed.text)
            self.assertEqual(client.get("/api/customer-intelligence/investigations?dataset=real").json()["items"], [])
            self.assertEqual(client.post(f"/api/customer-intelligence/investigations/{incident_id}/run").status_code, 404)
        self.assertIsNone(self.orchestrator.get_incident(incident_id))
        self.assertIsNone(self.store.analysis(feedback_id))
        self.assertEqual(self.store.list_records(10, 0)["total"], 0)
        self.assertEqual(self.routines.list_runs(), [])
        self.assertEqual(self.store.proposals()["items"], [])
        recreated = await self.service.process(FeedbackBatch(records=[record]))
        new_incident_id = recreated["records"][0]["incident_id"]
        self.assertNotEqual(new_incident_id, incident_id)
        with TestClient(self.application.app) as client:
            self.assertEqual(client.get("/api/customer-intelligence/investigations?dataset=real").json()["items"][0]["incident_id"], new_incident_id)

    async def test_stopped_working_feedback_creates_visible_investigation(self):
        result = await self.service.process(FeedbackBatch(records=[FeedbackInput(
            external_id="stopped-app-pipeline", original_content="My app stopped working")]))
        incident_id = result["records"][0]["incident_id"]
        self.assertIsNotNone(incident_id)
        with TestClient(self.application.app) as client:
            visible_ids = {item["incident_id"] for item in client.get(
                "/api/customer-intelligence/investigations?dataset=real").json()["items"]}
        self.assertIn(incident_id, visible_ids)

    async def test_real_dataset_excludes_synthetic_and_summarizes_sentiment(self):
        records = [
            FeedbackInput(source="google_play", app_id="com.flyingflea.ffapp", source_url="https://play.google.com/store/apps/details?id=com.flyingflea.ffapp",
                          external_id="real", original_content="Great electric motorcycle app", rating=5),
            FeedbackInput(source="synthetic", external_id="test", original_content="Love the design", rating=5),
        ]
        await self.service.process(FeedbackBatch(records=records))
        api = FastAPI()
        api.include_router(self.service.router())
        with TestClient(api) as client:
            listing = client.get("/api/feedback?dataset=real").json()
            self.assertEqual(listing["total"], 1)
            self.assertEqual(listing["items"][0]["source"], "google_play")
            overview = client.get("/api/customer-intelligence/overview?dataset=real").json()
            self.assertEqual(overview["sentiment_counts"], {"positive": 1})
            self.assertEqual(overview["average_rating"], 5)
            self.assertEqual(overview["investigations"], 0)

    async def test_ticket_authorization_and_idempotent_creation(self):
        result = await self.service.process(FeedbackBatch(records=[FeedbackInput(external_id="login", original_content="Login fails and OTP does not arrive", rating=1)]))
        identifier = result["records"][0]["incident_id"]
        api = FastAPI()
        api.include_router(self.service.router())
        with TestClient(api) as client, patch.dict("os.environ", {"CX_OPERATOR_TOKEN": "test-operator-token", "CX_OPERATOR_ID": "test-operator"}):
            url = f"/api/customer-intelligence/approvals/{identifier}/approve"
            self.assertEqual(client.post(url, json={}).status_code, 403)
            first = client.post(url, json={}, headers={"Authorization": "Bearer test-operator-token"})
            second = client.post(url, json={}, headers={"Authorization": "Bearer test-operator-token"})
            self.assertEqual(first.status_code, 200, first.text)
            self.assertTrue(second.json()["idempotent"])
            self.assertEqual(first.json()["ticket"], second.json()["ticket"])
            self.assertEqual(self.store.proposals()["items"][0]["approver"], "test-operator")
            incident = self.orchestrator.get_incident(identifier)
            self.assertEqual(incident["poc"]["ticket"], first.json()["ticket"])
            self.assertEqual(incident["poc"]["routine_execution"]["status"], "completed")
            with self.store.connection() as connection:
                self.assertEqual(connection.execute("SELECT count(*) FROM feedback_audit_events").fetchone()[0], 2)

    async def test_rejection_requires_reason_and_never_creates_ticket(self):
        result = await self.service.process(FeedbackBatch(records=[FeedbackInput(external_id="rejected", original_content="Charging is not starting", rating=1)]))
        identifier = result["records"][0]["incident_id"]
        api = FastAPI()
        api.include_router(self.service.router())
        with TestClient(api) as client, patch.dict("os.environ", {"CX_OPERATOR_TOKEN": "test-operator-token"}):
            headers = {"Authorization": "Bearer test-operator-token"}
            url = f"/api/customer-intelligence/approvals/{identifier}/reject"
            self.assertEqual(client.post(url, json={}, headers=headers).status_code, 422)
            rejected = client.post(url, json={"note": "Need additional evidence"}, headers=headers)
            self.assertEqual(rejected.status_code, 200, rejected.text)
            self.assertIsNone(rejected.json()["ticket"])
            self.assertEqual(self.orchestrator.get_incident(identifier)["poc"]["routine_execution"]["status"], "rejected")
            approved = client.post(f"/api/customer-intelligence/approvals/{identifier}/approve", json={}, headers=headers)
            self.assertEqual(approved.status_code, 409)

    async def test_feedback_api_validation_and_duplicate_original_immutability(self):
        original = FeedbackInput(external_id="immutable", original_content="Battery is stuck", rating=1)
        identifier, _ = self.store.retain(original)
        self.store.retain(FeedbackInput(external_id="immutable", original_content="Changed content", rating=5))
        self.assertEqual(self.store.list_records(10, 0)["items"][0]["original_content"], original.original_content)
        api = FastAPI()
        api.include_router(self.service.router())
        with TestClient(api) as client:
            self.assertEqual(client.post("/api/feedback/import", json={"records": [{"external_id": "bad", "original_content": "bad rating", "rating": 6}]}).status_code, 422)
            response = client.post("/api/feedback/import", json={"records": [original.model_dump()]})
            self.assertEqual(response.status_code, 202)
            self.assertEqual(client.get(f"/api/customer-intelligence/jobs/{response.json()['job_id']}").json()["status"], "queued")


if __name__ == "__main__":
    unittest.main()