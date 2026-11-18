import unittest

from knowledge_base_agent.relevance import rank_passages
from knowledge_base_agent.ma04_document_retrieval import hybrid_rank
from customer_intelligence import FeedbackInput, classify_feedback


class CustomerRetrievalTests(unittest.TestCase):
    def test_hybrid_search_combines_semantic_and_lexical_candidates(self):
        semantic = [
            {"id": "meaning-match", "text": "The motorcycle display estimates remaining travel distance.", "metadata": {}, "score": 0.82, "retrieval_method": "chroma_all-MiniLM-L6-v2_cosine"},
            {"id": "exact-match", "text": "Estimated range uses Conservative, Average and Aggressive projections.", "metadata": {}, "score": 0.74, "retrieval_method": "chroma_all-MiniLM-L6-v2_cosine"},
        ]
        lexical = [
            {"id": "exact-match", "text": "Estimated range uses Conservative, Average and Aggressive projections.", "metadata": {}, "score": 3.2, "retrieval_method": "bm25_lexical_with_query_expansion"},
            {"id": "keyword-only", "text": "Range projection specification table.", "metadata": {}, "score": 1.1, "retrieval_method": "bm25_lexical_with_query_expansion"},
        ]
        results = hybrid_rank(semantic, lexical, limit=3)
        self.assertEqual({item["id"] for item in results}, {"meaning-match", "exact-match", "keyword-only"})
        self.assertEqual(results[0]["id"], "exact-match")
        self.assertEqual(results[0]["retrieval_method"], "hybrid_chroma_bm25_rrf")

    def test_exact_app_complaint_retrieves_app_chunks_not_common_words(self):
        passages = [
            {"id": "app-sync", "text": "Issue Description: Data Not syncing with Mobile App Resolution Details: Check connectivity and contact authorized support if syncing does not resume."},
            {"id": "brakes", "text": "If it is not working inspect the brake. The bike should be serviced."},
            {"id": "manual", "text": "Flying Flea application connects the mobile device. Select Bike Management to view vehicle data."},
        ]
        query = "why my app is not working? any issue with the app?"
        results = rank_passages(query, passages)
        self.assertIn("app-sync", [item["id"] for item in results])
        self.assertNotIn("brakes", [item["id"] for item in results])
        analysis = classify_feedback(FeedbackInput(external_id="app-failure", original_content=query), lambda text: text)
        self.assertEqual(analysis.topic, "mobile_app")
        self.assertTrue(analysis.requires_investigation)

    def test_no_query_evidence_returns_no_match(self):
        self.assertEqual(rank_passages("app not working", [{"id": "battery", "text": "Battery charging instructions"}]), [])

    def test_planner_does_not_discard_app_manual_passages(self):
        from poc_agents.ma24_runbook_planner import RunbookFixPlanner

        incident = {"incident_id": "APP-TEST", "summary": "why my app is not working? any issue with the app?",
                    "alert": {"customer_feedback": {"analysis": {"topic": "unknown"}}},
                    "evidence": {"sops": {"evidence": [{"id": "manual", "text": "Flying Flea app: select Bike Management to view vehicle data and settings.", "metadata": {"document_type": "pdf"}, "citation": {"source": "manual.pdf", "chunk": 2}}]}}}
        result = RunbookFixPlanner().plan(incident, {})
        self.assertEqual(result["answer_status"], "relevant_evidence_requires_clarification")
        self.assertEqual(result["knowledge_guidance"][0]["citation"]["source"], "manual.pdf")

    def test_app_troubleshooting_checklist_ranks_first_and_is_extracted(self):
        from poc_agents.ma24_runbook_planner import RunbookFixPlanner

        passages = [{"id": "features", "text": "App app app mobile features", "metadata": {"document_type": "pdf"}},
                    {"id": "checks", "text": "App not showing live data, check the following: \uf0a2 Bluetooth: Enable Bluetooth on your mobile. \uf0a2 Network: Check your internet connection.", "metadata": {"document_type": "pdf"}, "citation": {"source": "owner-manual.pdf", "chunk": 88}}]
        ranked = rank_passages("why my app is not working?", passages)
        self.assertEqual(ranked[0]["id"], "checks")
        incident = {"incident_id": "APP-CHECK", "summary": "why my app is not working?", "alert": {"customer_feedback": {"analysis": {"topic": "mobile_app"}}}, "evidence": {"sops": {"evidence": ranked}}}
        guidance = RunbookFixPlanner().plan(incident, {})["knowledge_guidance"]
        self.assertEqual(guidance[0]["evidence_id"], "checks")
        self.assertIn("1. Bluetooth:", guidance[0]["text"])