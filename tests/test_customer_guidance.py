import unittest

from poc_agents.ma24_runbook_planner import RunbookFixPlanner


class CustomerGuidanceTests(unittest.TestCase):
    def test_unknown_negative_manual_feedback_is_not_discarded(self):
        from customer_intelligence import FeedbackInput, classify_feedback

        for text in ("My bike is not working", "This app isn't doing what it should", "It won't start", "Why is it not doing anything?", "The screen freezes every morning"):
            with self.subTest(text=text):
                result = classify_feedback(FeedbackInput(external_id="manual-negative", original_content=text), lambda value: value)
                self.assertEqual(result.sentiment, "negative")
                self.assertTrue(result.requires_investigation)
                self.assertEqual(result.intent, "complaint")

    def test_negative_word_fragments_do_not_make_praise_a_complaint(self):
        from customer_intelligence import FeedbackInput, classify_feedback

        result = classify_feedback(FeedbackInput(external_id="praise", original_content="Great notification design, excellent app"), lambda value: value)
        self.assertEqual(result.sentiment, "positive")
        self.assertFalse(result.requires_investigation)

    def test_exact_customer_pin_failure_and_howto_are_routed(self):
        from customer_intelligence import FeedbackInput, classify_feedback

        for text in ("how to change the bike pin? i have so many times but i couldnt change it", "How do I reset my PIN?"):
            result = classify_feedback(FeedbackInput(external_id="pin-test", original_content=text), lambda value: value)
            self.assertEqual(result.topic, "login_authentication")
            self.assertTrue(result.requires_investigation)
        self.assertEqual(classify_feedback(FeedbackInput(external_id="failed", original_content="I couldnt change the bike pin"), lambda value: value).sentiment, "negative")

    def test_manual_pin_procedure_is_not_ignored_without_tracker_labels(self):
        incident = self.incident("Changing the Primary PIN. Select Account, press OK and enter the OTP to change your PIN.")
        passage = incident["evidence"]["sops"]["evidence"][0]
        passage["metadata"] = {"document_type": "pdf"}
        passage["citation"] = {"source": "owner-manual.pdf", "chunk": 84}
        guidance = RunbookFixPlanner().customer_guidance(incident)
        self.assertEqual(guidance[0]["source_kind"], "manual_or_sop")
        self.assertIn("Select Account", guidance[0]["text"])

    def test_customer_sync_steps_prefer_manual_over_internal_tracker_guidance(self):
        incident = self.incident("why my app data is not syncing to the cloud? what can i do?")
        incident["alert"]["customer_feedback"]["content"] = "why my app data is not syncing to the cloud? what can i do?"
        incident["alert"]["customer_feedback"]["analysis"]["topic"] = "mobile_app"
        incident["evidence"]["sops"]["evidence"] = [
            {"id": "tracker", "text": "Issue Description: Telebuff overflow Solution (FIX): Clear Telebuff on the vehicle using Mech App.",
             "metadata": {"document_type": "issue_tracker"}, "review_required": True,
             "citation": {"source": "tracker.csv", "row": 9}, "retrieval_method": "bm25"},
            {"id": "manual", "text": "To sync to the cloud, your mobile must have Wi-Fi or mobile data. \uf0a2 Bluetooth Connection: Keep Bluetooth enabled near the motorcycle. \uf0a2 GPS/GSM Signal: Move to an open area.",
             "metadata": {"document_type": "pdf"}, "review_required": True,
             "citation": {"source": "owner-manual.pdf", "chunk": 111}, "retrieval_method": "bm25"},
        ]
        result = RunbookFixPlanner().plan(incident, {})
        self.assertEqual(result["title"], "Try these documented app-sync checks")
        self.assertEqual(result["knowledge_guidance"][0]["citation"]["source"], "owner-manual.pdf")
        self.assertIn("Wi-Fi or mobile data", result["knowledge_guidance"][0]["text"])
        self.assertNotIn("Mech App", result["knowledge_guidance"][0]["text"])

    def test_range_review_gets_manual_estimate_context_without_claim_validation(self):
        incident = self.incident("The claimed 155 km range differs from the 86 km I experienced.")
        incident["alert"]["customer_feedback"]["analysis"]["topic"] = "vehicle_range"
        incident["evidence"]["sops"]["evidence"] = [{
            "id": "range-manual", "text": "The display provides three estimated range projections based on current and recent riding style. Conservative shows maximum possible range; Average shows typical daily range; Aggressive shows minimum expected range during high speeds, rapid acceleration, frequent stops or steep climbs.",
            "metadata": {"document_type": "pdf"}, "review_required": True,
            "citation": {"source": "owner-manual.pdf", "chunk": 113},
        }]
        result = RunbookFixPlanner().plan(incident, {})
        self.assertEqual(result["answer_status"], "documented_guidance_requires_review")
        self.assertEqual(result["knowledge_guidance"][0]["citation"]["chunk"], 113)
        self.assertIn("Conservative", result["knowledge_guidance"][0]["text"])
        self.assertIn("quoted range claim", result["description"])

    def test_feature_request_gets_manual_riding_mode_controls_and_limits_claim(self):
        review = "The app cannot change riding modes, create profiles, or change charging mode."
        incident = self.incident(review)
        incident["alert"]["customer_feedback"]["analysis"]["topic"] = "mobile_app"
        incident["alert"]["customer_feedback"]["content"] = review
        incident["evidence"]["sops"]["evidence"] = [{
            "id": "riding-modes", "text": "The 5 Riding modes can only be selected through the twist shifter or instrument cluster. The selected mode will reflect on the Flying Flea App.",
            "metadata": {"document_type": "pdf"}, "review_required": True, "retrieval_method": "bm25",
            "citation": {"source": "owner-manual.pdf", "chunk": 170},
        }]
        result = RunbookFixPlanner().plan(incident, {})
        self.assertEqual(result["answer_status"], "documented_guidance_requires_review")
        self.assertEqual(result["knowledge_guidance"][0]["citation"]["chunk"], 170)
        self.assertIn("twist shifter", result["knowledge_guidance"][0]["text"])
        self.assertIn("profiles", result["description"])

    def test_manual_pin_bullets_omit_sample_screen_values(self):
        incident = self.incident('\uf0a2 Select Account and press OK. NOTE default pin information \uf0a2 Next to the Account, select Change Bike Pin and press OK. \uf0a2 An OTP will be sent to your registered mobile number. \uf0a2 Enter the OTP using the keypad. \uf0a2 Next, enter a new 5 digit PIN and re-enter to confirm. P Account 75 0% 12345')
        incident["evidence"]["sops"]["evidence"][0]["metadata"] = {"document_type": "pdf"}
        result = RunbookFixPlanner().customer_guidance(incident)[0]["text"]
        self.assertIn("1. Select Account", result)
        self.assertIn("5. Next, enter", result)
        self.assertNotIn("12345", result)

    def test_manual_continuation_does_not_expose_keypad_or_secondary_pin_steps(self):
        incident = self.incident('12345 ENTER OTP \uf0a2 Once the cluster displays the message Pin Changed Successfully, your primary user PIN is updated. 3. Secondary Pin (Digital Pin Sharing) \uf0a2 Tap Bike Management and share a secondary PIN.')
        incident["evidence"]["sops"]["evidence"][0]["metadata"] = {"document_type": "pdf"}
        text = RunbookFixPlanner().customer_guidance(incident)[0]["text"]
        self.assertIn("Pin Changed Successfully", text)
        self.assertNotIn("12345", text)
        self.assertNotIn("share a secondary", text)

    def test_reference_ingestion_does_not_read_raw_tracker_csv(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        from knowledge_base_agent.ma03_document_ingestion import DocumentIngestionMicroAgent

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "manual.pdf").write_bytes(b"test PDF placeholder")
            (root / "tracker.csv").write_text("Reported by,VIN\nPrivate Person,Private VIN", encoding="utf-8")
            ingestor = DocumentIngestionMicroAgent(root)
            with patch.object(ingestor, "_extract", return_value="OTP guidance from manual") as extract:
                result = ingestor.prepare({".pdf", ".docx"})
            self.assertEqual(result["files_read"], 1)
            self.assertEqual(extract.call_args.args[0].name, "manual.pdf")
            self.assertEqual(result["documents"][0]["metadata"]["document_type"], "pdf")

    def test_reference_size_limit_is_explicit(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        from knowledge_base_agent.ma03_document_ingestion import DocumentIngestionMicroAgent

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "manual.pdf").write_bytes(b"bounded-placeholder")
            ingestor = DocumentIngestionMicroAgent(root)
            with patch.object(ingestor, "_extract", return_value="Manual instructions"):
                self.assertEqual(ingestor.prepare({".pdf"}, 1)["files_read"], 0)
                self.assertEqual(ingestor.prepare({".pdf"}, 100)["files_read"], 1)

    def incident(self, text, stale=False):
        return {"incident_id": "INC-OTP-TEST", "alert": {"customer_feedback": {
            "analysis": {"topic": "login_authentication"}}}, "evidence": {"sops": {"evidence": [
                {"id": "otp-row", "text": text, "stale": stale, "review_required": True,
                 "citation": {"source": "uploaded-tracker.csv", "row": 7}}
            ]}}}

    def test_extracts_documented_otp_guidance_with_citation(self):
        result = RunbookFixPlanner().plan(self.incident(
            "Issue Description: OTP not received\nSolution (FIX): Use the originally registered contact.\nStatus: Closed"), {})
        self.assertEqual(result["answer_status"], "documented_guidance_requires_review")
        self.assertEqual(result["knowledge_guidance"][0]["text"], "Use the originally registered contact.")
        self.assertEqual(result["knowledge_guidance"][0]["citation"]["row"], 7)
        self.assertTrue(result["knowledge_guidance"][0]["review_required"])

    def test_unrelated_stale_or_missing_solution_does_not_invent_answer(self):
        for text, stale in [("Issue Description: Battery range\nSolution (FIX): Review range", False),
                            ("Issue Description: OTP not received", False),
                            ("Issue Description: OTP\nSolution (FIX): Use registered contact", True)]:
            result = RunbookFixPlanner().plan(self.incident(text, stale), {})
            self.assertEqual(result["answer_status"], "insufficient_knowledge")
            self.assertEqual(result["knowledge_guidance"], [])

    def test_flattened_tracker_skips_pending_and_extracts_resolution(self):
        result = RunbookFixPlanner().plan(self.incident(
            "Issue Description: OTP not received Status: Open Containment Action: Pending Corrective Action: Pending Resolution Details: Use the originally registered contact. Priority: P3"), {})
        self.assertEqual(result["knowledge_guidance"][0]["text"], "Use the originally registered contact.")

    def test_customer_otp_question_triggers_investigation_without_rating(self):
        from customer_intelligence import FeedbackInput, classify_feedback

        result = classify_feedback(FeedbackInput(external_id="otp-demo", original_content="why my otp is not working or getting"), lambda text: text)
        self.assertEqual(result.topic, "login_authentication")
        self.assertEqual(result.sentiment, "negative")
        self.assertTrue(result.requires_investigation)

    def test_hypothesis_is_not_presented_as_solution(self):
        result = RunbookFixPlanner().plan(self.incident(
            "Issue Description: OTP missing Resolution Details: There is no concrete evidence, but the hypothesis is connectivity."), {})
        self.assertEqual(result["answer_status"], "insufficient_knowledge")