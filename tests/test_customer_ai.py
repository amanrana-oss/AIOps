import unittest
from unittest.mock import patch

from customer_ai import CustomerAIError, HuggingFaceCustomerAI


class CustomerAITests(unittest.IsolatedAsyncioTestCase):
    async def test_unconfigured_ai_fails_explicitly(self):
        ai = HuggingFaceCustomerAI()
        with patch.dict("os.environ", {"CX_AI_PROVIDER": "none"}):
            self.assertFalse(ai.status()["configured"])
            with self.assertRaises(CustomerAIError):
                await ai.understand("My bike PIN cannot be changed", None)

    async def test_neutral_question_can_require_investigation(self):
        ai = HuggingFaceCustomerAI()
        with patch.object(ai, "generate", return_value={"sentiment": "neutral", "topic": "login_authentication", "intent": "question", "language": "English", "requires_investigation": True, "evidence": ["PIN change request"]}):
            result = await ai.understand("How can I change my PIN?", None)
            self.assertTrue(result.requires_investigation)
            self.assertEqual(result.intent, "question")

    async def test_answer_requires_valid_citations(self):
        ai = HuggingFaceCustomerAI()
        passages = [{"id": "manual-84", "text": "Select Account then Change Bike PIN."}]
        with patch.object(ai, "generate", return_value={"status": "supported", "summary": "Documented PIN procedure", "steps": [{"text": "Select Account.", "evidence_ids": ["invented"]}]}):
            with self.assertRaises(CustomerAIError):
                await ai.answer("Change PIN", passages)
        with patch.object(ai, "generate", return_value={"status": "supported", "summary": "Documented PIN procedure", "steps": [{"text": "Select Account.", "evidence_ids": ["manual-84"]}]}):
            self.assertEqual((await ai.answer("Change PIN", passages)).status, "supported")

    async def test_missing_knowledge_never_invents_answer(self):
        ai = HuggingFaceCustomerAI()
        result = await ai.answer("Unknown issue", [])
        self.assertEqual(result.status, "insufficient_evidence")
        self.assertEqual(result.steps, [])