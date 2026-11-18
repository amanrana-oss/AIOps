"""MA-24: map candidate incident patterns to cautious runbook suggestions."""

from __future__ import annotations

from typing import Any
import re
from knowledge_base_agent.relevance import rank_passages


class RunbookFixPlanner:
    @staticmethod
    def _primary_pin_steps(text: str) -> list[str]:
        lowered = text.casefold()
        requirements = (
            "account" in lowered,
            "change bike pin" in lowered,
            "otp" in lowered,
            "new 5 digit pin" in lowered or "new 5-digit pin" in lowered,
        )
        if not all(requirements):
            return []
        steps = [
            "On the motorcycle display, open Account and press OK.",
            "Select Change Bike PIN and press OK.",
            "Enter the OTP sent to your registered mobile number, using the arrow pad or the on-screen keypad.",
            "Enter a new 5-digit PIN, then enter it again to confirm.",
        ]
        if "pin changed successfully" in lowered:
            steps.append("The change is complete when the display says PIN Changed Successfully.")
        return steps

    @staticmethod
    def _manual_customer_steps(text: str, query: str) -> list[str]:
        lowered_query = query.casefold()
        lowered_text = text.casefold()
        if ("estimated range" in lowered_text and "conservative" in lowered_text
                and "average" in lowered_text and "aggressive" in lowered_text
                and "range" in lowered_query):
            return [
                "The owner manual describes three estimated-range projections on the motorcycle display, based on battery level and current/recent riding style.",
                "Conservative is the maximum-style estimate, Average is a typical daily estimate, and Aggressive is the lower estimate for higher power use such as high speeds, rapid acceleration, frequent stops or steep climbs.",
            ]
        if ("5 riding modes can only be selected" in lowered_text
                and "reflect on the flying flea app" in lowered_text
                and any(term in lowered_query for term in ("mode", "profile", "app"))):
            return [
                "The owner manual says the five riding modes are selected using the motorcycle's twist shifter or instrument cluster; the selected mode is reflected in the Flying Flea app.",
            ]
        if any(term in lowered_query for term in ("sync", "cloud", "app", "connect", "data")) and "check the following" in lowered_text:
            checklist = text.split("check the following", 1)[1].split("Troubleshooting Steps", 1)[0]
            steps = []
            labels = {
                "proximity & bluetooth": "Keep your phone within 3 metres of the motorcycle and turn Bluetooth on.",
                "power status": "Make sure the motorcycle is ON or in standby so the app can communicate with it.",
                "network signal mobile": "Check that your phone has an active internet connection through Wi-Fi or mobile data.",
                "app permissions": "In phone settings, allow the Flying Flea app to use Location Services and Bluetooth/Nearby Devices.",
            }
            for bullet in checklist.split("\uf0a2"):
                label, separator, _detail = bullet.partition(":")
                if separator:
                    instruction = labels.get(label.strip().casefold())
                    if instruction and instruction not in steps:
                        steps.append(instruction)
            return steps
        if any(term in lowered_query for term in ("pair", "connect", "bluetooth")) and "pair new device" in lowered_text:
            return [
                "On the motorcycle display, open Settings > Connections > Mobile Device.",
                "Turn Bluetooth on in your phone, then choose Pair New Device on the motorcycle display.",
                "Select your phone from the nearby devices shown on the display.",
                "Confirm the passcode shown on the motorcycle display to finish pairing.",
            ]
        safe_actions = (
            ("pair new device", "Pair the phone: On the motorcycle display, open Settings > Connections > Mobile Device > Pair New Device, select your phone, then confirm the passcode shown on the display."),
            ("confirm the passcode at the cluster", "Confirm the passcode shown on the motorcycle display to finish pairing the phone."),
            ("within 3 metres", "Proximity and Bluetooth: Keep your phone within 3 metres, with Bluetooth enabled; start pairing from the Flying Flea app."),
            ("power on or be in standby mode", "Motorcycle power: Confirm the motorcycle is ON or in standby mode."),
            ("active internet connection", "Phone internet: Confirm Wi-Fi or mobile data is available."),
            ("permission to access location services and bluetooth", "App permissions: Allow the Flying Flea app to access Location Services and Bluetooth/Nearby Devices in phone settings."),
            ("toggle your mobile’s bluetooth", "Refresh Bluetooth: Turn Bluetooth off for 5 seconds, then turn it back on."),
            ("toggle your mobile's bluetooth", "Refresh Bluetooth: Turn Bluetooth off for 5 seconds, then turn it back on."),
            ("force close app", "Restart the app: Close the Flying Flea app completely, then open it again."),
            ("forget this device", "Re-pair: In phone Bluetooth settings, forget the Flying Flea device, then pair again from inside the app."),
            ("briefly press the power button", "Wake the connectivity module: If the motorcycle has been unused for over 48 hours, briefly press its power button."),
            ("app store/play store", "App version: Check the App Store or Play Store for a Flying Flea app update."),
            ("clear app cache", "Android app cache: Open Settings > Apps > Flying Flea > Storage > Clear Cache."),
            ("wi-fi or mobile data", "Internet: Confirm the phone has Wi-Fi or mobile data while syncing."),
            ("good network area", "Network signal: Move the motorcycle to an open area with a good GPS/mobile signal."),
            ("motorcycle's power button is on", "Motorcycle power: Confirm the motorcycle is on or in Park mode."),
            ("bluetooth and push notifications enabled", "App setup: Keep Bluetooth enabled; enable push notifications if you want app notifications."),
            ("always use the original charging cable", "Charging safety: Use only the original Flying Flea charging cable. Keep the port, cable, and hands dry, and charge in a cool, dry, well-ventilated place."),
            ("ensure the motorcycle is powered on and in park mode", "Charging: Put the motorcycle in Park with the wall switch off. Connect the approved charging cable and socket as described in the cited manual, then switch the supply on."),
            ("charging instructions guide for charging the motorcycle", "Charging: Follow the cited manual's charge-port instructions. Stop immediately and unplug if the cable or connection becomes excessively hot."),
        )
        segments = re.split(r"\uf0a2", text)
        steps = []
        for segment in segments:
            lowered = segment.casefold()
            if any(term in lowered for term in ("adb", "command prompt", "telebuff", "firmware flashing", "root access")):
                continue
            for marker, instruction in safe_actions:
                if marker in lowered and instruction not in steps:
                    steps.append(instruction)
        return steps[:5]

    @staticmethod
    def customer_guidance(incident: dict[str, Any]) -> list[dict[str, Any]]:
        feedback = incident.get("alert", {}).get("customer_feedback", {})
        topic = feedback.get("analysis", {}).get("topic", "unknown")
        terms = {"login_authentication": ("otp", "login", "passcode", "primary pin", "digital pin"),
                 "battery_status": ("battery", "soc"), "charging": ("charging", "charger"),
                 "connectivity": ("connectivity", "bluetooth"), "vehicle_range": ("range",)}.get(topic, ())
        query = str(feedback.get("content") or incident.get("summary") or " ".join(terms))
        retained = incident.get("evidence", {}).get("sops", {}).get("evidence", [])
        candidates = retained[:8] if retained and all(item.get("retrieval_method") for item in retained) else rank_passages(query, retained, limit=8)
        guidance = []
        feature_request = feedback.get("analysis", {}).get("intent") == "feature_request"
        primary_pin_query = topic == "login_authentication" and re.search(r"\b(primary\s+)?pin\b", query, re.IGNORECASE) and not re.search(r"secondary|digital pin|share", query, re.IGNORECASE)
        for passage in candidates:
            text = str(passage.get("text", ""))
            if passage.get("stale"):
                continue
            if feature_request and "5 riding modes can only be selected" not in text.casefold():
                continue
            if primary_pin_query and "secondary pin" in text.casefold() and not "procedure of primary user pin change" in text.casefold():
                continue
            guidance_kind = "documented_resolution"
            fields = re.split(r"\s+(?=(?:Issue Description|Reported Date|Category|Status|Containment Action|Corrective Action|Sub-Category|Product|Priority|Severity|Resolution Details|RCA|Solution \(FIX\)|Date):)", text)
            answers = []
            for label in ("Solution (FIX)", "Resolution Details", "Corrective Action", "Containment Action"):
                for field in fields:
                    match = re.match(re.escape(label) + r"\s*:\s*(.*)", field, re.IGNORECASE | re.DOTALL)
                    if match and len(match.group(1).strip()) >= 15:
                        answers.append(match.group(1).strip())
                if answers:
                    break
            if not answers and passage.get("metadata", {}).get("document_type") in {"pdf", "docx"}:
                manual_steps = RunbookFixPlanner._manual_customer_steps(text, query)
                if manual_steps:
                    answers.append("\n".join(manual_steps if all(step[:1].isdigit() for step in manual_steps) else
                                                (f"{index}. {step}" for index, step in enumerate(manual_steps, start=1))))
                    guidance_kind = "customer_manual_steps"
                procedural = re.search(r"\b(tap|select|enter|press|choose|open)\b", text, re.IGNORECASE)
                if procedural and topic == "login_authentication" and re.search(r"\b(primary pin|change.{0,30}pin|pin.{0,30}chang\w*)\b", text, re.IGNORECASE):
                    steps = RunbookFixPlanner._primary_pin_steps(text)
                    if steps:
                        answers = ["\n".join(f"{index}. {step}" for index, step in enumerate(steps, start=1))]
                        guidance_kind = "customer_manual_steps"
                    elif "Secondary Pin (Digital Pin Sharing)" in text:
                        confirmation = re.search(r"Once the cluster displays[^.]+\.", text)
                        if confirmation:
                            answers.append(confirmation.group())
                    else:
                        answers.append(text)
            if not answers:
                continue
            excerpt = answers[0]
            if "no concrete evidence" in excerpt.casefold() or any(item["text"] == excerpt[:2200] for item in guidance):
                continue
            guidance.append({
                "text": excerpt[:2200], "citation": passage.get("citation", {}),
                "guidance_kind": guidance_kind,
                "source_kind": "manual_or_sop" if passage.get("metadata", {}).get("document_type") in {"pdf", "docx"} else "historical_tracker",
                "evidence_id": passage.get("id"), "review_required": passage.get("review_required", True),
                "applicability": "Confirm this source applies to the reported workflow, model and software version before using it.",
            })
            if primary_pin_query and guidance_kind == "customer_manual_steps":
                break
        return sorted(guidance, key=lambda item: (
            item.get("source_kind") != "manual_or_sop",
            item.get("guidance_kind") == "reference_excerpt",
        ))[:3]

    ACTIONS = {
        "database_query_timeout": ("Inspect PostgreSQL query timeout", "Review the failed query, active connections, and pool waiters; compare them with the incident trace before changing limits."),
        "database_pool_pressure": ("Check database pool saturation", "Review pool limits and active query waiters."),
        "messaging_lag": ("Inspect consumer capacity", "Check consumer health and queue depth before scaling."),
        "deployment_with_symptoms": ("Review the latest deployment", "Compare release timestamp with the incident timeline; consider rollback only after approval."),
        "application_exception": ("Inspect the exception trace", "Validate the failing code path and recent changes before deploying a fix."),
        "high_latency": ("Inspect latency contributors", "Compare service traces, dependency timings, and recent change events."),
    }

    def plan(self, incident: dict[str, Any], rca: dict[str, Any]) -> dict[str, Any]:
        if incident.get("alert", {}).get("customer_feedback"):
            guidance = self.customer_guidance(incident)
            reference_only = bool(guidance) and all(item.get("guidance_kind") == "reference_excerpt" for item in guidance)
            feedback = incident.get("alert", {}).get("customer_feedback", {})
            topic = feedback.get("analysis", {}).get("topic", "unknown")
            customer_text = str(feedback.get("content") or incident.get("summary", ""))
            primary_pin_query = topic == "login_authentication" and re.search(r"\b(primary\s+)?pin\b", customer_text, re.IGNORECASE) and not re.search(r"secondary|digital pin|share", customer_text, re.IGNORECASE)
            feature_request = feedback.get("analysis", {}).get("intent") == "feature_request" or bool(
                re.search(r"\b(?:can't|cannot|doesn't let(?: you)?|does not let(?: you)?)\b.{0,80}\b(?:adjust|change|create|add|control|customi[sz]e)\b", customer_text.casefold().replace("\u2019", "'"))
                and re.search(r"\b(?:mode|profile|feature|setting|function|option)s?\b", customer_text, re.IGNORECASE)
            )
            actionable_manual = bool(guidance) and guidance[0].get("guidance_kind") == "customer_manual_steps"
            if primary_pin_query and actionable_manual:
                title = "Follow these primary PIN steps"
                description = "The owner manual provides these steps for changing the primary bike PIN. Use the registered phone for its OTP; do not share the PIN."
            elif feature_request and actionable_manual:
                title = "Riding mode controls documented; other features need confirmation"
                description = "The cited owner-manual passage says riding modes are selected on the motorcycle and the selected mode appears in the app. It does not confirm whether app profiles or charging-mode controls are supported. Treat those points as feature feedback and ask the product/support team; this is not evidence of a charging fault."
            elif topic == "vehicle_range" and actionable_manual:
                title = "Understand the manual's range estimates"
                description = "The cited owner-manual passage explains the display's Conservative, Average and Aggressive estimates. It does not verify a quoted range claim or diagnose the rider's real-world range; confirm riding conditions and vehicle data with authorized support."
            elif actionable_manual and topic == "charging":
                title = "Follow these documented charging steps"
                description = "The owner manual provides the following charging instructions and safety precautions. Stop if the cable, port, or connection becomes hot or damaged."
            elif actionable_manual and topic in {"mobile_app", "connectivity"}:
                title = "Try these documented app-connectivity steps"
                description = "The owner manual provides the following checks for app connection and cloud sync. If the issue remains, the investigation can be escalated for support."
            else:
                title = "Relevant manual guidance found; clarify the failing function" if reference_only else "Guidance found in your knowledge base" if guidance else "No matching documented solution found"
                description = "These are relevant source excerpts, not a confirmed repair. Which app screen or function fails, what error appears, and which app/phone version are you using? Use that detail to narrow the investigation. Technical service procedures require authorized support." if reference_only else "Review the documented guidance below and confirm it applies to this customer. Historical resolutions are not a confirmed diagnosis. Backend changes must be handled by authorized support." if guidance else "No relevant resolution procedure was found in the retrieved knowledge. Request support investigation rather than guessing a fix."
            return {
                "action_id": f"ACT-{incident['incident_id'][-8:]}", "title": title, "description": description,
                "knowledge_guidance": guidance,
                "answer_status": "relevant_evidence_requires_clarification" if reference_only else "documented_guidance_requires_review" if guidance else "insufficient_knowledge",
                "decision_basis": "Customer report plus retrieved, potentially unreviewed historical evidence; cause remains unconfirmed.",
                "verification": "Obtain customer/support confirmation and repeat the reported workflow after an authorized fix; healthy synthetic telemetry alone cannot verify customer resolution.",
                "target": incident.get("service", "customer-experience"), "mode": "manual_review_only",
            }
        hypothesis = rca.get("hypotheses", [{}])[0]
        title, description = self.ACTIONS.get(
            hypothesis.get("id"),
            ("Inspect the affected service", "Review the failed trace and dependency events before choosing a remediation."),
        )
        service = str(incident.get("service", "unknown"))
        events = incident.get("evidence", {}).get("trace_events", [])
        primary_event = next(
            (event for event in events if event.get("service.name") == service),
            events[0] if events else {},
        )
        message = str(primary_event.get("message", "")).casefold()
        error_type = str(primary_event.get("error.type", "")).casefold()
        is_postgres_timeout = service in {"postgresql", "postgres", "database"} and (
            "timeout" in message or "timeout" in error_type
        )
        if is_postgres_timeout:
            title = "Inspect PostgreSQL query timeout"
            description = "Review the failed query, active connections, and pool waiters; compare them with the incident trace before changing limits."
        elif hypothesis.get("id") == "application_exception" and service == "vehicle-telemetry-api":
            title = "Inspect the telemetry failure path"
            description = "Review the failing trace and downstream dependency timings to identify where the request starts timing out."
        elif hypothesis.get("id") in {"log_failure", "alert_signal"}:
            service_label = service.replace("-", " ")
            title = f"Inspect {service_label} failure evidence"
            description = "Review the observed error and related trace events; verify the failing dependency before selecting a remediation."
        verification = {
            "database_query_timeout": "Confirm the failed query succeeds, connection usage is within limits, and request timeouts have stopped.",
            "database_pool_pressure": "Confirm active connections and pool waiters return below their alert thresholds.",
            "messaging_lag": "Confirm consumer processing resumes and queue lag trends down without increasing retries.",
            "deployment_with_symptoms": "Compare error rate and latency before and after the reviewed deployment action.",
            "application_exception": "Confirm the exception signature stops appearing and affected requests succeed.",
            "high_latency": "Confirm p95 latency returns toward its normal baseline while errors remain stable.",
        }.get(
            hypothesis.get("id"),
            "Confirm the observed error stops recurring and the affected service returns to its normal health range.",
        )
        if is_postgres_timeout:
            verification = "Confirm the query succeeds, connection usage is within limits, and request timeouts have stopped."
        return {
            "action_id": f"ACT-{incident['incident_id'][-8:]}",
            "title": title,
            "description": description,
            "decision_basis": hypothesis.get("rationale", "Selected from the incident's retained alert and trace evidence."),
            "verification": verification,
            "target": incident.get("service", "unknown"),
            "mode": "simulation_only",
        }