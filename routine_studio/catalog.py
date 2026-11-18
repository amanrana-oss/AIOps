"""Seed metadata for the existing agents and local routine templates."""

from __future__ import annotations

from typing import Any

AGENT_CATALOG = [
    {"agent_id":"AG-01","name":"Orchestrator","description":"Selects and coordinates an incident routine.","responsibilities":["Alert routing","Routine selection","Incident persistence"],"instructions":"Choose a routine from alert evidence; do not claim a cause.","micro_agents":["MA-01","MA-02"],"tools":["tool.inspect_incident"],"memory":{"incident":True,"historical":True},"model":{"provider":"none","model":"deterministic"},"status":"active"},
    {"agent_id":"AG-02","name":"Knowledge Base","description":"Retrieves local logs and cited SOP evidence.","responsibilities":["SOP retrieval","Log retrieval","Evidence governance"],"instructions":"Return cited evidence and limitations; never present retrieval as diagnosis.","micro_agents":["MA-03","MA-04","MA-05","MA-06","MA-07"],"tools":["tool.search_logs","tool.search_sops","tool.inspect_dependencies"],"memory":{"incident":True,"historical":True},"model":{"provider":"none","model":"deterministic"},"status":"active"},
    {"agent_id":"AG-08","name":"Monitoring","description":"Processes normalized synthetic events and emits alert candidates.","responsibilities":["Collection","Normalization","Health and anomaly detection","Alert candidates"],"instructions":"Emit evidence-backed alerts; do not infer a root cause.","micro_agents":[f"MA-{number}" for number in range(9,16)],"tools":["tool.get_metrics","tool.inspect_traces"],"memory":{"incident":False,"historical":False},"model":{"provider":"none","model":"deterministic"},"status":"active"},
    {"agent_id":"AG-16","name":"Correlation","description":"Correlates event time, dependency observations, and failure-pattern rules.","responsibilities":["Timeline analysis","Dependency context","Failure-pattern rules"],"instructions":"Report matching rules as candidates, never causal proof.","micro_agents":["MA-17","MA-18","MA-19"],"tools":["tool.inspect_traces","tool.inspect_dependencies"],"memory":{"incident":True,"historical":False},"model":{"provider":"none","model":"deterministic"},"status":"active"},
    {"agent_id":"AG-20","name":"RCA Hypotheses","description":"Produces evidence-linked candidate explanations.","responsibilities":["Generate hypotheses","Validate evidence references"],"instructions":"Keep causality unconfirmed and cite event references.","micro_agents":["MA-21","MA-22"],"tools":["tool.search_historical_incidents"],"memory":{"incident":True,"historical":True},"model":{"provider":"ollama","model":"qwen3:8b","enabled":False},"status":"active"},
    {"agent_id":"AG-23","name":"Resolution Planner","description":"Suggests a bounded response and requires approval before dry run.","responsibilities":["Plan response","Safety gate","Simulated execution"],"instructions":"Prefer inspect/verify guidance; never invoke real infrastructure actions.","micro_agents":["MA-24","MA-25","MA-26"],"tools":["tool.generate_remediation_plan","tool.simulate_recovery","tool.verify_recovery"],"memory":{"incident":True,"historical":True},"model":{"provider":"ollama","model":"qwen3:8b","enabled":False},"status":"active"},
    {"agent_id":"AG-27","name":"Local Ticketing","description":"Creates local-only incident tickets on operator request.","responsibilities":["Enrich local ticket","Validate ticket lifecycle"],"instructions":"Never contact external ticketing systems.","micro_agents":["MA-28","MA-29"],"tools":["tool.create_local_ticket"],"memory":{"incident":True,"historical":False},"model":{"provider":"none","model":"deterministic"},"status":"active"},
]

_MICRO_AGENT_ROWS = [
    ("MA-01","AG-01","Alert grouping","Group trace-correlated alert candidates.","alert_group","grouped_alerts"),
    ("MA-02","AG-01","Routine selector","Select an enabled routine using deterministic trigger rules.","incident_alert","routine_selection"),
    ("MA-03","AG-02","Document ingestion","Parse and chunk local SOP documents.","document_files","indexed_chunks"),
    ("MA-04","AG-02","Document retrieval","Retrieve cited local SOP passages.","query","cited_passages"),
    ("MA-05","AG-02","Log retrieval","Search bounded local or configured OpenSearch logs.","log_query","log_evidence"),
    ("MA-06","AG-02","Knowledge graph","Read the configured local service graph.","service_name","dependency_context"),
    ("MA-07","AG-02","Knowledge governance","Flag provenance and review requirements.","retrieved_evidence","governance_flags"),
    ("MA-09","AG-08","Source collector","Accept events through bounded local connectors.","event_envelope","queued_event"),
    ("MA-10","AG-08","Normalizer","Normalize event fields and quality warnings.","raw_event","normalized_event"),
    ("MA-11","AG-08","Service health","Update rolling service health metrics.","normalized_event","health_metrics"),
    ("MA-12","AG-08","Log/trace analyzer","Group errors and retain bounded trace context.","normalized_event","failure_signal"),
    ("MA-13","AG-08","Anomaly detector","Apply deterministic thresholds and baselines.","normalized_event","anomaly_signals"),
    ("MA-14","AG-08","Event detector","Deduplicate alert candidates.","signals","alert_candidates"),
    ("MA-15","AG-08","Feedback analyzer","Redact and summarize synthetic feedback.","review_text","feedback_summary"),
    ("MA-17","AG-16","Temporal correlator","Order evidence and identify nearby changes.","incident_evidence","timeline"),
    ("MA-18","AG-16","Dependency analyzer","Summarize observed links in local service map.","incident_evidence","dependency_links"),
    ("MA-19","AG-16","Failure pattern scorer","Apply explainable failure-pattern rules.","incident_evidence","pattern_candidates"),
    ("MA-21","AG-20","Hypothesis generator","Generate evidence-linked candidate explanations.","incident_evidence","hypotheses"),
    ("MA-22","AG-20","Hypothesis validator","Validate referenced events and evidence coverage.","hypotheses","validated_hypotheses"),
    ("MA-24","AG-23","Runbook planner","Suggest a bounded operational inspection step.","incident_and_hypothesis","response_plan"),
    ("MA-25","AG-23","Safety gate","Require human approval for any simulated action.","response_plan","gated_plan"),
    ("MA-26","AG-23","Dry-run executor","Record a local simulation; never modify external systems.","approved_plan","simulation_result"),
    ("MA-28","AG-27","Ticket enricher","Create a local-only incident ticket draft.","incident","local_ticket"),
    ("MA-29","AG-27","Ticket lifecycle","Validate local ticket state transitions.","ticket_and_status","updated_ticket"),
]
MICRO_AGENT_CATALOG = [
    {"micro_agent_id":row[0],"parent_agent_id":row[1],"name":row[2],"objective":row[3],"input_schema":{"type":"object","required_context":row[4]},"output_schema":{"type":"object","result":row[5]},"tools":[],"instructions":row[3],"validation_rules":["Output must be JSON serializable","Causality remains unconfirmed unless an explicitly reviewed test says otherwise"],"timeout_seconds":30,"retry_policy":{"max_attempts":1,"backoff_seconds":0},"status":"active"}
    for row in _MICRO_AGENT_ROWS
]

TOOL_CATALOG = [
    {"tool_id":"tool.search_logs","name":"Search logs","description":"Search retained synthetic/local operational logs.","input_schema":{"type":"object","query":"string"},"output_schema":{"type":"object","items":"array"},"permission":"read_local","handler":"search_logs"},
    {"tool_id":"tool.get_metrics","name":"Get metrics","description":"Read simulator and rolling service metrics.","input_schema":{"type":"object","service":"string?"},"output_schema":{"type":"object","metrics":"object"},"permission":"read_local","handler":"get_metrics"},
    {"tool_id":"tool.inspect_traces","name":"Inspect traces","description":"Read locally retained events for a trace ID.","input_schema":{"type":"object","trace_id":"string"},"output_schema":{"type":"object","events":"array"},"permission":"read_local","handler":"inspect_traces"},
    {"tool_id":"tool.inspect_dependencies","name":"Inspect dependencies","description":"Read the simulator's configured dependency map.","input_schema":{"type":"object","service":"string"},"output_schema":{"type":"object","dependencies":"array"},"permission":"read_local","handler":"inspect_dependencies"},
    {"tool_id":"tool.search_sops","name":"Search SOPs","description":"Retrieve cited local SOP passages.","input_schema":{"type":"object","query":"string"},"output_schema":{"type":"object","evidence":"array"},"permission":"read_local","handler":"search_sops"},
    {"tool_id":"tool.search_historical_incidents","name":"Search incidents","description":"Search local incident summaries by text.","input_schema":{"type":"object","query":"string"},"output_schema":{"type":"object","incidents":"array"},"permission":"read_local","handler":"search_incidents"},
    {"tool_id":"tool.generate_remediation_plan","name":"Generate remediation plan","description":"Create a deterministic, approval-gated response plan.","input_schema":{"type":"object","incident_id":"string"},"output_schema":{"type":"object","actions":"array"},"permission":"plan_only","handler":"plan_response"},
    {"tool_id":"tool.simulate_recovery","name":"Simulate recovery","description":"Run the local simulator's recovery sequence.","input_schema":{"type":"object"},"output_schema":{"type":"object","mode":"simulation_only"},"permission":"simulation_only","handler":"simulate_recovery"},
    {"tool_id":"tool.verify_recovery","name":"Verify recovery","description":"Read current local service health to evaluate a recovery check.","input_schema":{"type":"object","service":"string?"},"output_schema":{"type":"object","health":"object"},"permission":"read_local","handler":"verify_recovery"},
    {"tool_id":"tool.create_local_ticket","name":"Create local ticket","description":"Create a ticket only inside the local POC database.","input_schema":{"type":"object","incident_id":"string"},"output_schema":{"type":"object","ticket":"object"},"permission":"operator_action","handler":"create_ticket"},
]

ROUTINE_DEFINITIONS: list[dict[str, Any]] = [
    {"routine_id":"routine.database_failure","name":"Database Failure Investigation","description":"Investigate database errors and pool pressure.","trigger":{"services":["postgresql","postgres","database"],"keywords":["query timeout","connection pool","database","postgres"]},"enabled":True,"nodes":[
        {"id":"collect","type":"agent","agent_id":"AG-02","operation":"collect_evidence","label":"Collect logs and SOP evidence","timeout_seconds":30,"retries":1},
        {"id":"correlate","type":"agent","agent_id":"AG-16","operation":"correlate","label":"Correlate database and dependent-service evidence","timeout_seconds":30,"retries":1},
        {"id":"hypothesis","type":"agent","agent_id":"AG-20","operation":"root_cause","label":"Generate database failure hypotheses","timeout_seconds":30,"retries":1},
        {"id":"response","type":"agent","agent_id":"AG-23","operation":"resolution","label":"Recommend safe database checks","timeout_seconds":30,"retries":0},
        {"id":"approval","type":"approval","agent_id":"AG-23","operation":"approval_checkpoint","label":"Wait for operator approval","timeout_seconds":0,"retries":0},
        {"id":"ticket","type":"agent","agent_id":"AG-27","operation":"ticket_handoff","label":"Wait for optional ticket handoff","timeout_seconds":0,"retries":0,"requires_operator":True}],"trigger_label":"Database failure"},
    {"routine_id":"routine.api_failure","name":"API Failure Investigation","description":"Investigate API errors and request failures.","trigger":{"services":["api","api-gateway","vehicle-telemetry-api"],"keywords":["http 5","request failed","exception","api"]},"enabled":True,"nodes":[
        {"id":"collect","type":"agent","agent_id":"AG-02","operation":"collect_evidence","label":"Collect request logs and SOP evidence","timeout_seconds":30,"retries":1},
        {"id":"correlate","type":"agent","agent_id":"AG-16","operation":"correlate","label":"Correlate request and dependency evidence","timeout_seconds":30,"retries":1},
        {"id":"hypothesis","type":"agent","agent_id":"AG-20","operation":"root_cause","label":"Generate API failure hypotheses","timeout_seconds":30,"retries":1},
        {"id":"response","type":"agent","agent_id":"AG-23","operation":"resolution","label":"Recommend safe API checks","timeout_seconds":30,"retries":0},
        {"id":"approval","type":"approval","agent_id":"AG-23","operation":"approval_checkpoint","label":"Wait for operator approval","timeout_seconds":0,"retries":0},
        {"id":"ticket","type":"agent","agent_id":"AG-27","operation":"ticket_handoff","label":"Wait for optional ticket handoff","timeout_seconds":0,"retries":0,"requires_operator":True}],"trigger_label":"API failure"},
    {"routine_id":"routine.performance_degradation","name":"Performance Degradation Investigation","description":"Investigate latency, saturation and throughput changes.","trigger":{"signal_types":["threshold_anomaly","baseline_anomaly"],"keywords":["latency","slow","throughput","saturation"]},"enabled":True,"nodes":[
        {"id":"metrics","type":"tool","tool_id":"tool.get_metrics","operation":"get_metrics","label":"Read service health metrics","timeout_seconds":15,"retries":1},
        {"id":"collect","type":"agent","agent_id":"AG-02","operation":"collect_evidence","label":"Collect latency traces and SOP evidence","timeout_seconds":30,"retries":1},
        {"id":"correlate","type":"agent","agent_id":"AG-16","operation":"correlate","label":"Correlate latency and dependency changes","timeout_seconds":30,"retries":1},
        {"id":"hypothesis","type":"agent","agent_id":"AG-20","operation":"root_cause","label":"Generate performance hypotheses","timeout_seconds":30,"retries":1},
        {"id":"response","type":"agent","agent_id":"AG-23","operation":"resolution","label":"Recommend verification checks","timeout_seconds":30,"retries":0}],"trigger_label":"Performance degradation"},
    {"routine_id":"routine.infrastructure_failure","name":"Infrastructure Failure Investigation","description":"Investigate simulated infrastructure health and resource pressure.","trigger":{"services":["kubernetes","infrastructure"],"keywords":["memory","disk","restart","container","infrastructure"]},"enabled":True,"nodes":[
        {"id":"metrics","type":"tool","tool_id":"tool.get_metrics","operation":"get_metrics","label":"Read resource and health metrics","timeout_seconds":15,"retries":1},
        {"id":"collect","type":"agent","agent_id":"AG-02","operation":"collect_evidence","label":"Collect infrastructure logs and SOP evidence","timeout_seconds":30,"retries":1},
        {"id":"correlate","type":"agent","agent_id":"AG-16","operation":"correlate","label":"Correlate service and platform evidence","timeout_seconds":30,"retries":1},
        {"id":"hypothesis","type":"agent","agent_id":"AG-20","operation":"root_cause","label":"Generate infrastructure hypotheses","timeout_seconds":30,"retries":1},
        {"id":"response","type":"agent","agent_id":"AG-23","operation":"resolution","label":"Recommend safe health checks","timeout_seconds":30,"retries":0}],"trigger_label":"Infrastructure failure"},
    {"routine_id":"routine.deployment_regression","name":"Deployment Regression Investigation","description":"Investigate symptoms near a simulated deployment or release.","trigger":{"keywords":["deployment","release","rollback"],"signal_types":["log_failure","threshold_anomaly"]},"enabled":True,"nodes":[
        {"id":"changes","type":"micro_agent","agent_id":"AG-16","micro_agent_id":"MA-17","operation":"correlate","label":"Inspect nearby release changes","timeout_seconds":20,"retries":1},
        {"id":"collect","type":"agent","agent_id":"AG-02","operation":"collect_evidence","label":"Collect post-release evidence","timeout_seconds":30,"retries":1},
        {"id":"hypothesis","type":"agent","agent_id":"AG-20","operation":"root_cause","label":"Assess release-related hypotheses","timeout_seconds":30,"retries":1},
        {"id":"approval","type":"approval","agent_id":"AG-23","operation":"approval_checkpoint","label":"Require operator review before rollback consideration","timeout_seconds":0,"retries":0}],"trigger_label":"Deployment regression"},
    {"routine_id":"routine.cascading_failure","name":"Cascading Failure Investigation","description":"Investigate a multi-service failure spanning observed dependencies.","trigger":{"minimum_related_signals":2,"keywords":["cascade","upstream","downstream"]},"enabled":True,"nodes":[
        {"id":"collect","type":"agent","agent_id":"AG-02","operation":"collect_evidence","label":"Collect related traces and logs","timeout_seconds":30,"retries":1},
        {"id":"deps","type":"micro_agent","agent_id":"AG-16","micro_agent_id":"MA-18","operation":"correlate","label":"Map observed service dependencies","timeout_seconds":20,"retries":1},
        {"id":"correlate","type":"agent","agent_id":"AG-16","operation":"correlate","label":"Score cross-service failure patterns","timeout_seconds":30,"retries":1},
        {"id":"hypothesis","type":"agent","agent_id":"AG-20","operation":"root_cause","label":"Generate cascade hypotheses","timeout_seconds":30,"retries":1},
        {"id":"response","type":"agent","agent_id":"AG-23","operation":"resolution","label":"Recommend containment checks","timeout_seconds":30,"retries":0}],"trigger_label":"Cascading failure"},
    {"routine_id":"routine.incident_learning","name":"Incident Learning Routine","description":"Prepare a human-reviewed learning candidate from a closed incident.","trigger":{"statuses":["closed"],"keywords":["learning","postmortem"]},"enabled":True,"nodes":[
        {"id":"collect","type":"agent","agent_id":"AG-02","operation":"collect_evidence","label":"Collect approved incident evidence","timeout_seconds":30,"retries":1},
        {"id":"review","type":"approval","agent_id":"AG-02","operation":"approval_checkpoint","label":"Require human review before learning ingestion","timeout_seconds":0,"retries":0},
        {"id":"learning","type":"tool","tool_id":"tool.search_sops","operation":"incident_learning_candidate","label":"Prepare pending learning candidate","timeout_seconds":30,"retries":0}],"trigger_label":"Incident learning"},
]


def default_routines() -> list[dict[str, Any]]:
    """Return a deep-ish copy of seed definitions for independent persistence."""
    import json

    return json.loads(json.dumps(ROUTINE_DEFINITIONS))
