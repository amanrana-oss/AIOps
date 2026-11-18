# Synthetic Operations Triage Guide

Status: demonstration example. This document is not an approved production runbook.

## Elevated service latency

1. Confirm the affected service and alert time window.
2. Compare request traces with dependency timings and recent deployment events.
3. Check database connection pool utilization and messaging queue lag.
4. Record event IDs and the time range used for the investigation.
5. Escalate to the service owner before changing configuration or restarting services.

## Database connection pool pressure

1. Review active and maximum database connections, acquisition wait time, and query errors.
2. Check whether pressure began after a deployment or traffic change.
3. Do not raise pool limits without checking database capacity and service concurrency.
4. Any restart, rollback, or configuration change requires an authorized operator.

## Consumer lag

1. Compare queue depth, consumer throughput, retries, and downstream availability.
2. Check whether lag is isolated to one consumer group or shared across services.
3. Validate capacity and idempotency before proposing a scale-out or replay.
4. Preserve offsets and event references for the incident record.