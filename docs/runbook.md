# Operations runbook

## Health signals

| Signal | Where | Healthy | Investigate when |
|---|---|---|---|
| Outbox backlog | `GET /stats` → `outbox_unpublished` | ~0 | Growing steadily: relay down or Kafka unreachable |
| Consumer lag | Kafka UI → Consumers, or `kafka-consumer-groups.sh --describe --group inventory-service` | Near 0 | Growing: scale `inventory` (up to partition count) or check logs for retry loops |
| Orders stuck `PENDING` | `GET /stats` → `orders_by_status` | Transient only | Any order `PENDING` for > 1 min |
| Kafka DLQ | Topic `orders.dlq` | Empty | Any message: inspect `dlq.error` header |
| Email DLQ | RabbitMQ UI → `notifications.email.dlq` | Empty | Any message: inspect `x-last-error` header |
| Retry queues | `notifications.email.retry.*` depth | Small, draining | Large: email provider degraded |

## Playbooks

### Kafka is down
Orders keep being accepted and wait in the outbox. No action needed beyond restoring Kafka;
the relay drains the backlog in order. Watch `outbox_unpublished` return to 0.

### Consumer lag is growing
```bash
docker compose up -d --scale inventory=3   # max useful = partition count (3)
```
For more throughput, add partitions. Note: this changes the key → partition mapping for **new**
messages, so do it when no order is mid-flight, or accept a short window of cross-partition reordering.

### Emails piling up in the DLQ
1. Read the errors: `python scripts/replay_email_dlq.py` (dry run; messages stay in the queue).
2. Fix the root cause (provider credentials, template bug, ...).
3. Replay: `python scripts/replay_email_dlq.py --replay` (resets the retry budget; safe to re-run
   because the worker is idempotent).

### Poison message in `orders.dlq`
Messages there failed schema validation or exhausted retries. Inspect the headers, fix the
producer or consumer, then re-produce the fixed payload to `orders` with the same key. Consumers
are idempotent by `event_id`, so replaying an already-handled event is harmless.

### Deploying a consumer
Rolling restarts are safe: SIGTERM finishes the in-flight message, commits its offset and leaves
the group. Cooperative-sticky assignment keeps rebalances incremental.
