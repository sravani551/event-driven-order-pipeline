# ADR 0004: Retry and dead-letter strategy per broker

- **Status:** Accepted

## Decision

| | Kafka consumers | RabbitMQ notification worker |
|---|---|---|
| Transient error | Retry **in place** with exponential backoff (0.5s, 1s, 2s; 4 attempts), blocking the partition | Re-publish to a **delay queue** (5s → 30s → 2m), ack the original |
| Poison message (schema violation) | Straight to `orders.dlq` with error + source topic/partition/offset headers | `nack(requeue=false)` → DLX → `notifications.email.dlq` |
| Retries exhausted | `orders.dlq` | `notifications.email.dlq` with `x-attempt` and `x-last-error` headers |
| Recovery | Re-produce from the DLQ topic after the fix | `scripts/replay_email_dlq.py --replay` |

## Rationale

- **Kafka:** skipping a failed message to retry it later would break per-order ordering, so retries
  block the partition. They are kept short so the consumer stays well inside `max.poll.interval.ms`.
- **RabbitMQ:** one queue per delay tier, each with a queue-level `x-message-ttl` that dead-letters
  back to the work exchange. A single queue with per-message TTLs would suffer head-of-line
  blocking: RabbitMQ only expires messages at the head of a queue.
- Retry copies are published with publisher confirms **before** the original is acked, so a crash
  can duplicate a message (absorbed by idempotency) but never lose one.
- Retry state travels in message headers (`x-attempt`), so workers stay stateless.
