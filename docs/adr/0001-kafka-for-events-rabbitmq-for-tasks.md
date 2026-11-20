# ADR 0001: Kafka for domain events, RabbitMQ for tasks

- **Status:** Accepted
- **Context date:** 2026-11

## Context

The pipeline has two kinds of messages with different needs:

1. **Domain events** (`OrderCreated`, `StockReserved`, `StockRejected`): facts that several
   services may react to, now or later. These need ordering per order, replay (e.g. to rebuild
   a read model or onboard a new consumer), and horizontal scaling.
2. **Tasks** (`EmailTask`): commands to do one unit of work exactly once from the business's
   point of view. These need per-message acknowledgement, delayed retries, and a place to park
   failures. Ordering does not matter.

## Decision

- Use **Kafka** for domain events. Topics are keyed by `order_id` so all events of one order
  land in the same partition and are consumed in order. Consumer groups give horizontal scaling
  bounded by partition count (3 by default).
- Use **RabbitMQ** for tasks. A quorum work queue with competing consumers, manual acks and
  `prefetch_count` gives fair dispatch and back-pressure; TTL delay queues give retries without
  blocking other work.

## Consequences

- Two brokers to operate instead of one. Acceptable here: each one handles the job it is designed for.
- Kafka retries must be **in place** (blocking the partition) to keep ordering, so the Kafka
  retry budget is kept short and a DLQ topic catches anything that still fails.
- RabbitMQ retries can be **out of band** (delay queues), so a failing email never delays others.

## Alternatives considered

- **Kafka only** (retry topics for emails): works, but delayed retry topics need extra consumers
  and pausing logic that RabbitMQ provides out of the box with TTL + DLX.
- **RabbitMQ only** (streams or topic exchanges for events): loses Kafka's long retention, replay
  by offset, and partition-based ordering guarantees at scale.
