# ADR 0002: Transactional outbox instead of publishing from request handlers

- **Status:** Accepted

## Context

Writing an order to PostgreSQL and publishing `OrderCreated` to Kafka are two separate systems.
Doing both in a request handler (a "dual write") fails in either order:

- commit then publish → crash in between = order saved, event lost, order stuck in `PENDING`;
- publish then commit → commit fails = event for an order that does not exist.

## Decision

Services write the event to an `outbox` table **in the same transaction** as the business change.
A separate `outbox-relay` process publishes unpublished rows to Kafka and marks them published only
after Kafka acknowledges them (`acks=all`, idempotent producer).

- Rows are claimed with `SELECT … FOR UPDATE SKIP LOCKED`, so multiple relay replicas can run.
- Rows are published in `id` order and keyed by `order_id`.

## Consequences

- **At-least-once publication**: a crash after Kafka acks but before the rows are marked publishes
  them again. Every consumer is therefore idempotent (ADR 0003).
- The API keeps accepting orders during a Kafka outage; the backlog is visible as
  `outbox_unpublished` in `/stats` and drains when Kafka recovers.
- Polling adds up to ~250 ms of latency. Change data capture (Debezium) would remove the polling,
  but needs another component to run; polling is enough at this scale.
- Published rows accumulate; production would add a retention job (e.g. delete rows published > 7 days ago).
