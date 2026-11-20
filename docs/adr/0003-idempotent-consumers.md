# ADR 0003: Idempotent consumers over broker-level exactly-once

- **Status:** Accepted

## Context

Both brokers deliver at-least-once: rebalances, crashes before an offset commit, and outbox
re-publication all cause redelivery. Kafka transactions give exactly-once only for
Kafka-to-Kafka flows. Our consumers write to PostgreSQL and RabbitMQ, which Kafka transactions do not cover.

## Decision

Every consumer inserts `(consumer, event_id)` into `processed_events` **in the same database
transaction** as its side effects. If the row already exists, the message is acknowledged and skipped.

- Kafka offsets are committed manually, only after that transaction commits.
- The Kafka → RabbitMQ bridge (`status-updater`) publishes the email task **with a publisher confirm
  before** committing the DB transaction. The task id is `uuid5(event_id)`, so a redelivered event
  produces the identical task, which the notification worker de-duplicates the same way.

## Consequences

- Exactly-once *effects* in our own state, built from at-least-once delivery.
- External side effects (the actual email) remain at-least-once in one narrow window: provider
  call succeeded, DB commit failed. Real providers solve this by accepting an idempotency key;
  `task_id` is designed to be passed as that key.
- `processed_events` grows without bound; production would partition it by date or expire rows
  past the maximum redelivery horizon.
