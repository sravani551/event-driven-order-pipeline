"""Order status updater: consumes `inventory` events from Kafka, updates the order,
and hands the customer email off to RabbitMQ as a task.

Bridge ordering (Kafka -> DB -> RabbitMQ) for at-least-once without lost emails:
  1. apply the change and claim the event id in the DB transaction (flushed, not committed)
  2. publish the EmailTask to RabbitMQ and wait for the publisher confirm
  3. commit the DB transaction
  4. commit the Kafka offset (done by run_consumer)
If we crash between 2 and 3 the event is redelivered and the email task is published
again with the SAME deterministic task_id, which the notification worker de-duplicates.
"""

from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.db import Order, get_sessionmaker, mark_processed
from app.events import EmailTask, InventoryEvent, StockReserved, inventory_event_adapter
from app.logging_setup import setup_logging, stop_event_on_signals

CONSUMER = "order-status-updater"
log = setup_logging(CONSUMER)


class UnknownOrder(Exception):
    pass


def email_task_id(event_id) -> str:
    return str(uuid5(NAMESPACE_URL, f"email:{event_id}"))


def apply_inventory_event(session: Session, event: InventoryEvent) -> EmailTask | None:
    """Update the order (flush only). Returns the email to send, or None for duplicates."""
    if not mark_processed(session, CONSUMER, event.event_id):
        return None
    order = session.get(Order, event.order_id, with_for_update=True)
    if order is None:
        raise UnknownOrder(f"order {event.order_id} not found")

    if isinstance(event, StockReserved):
        order.status, order.status_reason = "CONFIRMED", None
        template, context = "order_confirmed", {}
    else:
        order.status, order.status_reason = "REJECTED", event.reason
        template, context = "order_rejected", {"reason": event.reason}
    session.flush()

    return EmailTask(
        task_id=email_task_id(event.event_id),
        order_id=order.id,
        to=order.customer_email,
        template=template,
        context=context,
    )


def main() -> None:
    from app.config import settings
    from app.kafka_utils import PoisonMessage, run_consumer
    from app.rabbit import ConfirmedPublisher

    Session = get_sessionmaker()
    publisher = ConfirmedPublisher()

    def handle(msg) -> None:
        try:
            event = inventory_event_adapter.validate_json(msg.value())
        except ValidationError as exc:
            raise PoisonMessage(f"invalid inventory event: {exc.errors()[:2]}") from exc
        with Session() as session:
            try:
                task = apply_inventory_event(session, event)
            except UnknownOrder as exc:
                raise PoisonMessage(str(exc)) from exc
            if task is None:
                log.info("Duplicate event %s skipped", event.event_id)
                return
            publisher.publish_email(task)  # waits for broker confirm
            session.commit()
        log.info("order=%s -> %s, email task %s queued", event.order_id, event.event_type, task.task_id)

    run_consumer(
        CONSUMER, [settings.inventory_topic], handle, stop_event_on_signals(), on_idle=publisher.keepalive
    )


if __name__ == "__main__":
    main()
