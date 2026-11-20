"""Inventory service: Kafka consumer group on the `orders` topic.

Run several replicas (`docker compose up --scale inventory=3`) and Kafka spreads the
topic's partitions across them. For each OrderCreated it, in ONE transaction:
  1. claims the event id (idempotency),
  2. locks the product rows and reserves stock (or rejects the order),
  3. writes a StockReserved / StockRejected event to the outbox.
Only after that commit is the Kafka offset committed.
"""

from collections import Counter

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.db import Product, add_outbox, get_sessionmaker, mark_processed
from app.events import OrderCreated, StockRejected, StockReserved
from app.logging_setup import setup_logging, stop_event_on_signals

CONSUMER = "inventory-service"
log = setup_logging(CONSUMER)


def handle_order_created(session: Session, event: OrderCreated) -> str:
    """Returns 'reserved', 'rejected' or 'duplicate'."""
    if not mark_processed(session, CONSUMER, event.event_id):
        return "duplicate"

    wanted = Counter()
    for item in event.items:
        wanted[item.sku] += item.quantity

    # Lock rows in a stable order to avoid deadlocks between concurrent consumers.
    products = {
        p.sku: p
        for p in session.scalars(
            select(Product).where(Product.sku.in_(sorted(wanted))).order_by(Product.sku).with_for_update()
        )
    }
    problems = []
    for sku, qty in sorted(wanted.items()):
        product = products.get(sku)
        if product is None:
            problems.append(f"unknown sku {sku}")
        elif product.stock < qty:
            problems.append(f"insufficient stock for {sku} (wanted {qty}, have {product.stock})")

    if problems:
        result = StockRejected(order_id=event.order_id, reason="; ".join(problems))
        outcome = "rejected"
    else:
        for sku, qty in wanted.items():
            products[sku].stock -= qty
        result = StockReserved(order_id=event.order_id)
        outcome = "reserved"

    add_outbox(session, settings.inventory_topic, key=str(event.order_id), event=result)
    session.commit()
    return outcome


def main() -> None:
    from app.kafka_utils import PoisonMessage, run_consumer

    Session = get_sessionmaker()

    def handle(msg) -> None:
        try:
            event = OrderCreated.model_validate_json(msg.value())
        except ValidationError as exc:
            raise PoisonMessage(f"invalid OrderCreated: {exc.errors()[:2]}") from exc
        with Session() as session:
            outcome = handle_order_created(session, event)
        log.info("order=%s partition=%s -> %s", event.order_id, msg.partition(), outcome)

    run_consumer(CONSUMER, [settings.orders_topic], handle, stop_event_on_signals())


if __name__ == "__main__":
    main()
