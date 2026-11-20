import json
import uuid

from sqlalchemy import select

from app.db import OutboxEvent, Product
from app.events import OrderCreated, OrderItem
from app.inventory_service import handle_order_created


def make_event(*items: tuple[str, int]) -> OrderCreated:
    return OrderCreated(
        order_id=uuid.uuid4(),
        customer_email="jo@example.com",
        items=[OrderItem(sku=s, quantity=q) for s, q in items],
    )


def stock(Session, sku: str) -> int:
    with Session() as s:
        return s.get(Product, sku).stock


def inventory_events(Session) -> list[dict]:
    with Session() as s:
        rows = s.scalars(select(OutboxEvent).where(OutboxEvent.topic == "inventory")).all()
        return [json.loads(r.payload) for r in rows]


def test_reserves_stock_and_emits_stock_reserved(Session):
    event = make_event(("KB-001", 3), ("KB-001", 2))  # duplicate SKUs are aggregated
    with Session() as s:
        assert handle_order_created(s, event) == "reserved"
    assert stock(Session, "KB-001") == 5
    [emitted] = inventory_events(Session)
    assert emitted["event_type"] == "StockReserved"
    assert emitted["order_id"] == str(event.order_id)


def test_rejects_when_insufficient_stock_without_touching_inventory(Session):
    event = make_event(("KB-001", 1), ("MN-003", 5))
    with Session() as s:
        assert handle_order_created(s, event) == "rejected"
    assert stock(Session, "KB-001") == 10  # all-or-nothing
    assert stock(Session, "MN-003") == 2
    [emitted] = inventory_events(Session)
    assert emitted["event_type"] == "StockRejected"
    assert "insufficient stock for MN-003" in emitted["reason"]


def test_rejects_unknown_sku(Session):
    with Session() as s:
        assert handle_order_created(s, make_event(("NOPE", 1))) == "rejected"
    assert "unknown sku NOPE" in inventory_events(Session)[0]["reason"]


def test_redelivered_event_is_processed_exactly_once(Session):
    event = make_event(("KB-001", 4))
    with Session() as s:
        assert handle_order_created(s, event) == "reserved"
    with Session() as s:  # Kafka redelivers the same message after a crash / rebalance
        assert handle_order_created(s, event) == "duplicate"
    assert stock(Session, "KB-001") == 6
    assert len(inventory_events(Session)) == 1
