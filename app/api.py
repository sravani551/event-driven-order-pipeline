"""Order API (FastAPI). Accepts orders and returns 202 immediately; the rest is asynchronous.

The order row and its OrderCreated event are committed in a single transaction
(transactional outbox), so the API never has to talk to Kafka directly.
"""

from collections import Counter
from collections.abc import Iterator
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import settings
from app.db import Order, OutboxEvent, ProcessedEvent, Product, add_outbox, get_sessionmaker
from app.events import OrderCreated, OrderItem

app = FastAPI(title="Order API", version="1.0.0")


def get_session() -> Iterator[Session]:
    with get_sessionmaker()() as session:
        yield session


class CreateOrderRequest(BaseModel):
    customer_email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", max_length=255)
    items: list[OrderItem] = Field(min_length=1, max_length=50)


class OrderResponse(BaseModel):
    id: UUID
    status: str
    status_reason: str | None
    customer_email: str
    items: list[OrderItem]


def to_response(order: Order) -> OrderResponse:
    return OrderResponse(
        id=order.id,
        status=order.status,
        status_reason=order.status_reason,
        customer_email=order.customer_email,
        items=order.items,
    )


@app.post("/orders", status_code=202, response_model=OrderResponse)
def create_order(
    req: CreateOrderRequest,
    session: Session = Depends(get_session),
    idempotency_key: str | None = Header(default=None, max_length=128),
) -> OrderResponse:
    # Client retries with the same Idempotency-Key get the original order back.
    if idempotency_key:
        existing = session.scalar(select(Order).where(Order.idempotency_key == idempotency_key))
        if existing:
            return to_response(existing)

    order = Order(
        id=uuid4(),
        idempotency_key=idempotency_key,
        customer_email=req.customer_email,
        items=[i.model_dump() for i in req.items],
        status="PENDING",
    )
    event = OrderCreated(order_id=order.id, customer_email=req.customer_email, items=req.items)
    session.add(order)
    add_outbox(session, settings.orders_topic, key=str(order.id), event=event)
    session.commit()  # order + event, atomically
    return to_response(order)


@app.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: UUID, session: Session = Depends(get_session)) -> OrderResponse:
    order = session.get(Order, order_id)
    if order is None:
        raise HTTPException(404, "Order not found")
    return to_response(order)


@app.get("/products")
def list_products(session: Session = Depends(get_session)) -> list[dict]:
    return [
        {"sku": p.sku, "name": p.name, "stock": p.stock}
        for p in session.scalars(select(Product).order_by(Product.sku))
    ]


@app.get("/stats")
def stats(session: Session = Depends(get_session)) -> dict:
    """Pipeline health at a glance: order states, outbox lag, per-consumer throughput."""
    by_status = dict(session.execute(select(Order.status, func.count()).group_by(Order.status)).all())
    pending_outbox = session.scalar(
        select(func.count()).select_from(OutboxEvent).where(OutboxEvent.published_at.is_(None))
    )
    processed = Counter(
        dict(
            session.execute(
                select(ProcessedEvent.consumer, func.count()).group_by(ProcessedEvent.consumer)
            ).all()
        )
    )
    return {
        "orders_by_status": by_status,
        "outbox_unpublished": pending_outbox,
        "processed_by_consumer": dict(processed),
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
