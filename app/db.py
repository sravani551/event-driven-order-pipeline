"""Database models plus the two patterns that make messaging reliable:

* Transactional outbox: business rows and the events describing them are written in
  ONE database transaction, so an event is never lost or published for a rolled-back change.
* Idempotent consumer: a (consumer, event_id) row is inserted in the same transaction as
  the consumer's side effects, so a redelivered message is detected and skipped.
"""

import uuid
from datetime import datetime
from functools import lru_cache

from pydantic import BaseModel
from sqlalchemy import JSON, DateTime, Integer, String, Text, Uuid, create_engine, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from app.config import settings


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), unique=True, nullable=True)
    customer_email: Mapped[str] = mapped_column(String(255))
    items: Mapped[list] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(32), default="PENDING")
    status_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Product(Base):
    __tablename__ = "products"

    sku: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    stock: Mapped[int] = mapped_column(Integer)


class OutboxEvent(Base):
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(128))
    key: Mapped[str] = mapped_column(String(128))
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    consumer: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


@lru_cache
def get_sessionmaker(url: str | None = None) -> sessionmaker[Session]:
    engine = create_engine(url or settings.database_url, pool_pre_ping=True)
    return sessionmaker(engine, expire_on_commit=False)


def add_outbox(session: Session, topic: str, key: str, event: BaseModel) -> None:
    """Stage an event for publishing. It becomes visible only if the caller commits."""
    session.add(
        OutboxEvent(
            topic=topic,
            key=key,
            event_type=getattr(event, "event_type", type(event).__name__),
            payload=event.model_dump_json(),
        )
    )


def mark_processed(session: Session, consumer: str, event_id: uuid.UUID | str) -> bool:
    """Claim an event for this consumer. Returns False if it was already processed.

    Call this FIRST in the transaction; commit it together with the side effects.
    """
    key = (consumer, str(event_id))
    if session.get(ProcessedEvent, key) is not None:
        return False
    session.add(ProcessedEvent(consumer=consumer, event_id=str(event_id)))
    try:
        session.flush()
    except IntegrityError:  # another instance claimed it concurrently (e.g. during a rebalance)
        session.rollback()
        return False
    return True
