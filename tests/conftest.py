import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, Product


@pytest.fixture
def Session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    maker = sessionmaker(engine, expire_on_commit=False)
    with maker() as s:
        s.add_all(
            [
                Product(sku="KB-001", name="Keyboard", stock=10),
                Product(sku="MN-003", name="Monitor", stock=2),
            ]
        )
        s.commit()
    return maker
