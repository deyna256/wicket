"""The PostgreSQL store, on SQLAlchemy's asyncio Core: a Backlog on the caller's session, a Ledger
on an engine. Install with `wicket[postgres]`; add `metadata` to your migrations."""

from sqlalchemy import BigInteger, Column, Index, Integer, MetaData, Table, Text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP

metadata = MetaData()

_pools = Table(
    "wicket_pools",
    metadata,
    Column("name", Text, primary_key=True),
    Column("version", BigInteger, nullable=False),
    Column("fairness", JSONB, nullable=False),
)
_tickets = Table(
    "wicket_tickets",
    metadata,
    Column("pool", Text, primary_key=True),
    Column("id", Text, primary_key=True),
    Column("tenant", Text, nullable=False),
    Column("traits", JSONB, nullable=False),
    Column("submitted_at", TIMESTAMP(timezone=True), nullable=False),
    Column("grants", Integer, nullable=False),
    Column("granted_at", TIMESTAMP(timezone=True)),
    Column("lease_end", TIMESTAMP(timezone=True)),
    Column("token", BigInteger),
)
_grants = Table(
    "wicket_grants",
    metadata,
    Column("pool", Text, nullable=False),
    Column("at", TIMESTAMP(timezone=True), nullable=False),
    Index("wicket_grants_pool_at", "pool", "at"),
)
