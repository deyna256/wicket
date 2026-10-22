"""The PostgreSQL store, on SQLAlchemy's asyncio Core: a Backlog on the caller's session, a Ledger
on an engine. Install with `wicket[postgres]`; add `metadata` to your migrations."""

import contextlib
from collections.abc import AsyncGenerator, Awaitable
from datetime import UTC, timedelta
from typing import Any

import msgspec
from sqlalchemy import (
    BigInteger,
    Column,
    ColumnElement,
    Index,
    Integer,
    MetaData,
    Row,
    Table,
    Text,
    delete,
    exc,
    func,
    insert,
    literal,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import insert as upsert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from wicket.fairness import Fairness
from wicket.results import (
    AlreadyLeased,
    Conflict,
    Gone,
    LeaseLost,
    Missing,
    Released,
    Renewed,
    StoreUnavailable,
    Withdrawn,
)
from wicket.tickets import Permit, Ticket, Version, Waiting
from wicket.view import PoolView

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

_COMMITTED = {"isolation_level": "READ COMMITTED"}
_SNAPSHOT = {"isolation_level": "REPEATABLE READ", "postgresql_readonly": True}


class _Reach(msgspec.Struct, frozen=True):
    """Where a driver's error becomes StoreUnavailable: any error while connecting, since that runs
    no SQL of ours, and after that only an error SQLAlchemy found the connection lost on."""

    async def connect(self, connecting: Awaitable[AsyncConnection]) -> AsyncConnection:
        try:
            return await connecting
        except Exception as error:
            raise StoreUnavailable(f"the database cannot be reached: {error}") from error

    @contextlib.asynccontextmanager
    async def executing(self) -> AsyncGenerator[None, None]:
        try:
            yield
        except exc.DBAPIError as error:
            if error.connection_invalidated:
                raise StoreUnavailable(
                    f"the connection to the database was lost: {error.orig}"
                ) from error
            raise


class PostgresBacklog(msgspec.Struct, frozen=True):
    """Submits and withdraws on the caller's session, in the caller's transaction; never commits."""

    session: AsyncSession

    async def submit(self, pool: str, ticket: Ticket) -> None:
        async with _Reach().executing():
            await _Reach().connect(self.session.connection())  # a session connects lazily
            await self.session.execute(
                upsert(_tickets)
                .values(
                    pool=pool,
                    id=ticket.id,
                    tenant=ticket.tenant,
                    traits=dict(ticket.traits),
                    submitted_at=func.clock_timestamp(),
                    grants=0,
                )
                .on_conflict_do_nothing()
            )

    async def withdraw(self, pool: str, ticket_id: str) -> Withdrawn | AlreadyLeased | Gone:
        async with _Reach().executing():
            await _Reach().connect(self.session.connection())
            removed = await self.session.execute(
                delete(_tickets)
                .where(
                    _tickets.c.pool == pool,
                    _tickets.c.id == ticket_id,
                    or_(
                        _tickets.c.lease_end.is_(None),
                        _tickets.c.lease_end <= func.clock_timestamp(),
                    ),
                )
                .returning(_tickets.c.id)
            )
            if removed.first() is not None:
                return Withdrawn()
            known = await self.session.execute(
                select(_tickets.c.id).where(_tickets.c.pool == pool, _tickets.c.id == ticket_id)
            )
            return AlreadyLeased() if known.first() is not None else Gone()


class PostgresLedger(msgspec.Struct, frozen=True):
    """The pool's side, each call in a short transaction of its own on a connection from `engine`.
    Every process serving a pool passes the same `retention`: a grant deletes older history."""

    engine: AsyncEngine
    retention: timedelta

    def __post_init__(self) -> None:
        if self.retention <= timedelta(0):
            raise ValueError(f"retention must be positive, not {self.retention}")

    async def view(self, pool: str) -> PoolView:
        async with self._connection(_SNAPSHOT) as connection:
            # now(), the transaction's start, comes before the snapshot: with clock_timestamp(), a
            # renewal committed after the snapshot would look expired. No test tells them apart.
            now = (await connection.execute(select(func.now()))).scalar_one()
            head = (
                await connection.execute(
                    select(_pools.c.version, _pools.c.fairness).where(_pools.c.name == pool)
                )
            ).first()
            rows = (await connection.execute(select(_tickets).where(_tickets.c.pool == pool))).all()
            times = (
                await connection.execute(
                    select(_grants.c.at)
                    .where(
                        _grants.c.pool == pool, _grants.c.at > now - self._seconds(self.retention)
                    )
                    .order_by(_grants.c.at)
                )
            ).scalars()
            moments = tuple(at.astimezone(UTC) for at in times)
        now = now.astimezone(UTC)
        waiting, leased = [], []
        for row in rows:
            (leased if row.lease_end is not None and row.lease_end > now else waiting).append(row)
        return PoolView(
            now=now,
            retention=self.retention,
            waiting=tuple(self._waiting(row) for row in waiting),
            leased=tuple(
                Permit(
                    pool,
                    msgspec.structs.replace(self._waiting(row), grants=row.grants - 1),
                    row.granted_at.astimezone(UTC),
                    row.token,
                )
                for row in leased
            ),
            grant_times=moments,
            fairness=msgspec.convert(head.fairness, Fairness) if head else Fairness(),
            version=head.version if head else 0,
        )

    async def grant(
        self, pool: str, ticket_id: str, *, version: Version, lease: timedelta, fairness: Fairness
    ) -> Permit | Conflict | Missing:
        async with self._connection(_COMMITTED) as connection:
            bump = upsert(_pools).values(
                name=pool, version=1, fairness=msgspec.to_builtins(fairness)
            )
            bumped = (
                await connection.execute(
                    bump.on_conflict_do_update(
                        index_elements=[_pools.c.name],
                        set_={"version": _pools.c.version + 1, "fairness": bump.excluded.fairness},
                        where=_pools.c.version == version,
                    ).returning(_pools.c.version, func.clock_timestamp())
                )
            ).first()
            if bumped is None or bumped[0] != version + 1:  # an insert at a view of another version
                await connection.rollback()
                return Conflict()
            token, now = bumped
            waiting = (
                select(_tickets.c.id)
                .where(
                    _tickets.c.pool == pool,
                    _tickets.c.id == ticket_id,
                    or_(_tickets.c.lease_end.is_(None), _tickets.c.lease_end <= now),
                )
                # a ticket a caller's open transaction holds is skipped: Missing, not a wait
                .with_for_update(skip_locked=True)
                .scalar_subquery()
            )
            row = (
                await connection.execute(
                    update(_tickets)
                    .where(_tickets.c.pool == pool, _tickets.c.id == waiting)
                    .values(
                        grants=_tickets.c.grants + 1,
                        granted_at=now,
                        token=token,
                        lease_end=literal(now) + self._seconds(lease),
                    )
                    .returning(
                        _tickets.c.id,
                        _tickets.c.tenant,
                        _tickets.c.traits,
                        _tickets.c.submitted_at,
                        (_tickets.c.grants - 1).label("grants"),
                    )
                )
            ).first()
            if row is None:
                await connection.rollback()
                return Missing()
            await connection.execute(insert(_grants).values(pool=pool, at=now))
            await connection.execute(
                delete(_grants).where(
                    _grants.c.pool == pool,
                    _grants.c.at <= literal(now) - self._seconds(self.retention),
                )
            )
            await connection.commit()
        return Permit(pool, self._waiting(row), now.astimezone(UTC), token)

    async def renew(self, permit: Permit, *, lease: timedelta) -> Renewed | LeaseLost:
        async with self._connection(_COMMITTED) as connection:
            renewed = await connection.execute(
                update(_tickets)
                .where(
                    _tickets.c.pool == permit.pool,
                    _tickets.c.id == permit.waiting.ticket.id,
                    _tickets.c.token == permit.token,
                    _tickets.c.lease_end > func.clock_timestamp(),
                )
                .values(lease_end=func.clock_timestamp() + self._seconds(lease))
            )
            await connection.commit()
        return Renewed() if renewed.rowcount == 1 else LeaseLost()

    async def release(self, permit: Permit) -> Released | LeaseLost:
        async with self._connection(_COMMITTED) as connection:
            released = await connection.execute(
                delete(_tickets).where(
                    _tickets.c.pool == permit.pool,
                    _tickets.c.id == permit.waiting.ticket.id,
                    _tickets.c.token == permit.token,
                )
            )
            await connection.commit()
        return Released() if released.rowcount == 1 else LeaseLost()

    @contextlib.asynccontextmanager
    async def _connection(self, options: dict[str, Any]) -> AsyncGenerator[AsyncConnection, None]:
        """A connection at the isolation this call needs, whatever the engine's own."""
        async with _Reach().executing(), contextlib.AsyncExitStack() as stack:
            connection = await _Reach().connect(stack.enter_async_context(self.engine.connect()))
            yield await connection.execution_options(**options)

    def _seconds(self, time: timedelta) -> ColumnElement[Any]:
        """A duration in seconds: `interval '1 day'` across a daylight-saving change lasts 23 h."""
        return func.make_interval(0, 0, 0, 0, 0, 0, time.total_seconds())

    def _waiting(self, row: Row[*tuple[Any, ...]]) -> Waiting:
        """A ticket read back through Ticket's own checks, at UTC whatever the session's zone."""
        ticket = msgspec.convert({"id": row.id, "tenant": row.tenant, "traits": row.traits}, Ticket)
        return Waiting(ticket, row.submitted_at.astimezone(UTC), row.grants)
