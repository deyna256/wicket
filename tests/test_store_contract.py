"""StoreContract: the obligations every pair of store adapters keeps (the spec's "Store
obligations"). Each test runs on every pair in STORES: the in-memory one, and PostgreSQL through
asyncpg and through psycopg, whose clock the tests move by hand too (see conftest.py). What only
PostgreSQL can show is in test_postgres.py."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Protocol

import pytest
from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import QueuePool

from wicket.fairness import Fairness
from wicket.limits import Concurrency
from wicket.memory import MemoryBacklog, MemoryLedger, MemoryState
from wicket.pool import Pool
from wicket.postgres import PostgresBacklog, PostgresLedger
from wicket.priority import FairShare, Priority
from wicket.results import (
    AlreadyLeased,
    Conflict,
    Gone,
    LeaseLost,
    Missing,
    Released,
    Renewed,
    Withdrawn,
)
from wicket.store import Backlog, Ledger
from wicket.tickets import Permit, Ticket, Waiting

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
LEASE = timedelta(seconds=30)
RETENTION = timedelta(hours=1)


class Clock:
    """The store's time, which the test moves."""

    def __init__(self) -> None:
        self.now = EPOCH

    def __call__(self) -> datetime:
        return self.now


class MemoryPair:
    """The in-memory adapters over one state, with a clock the test moves."""

    def __init__(self) -> None:
        self.clock = Clock()
        state = MemoryState(retention=RETENTION, clock=self.clock)
        self.backlog = MemoryBacklog(state)
        self.ledger = MemoryLedger(state)

    async def elapse(self, time: timedelta) -> None:
        self.clock.now += time


class Committing:
    """A Backlog committing each call in a session of its own, as a caller's unit of work would."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def submit(self, pool: str, ticket: Ticket) -> None:
        async with AsyncSession(self.engine) as session:
            await PostgresBacklog(session).submit(pool, ticket)
            await session.commit()

    async def withdraw(self, pool: str, ticket_id: str) -> Withdrawn | AlreadyLeased | Gone:
        async with AsyncSession(self.engine) as session:
            answer = await PostgresBacklog(session).withdraw(pool, ticket_id)
            await session.commit()
            return answer


class PostgresPair:
    """The PostgreSQL adapters on the test database, emptied and with its clock at EPOCH."""

    def __init__(self, database: URL) -> None:
        self.engine = create_async_engine(database)
        self.backlog = Committing(self.engine)
        self.ledger = PostgresLedger(self.engine, RETENTION)

    async def start(self) -> None:
        async with self.engine.begin() as connection:
            for table in ("wicket_pools", "wicket_tickets", "wicket_grants"):
                await connection.execute(text(f"delete from {table}"))
            await connection.execute(text("update wicket_test_clock set at = :at"), {"at": EPOCH})

    async def stop(self) -> None:
        pool = self.engine.pool
        try:
            assert isinstance(pool, QueuePool) and pool.checkedout() == 0  # all given back
        finally:
            await self.engine.dispose()

    async def elapse(self, time: timedelta) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(
                text("update wicket_test_clock set at = at + make_interval(secs => :s)"),
                {"s": time.total_seconds()},
            )


class Pair(Protocol):
    """What a test needs of a store pair: its two adapters, and a way to move its clock."""

    @property
    def backlog(self) -> Backlog: ...

    @property
    def ledger(self) -> Ledger: ...

    async def elapse(self, time: timedelta) -> None: ...


STORES = ["memory", "postgres-asyncpg", "postgres-psycopg"]


@pytest.fixture(params=STORES)
async def store(request: pytest.FixtureRequest) -> AsyncGenerator[Pair, None]:
    if request.param == "memory":
        yield MemoryPair()
        return
    database: URL = request.getfixturevalue("database")
    driver = request.param.removeprefix("postgres-")
    pair = PostgresPair(database.set(drivername=f"postgresql+{driver}"))
    await pair.start()
    try:
        yield pair
    finally:
        await pair.stop()


async def granted(store: Pair, ticket_id: str, pool: str = "p", lease: timedelta = LEASE) -> Permit:
    """Grant one ticket at the current version, as a pool would."""
    view = await store.ledger.view(pool)
    chosen = next(w for w in view.waiting if w.ticket.id == ticket_id)
    permit = await store.ledger.grant(
        pool, ticket_id, version=view.version, lease=lease, fairness=view.fairness.after(chosen)
    )
    assert isinstance(permit, Permit)
    return permit


async def test_a_pool_nobody_used_is_empty_at_version_zero(store: Pair):
    view = await store.ledger.view("never")
    assert (view.waiting, view.leased, view.grant_times, view.version, view.fairness) == (
        (),
        (),
        (),
        0,
        Fairness(),
    )


async def test_a_submitted_ticket_waits_with_the_store_time(store: Pair):
    await store.backlog.submit("p", Ticket("1", "ann", {"tier": 0.5}))
    view = await store.ledger.view("p")
    assert view.waiting == (Waiting(Ticket("1", "ann", {"tier": 0.5}), view.now, 0),)


async def test_submitting_a_known_id_keeps_the_first_tenant_and_traits(store: Pair):
    await store.backlog.submit("p", Ticket("1", "ann", {"tier": 0.5}))
    await store.backlog.submit("p", Ticket("1", "bob", {"tier": 1.0}))
    assert [w.ticket for w in (await store.ledger.view("p")).waiting] == [
        Ticket("1", "ann", {"tier": 0.5})
    ]


async def test_the_store_keeps_its_own_copy_of_the_traits(store: Pair):
    traits = {"tier": 0.5}
    await store.backlog.submit("p", Ticket("1", "ann", traits))
    traits["tier"] = 9.0
    assert (await store.ledger.view("p")).waiting[0].ticket.traits == {"tier": 0.5}


async def test_a_grant_leases_the_ticket_and_writes_its_time_fairness_and_version(
    store: Pair,
):
    await store.backlog.submit("p", Ticket("1", "ann"))
    permit = await granted(store, "1")
    view = await store.ledger.view("p")
    assert (permit.pool, permit.waiting.ticket.id, permit.attempt, permit.token) == ("p", "1", 1, 1)
    assert view.waiting == () and view.leased == (permit,)
    assert view.grant_times == (permit.granted_at,) and view.version == 1
    assert view.fairness == Fairness().after(permit.waiting)


async def test_a_grant_at_a_stale_version_is_a_conflict(store: Pair):
    for id in "12":
        await store.backlog.submit("p", Ticket(id, "ann"))
    stale = await store.ledger.view("p")
    await granted(store, "1")
    answer = await store.ledger.grant(
        "p", "2", version=stale.version, lease=LEASE, fairness=stale.fairness
    )
    assert answer == Conflict()


async def test_concurrent_grants_at_one_version_lease_one_ticket(store: Pair):
    for id in range(20):
        await store.backlog.submit("p", Ticket(str(id), f"t{id}"))
    view = await store.ledger.view("p")
    answers = await asyncio.gather(
        *(
            store.ledger.grant(
                "p", w.ticket.id, version=view.version, lease=LEASE, fairness=view.fairness
            )
            for w in view.waiting
        )
    )
    assert sum(isinstance(a, Permit) for a in answers) == 1
    assert len((await store.ledger.view("p")).leased) == 1


async def test_a_grant_of_a_leased_ticket_at_the_current_version_is_missing(store: Pair):
    await store.backlog.submit("p", Ticket("1", "ann"))
    await granted(store, "1")
    view = await store.ledger.view("p")
    answer = await store.ledger.grant(
        "p", "1", version=view.version, lease=LEASE, fairness=view.fairness
    )
    assert answer == Missing()


async def test_a_grant_of_a_withdrawn_or_unknown_ticket_is_missing(store: Pair):
    await store.backlog.submit("p", Ticket("1", "ann"))
    view = await store.ledger.view("p")
    await store.backlog.withdraw("p", "1")
    for id in ("1", "nobody"):
        assert (
            await store.ledger.grant(
                "p", id, version=view.version, lease=LEASE, fairness=view.fairness
            )
            == Missing()
        )
    assert (await store.ledger.view("p")).version == view.version


async def test_only_a_grant_changes_the_version(store: Pair):
    await store.backlog.submit("p", Ticket("1", "ann"))
    await store.backlog.submit("p", Ticket("2", "ann"))
    permit = await granted(store, "1")
    assert await store.ledger.renew(permit, lease=LEASE) == Renewed()
    await store.backlog.withdraw("p", "2")
    assert await store.ledger.release(permit) == Released()
    assert (await store.ledger.view("p")).version == 1


async def test_a_renewal_extends_the_lease(store: Pair):
    await store.backlog.submit("p", Ticket("1", "ann"))
    permit = await granted(store, "1")
    await store.elapse(LEASE - timedelta(seconds=1))
    assert await store.ledger.renew(permit, lease=LEASE) == Renewed()
    await store.elapse(timedelta(seconds=2))
    assert (await store.ledger.view("p")).leased == (permit,)


async def test_an_expired_lease_puts_the_ticket_back_with_its_grants_and_submit_time(
    store: Pair,
):
    await store.backlog.submit("p", Ticket("1", "ann"))
    submitted = (await store.ledger.view("p")).waiting[0].submitted_at
    permit = await granted(store, "1")
    await store.elapse(LEASE)
    view = await store.ledger.view("p")
    assert view.leased == () and view.waiting == (Waiting(Ticket("1", "ann"), submitted, 1),)
    assert await store.ledger.renew(permit, lease=LEASE) == LeaseLost()


async def test_a_release_after_the_lease_ends_succeeds_until_the_ticket_is_leased_again(
    store: Pair,
):
    for id in "12":
        await store.backlog.submit("p", Ticket(id, "ann"))
    late = await granted(store, "1")
    await store.elapse(LEASE)
    assert await store.ledger.release(late) == Released()
    assert [w.ticket.id for w in (await store.ledger.view("p")).waiting] == ["2"]
    stale = await granted(store, "2")
    await store.elapse(LEASE)
    again = await granted(store, "2")
    assert again.token > stale.token and again.attempt == 2
    assert await store.ledger.release(stale) == LeaseLost()
    assert await store.ledger.renew(stale, lease=LEASE) == LeaseLost()
    assert (await store.ledger.view("p")).leased == (again,)


async def test_withdraw_answers_by_the_state_of_the_ticket(store: Pair):
    for id in "123":
        await store.backlog.submit("p", Ticket(id, "ann"))
    assert await store.backlog.withdraw("p", "1") == Withdrawn()
    hung = await granted(store, "2")
    assert await store.backlog.withdraw("p", "2") == AlreadyLeased()
    await store.elapse(LEASE)
    assert await store.backlog.withdraw("p", "2") == Withdrawn()  # its lease ended: it was waiting
    assert await store.ledger.release(hung) == LeaseLost()
    assert await store.backlog.withdraw("p", "nobody") == Gone()


async def test_a_ticket_submitted_again_after_it_is_gone_starts_anew(store: Pair):
    await store.backlog.submit("p", Ticket("1", "ann"))
    assert await store.ledger.release(await granted(store, "1")) == Released()
    await store.backlog.submit("p", Ticket("1", "ann"))
    assert (await store.ledger.view("p")).waiting[0].grants == 0


async def test_two_pools_keep_equal_ids_and_tokens_apart(store: Pair):
    for pool in ("a", "b"):
        await store.backlog.submit(pool, Ticket("1", "ann"))
    in_a, in_b = await granted(store, "1", "a"), await granted(store, "1", "b")
    assert in_a.token == in_b.token == 1
    assert await store.ledger.release(in_a) == Released()
    assert (await store.ledger.view("b")).leased == (in_b,)


async def test_the_view_reports_the_retention_and_the_grants_within_it(store: Pair):
    await store.backlog.submit("p", Ticket("1", "ann"))
    first = await granted(store, "1", lease=timedelta(seconds=1))
    await store.elapse(timedelta(minutes=30))
    await store.backlog.submit("p", Ticket("2", "ann"))
    second = await granted(store, "2")
    view = await store.ledger.view("p")
    assert view.retention == RETENTION
    assert view.grant_times == (first.granted_at, second.granted_at)
    await store.elapse(timedelta(minutes=31))
    assert (await store.ledger.view("p")).grant_times == (second.granted_at,)


async def test_pools_keep_equal_permits_apart(store: Pair):
    for pool in ("a", "b"):
        await store.backlog.submit(pool, Ticket("1", "ann"))
    in_a, in_b = await granted(store, "1", "a"), await granted(store, "1", "b")
    assert await store.ledger.release(in_a) == Released()
    assert await store.ledger.renew(in_a, lease=LEASE) == LeaseLost()
    assert await store.ledger.renew(in_b, lease=LEASE) == Renewed()


async def test_pools_of_several_processes_never_exceed_the_limit(store: Pair):
    for n in range(12):
        await store.backlog.submit("p", Ticket(str(n), f"t{n % 3}"))
    pools = [
        Pool(
            store.ledger,
            "p",
            priority=Priority((FairShare(), 1)),
            limits=(Concurrency(2),),
            lease=timedelta(hours=1),
            poll=timedelta(milliseconds=20),
        )
        for _ in range(3)  # three "processes"
    ]
    running = peak = 0

    async def worker(serving: Pool) -> None:
        nonlocal running, peak
        for _ in range(2):
            async with serving.permit():
                running += 1
                peak = max(peak, running)
                await asyncio.sleep(0.03)
                running -= 1

    await asyncio.gather(*(worker(pools[n % 3]) for n in range(6)))
    assert peak == 2
    assert (await store.ledger.view("p")).waiting == ()
