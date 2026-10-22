"""What only the PostgreSQL store can show: other transactions, isolation, time zones, cancellation,
a server that goes away, and the schema. Every test runs on asyncpg and on psycopg, on the test
database whose clock the tests move by hand (see conftest.py)."""

import asyncio
import contextlib
import random
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import URL, create_engine, event, inspect, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.util import await_only

from wicket.fairness import Fairness
from wicket.limits import Concurrency, Rate
from wicket.postgres import PostgresBacklog, PostgresLedger, metadata
from wicket.results import Conflict, Missing, Renewed, StoreUnavailable
from wicket.tickets import Permit, Ticket, Waiting

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
LEASE = timedelta(seconds=30)
HOUR = timedelta(hours=1)
BERLIN_SPRING = datetime(2027, 3, 28, 0, 50, tzinfo=UTC)  # Berlin moves to summer time at 01:00 UTC


@pytest.fixture(params=["asyncpg", "psycopg"])
def url(request: pytest.FixtureRequest, database: URL) -> URL:
    return database.set(drivername=f"postgresql+{request.param}")


def settings(url: URL, **values: str) -> dict[str, Any]:
    """Session settings as each driver takes them in `connect_args`."""
    if url.drivername.endswith("psycopg"):
        return {"options": " ".join(f"-c {name}={value}" for name, value in values.items())}
    return {"server_settings": values}


def given_back(engine: AsyncEngine) -> bool:
    pool = engine.pool
    return isinstance(pool, QueuePool) and pool.checkedout() == 0


async def reset(engine: AsyncEngine, at: datetime = EPOCH) -> None:
    async with engine.connect() as connection:
        connection = await connection.execution_options(isolation_level="READ COMMITTED")
        for table in ("wicket_pools", "wicket_tickets", "wicket_grants"):
            await connection.execute(text(f"delete from {table}"))
        await connection.execute(text("update wicket_test_clock set at = :at"), {"at": at})
        await connection.commit()


async def elapse(engine: AsyncEngine, time: timedelta) -> None:
    async with engine.connect() as connection:
        connection = await connection.execution_options(isolation_level="READ COMMITTED")
        await connection.execute(
            text("update wicket_test_clock set at = at + make_interval(secs => :s)"),
            {"s": time.total_seconds()},
        )
        await connection.commit()


@contextlib.asynccontextmanager
async def engine_at(url: URL, at: datetime = EPOCH, **options: Any) -> AsyncGenerator[AsyncEngine]:
    """An engine on the emptied test database, its clock at `at`; every connection given back."""
    engine = create_async_engine(url, pool_size=25, **options)
    try:
        await reset(engine, at)
        yield engine
        assert given_back(engine)
    finally:
        await engine.dispose()


@pytest.fixture
async def engine(url: URL) -> AsyncGenerator[AsyncEngine]:
    async with engine_at(url) as engine:
        yield engine


async def submit(engine: AsyncEngine, *ids: str, pool: str = "p") -> None:
    async with AsyncSession(engine) as session:
        for id in ids:
            await PostgresBacklog(session).submit(pool, Ticket(id, f"t{id}"))
        await session.commit()


async def grant_first(
    ledger: PostgresLedger, pool: str = "p", lease: timedelta = LEASE
) -> Permit | Conflict | Missing:
    view = await ledger.view(pool)
    first = view.waiting[0]
    return await ledger.grant(
        pool,
        first.ticket.id,
        version=view.version,
        lease=lease,
        fairness=view.fairness.after(first),
    )


async def terminate_every_session(url: URL) -> None:
    """What a server restart does to the pooled connections."""
    others = "from pg_stat_activity where datname = current_database() and pid <> pg_backend_pid()"
    admin = create_async_engine(url, isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as connection:
            await connection.execute(text(f"select pg_terminate_backend(pid) {others}"))
            for _ in range(50):  # until the backends are surely gone
                left = (await connection.execute(text(f"select count(*) {others}"))).scalar_one()
                if left == 0:
                    return
                await asyncio.sleep(0.02)
            raise AssertionError(f"{left} backends are still there after being terminated")
    finally:
        await admin.dispose()


async def test_a_ticket_held_by_an_open_caller_transaction_is_missing_at_once(engine: AsyncEngine):
    await submit(engine, "1", "2")
    ledger = PostgresLedger(engine, HOUR)
    view = await ledger.view("p")
    one, two = sorted(view.waiting, key=lambda w: w.ticket.id)
    async with AsyncSession(engine) as caller:
        await PostgresBacklog(caller).withdraw("p", "1")  # left open
        held = await asyncio.wait_for(
            ledger.grant(
                "p", "1", version=view.version, lease=LEASE, fairness=view.fairness.after(one)
            ),
            1,
        )
        next_one = await asyncio.wait_for(
            ledger.grant(
                "p", "2", version=view.version, lease=LEASE, fairness=view.fairness.after(two)
            ),
            1,
        )
        assert held == Missing() and isinstance(next_one, Permit)
        await caller.rollback()


@pytest.mark.parametrize(
    "before", ["FROM wicket_pools", "FROM wicket_tickets", "FROM wicket_grants"]
)
async def test_a_view_does_not_see_a_grant_committed_during_it(
    engine: AsyncEngine, url: URL, before: str
):
    await submit(engine, "1", "2")
    ledger = PostgresLedger(engine, HOUR)
    fired: list[bool] = []

    def grant_in_between(connection: Any, cursor: Any, statement: str, *args: Any) -> None:
        if before in statement and statement.lstrip().startswith("SELECT") and not fired:
            fired.append(True)
            chosen = first.waiting[0]
            granted = await_only(
                rival.grant(
                    "p",
                    chosen.ticket.id,
                    version=first.version,
                    lease=LEASE,
                    fairness=first.fairness.after(chosen),
                )
            )
            assert isinstance(granted, Permit)

    try:
        rival_engine = create_async_engine(url)
        rival = PostgresLedger(rival_engine, HOUR)
        first = await ledger.view("p")
        event.listen(engine.sync_engine, "before_cursor_execute", grant_in_between)
        view = await ledger.view("p")
    finally:
        if event.contains(engine.sync_engine, "before_cursor_execute", grant_in_between):
            event.remove(engine.sync_engine, "before_cursor_execute", grant_in_between)
        await rival_engine.dispose()
    assert fired and (view.version, view.leased, view.grant_times) == (first.version, (), ())
    assert Concurrency(1).admits(view)  # the stale view admits, and its grant must lose
    other = view.waiting[1]
    stale = await ledger.grant(
        "p", other.ticket.id, version=view.version, lease=LEASE, fairness=view.fairness.after(other)
    )
    assert stale == Conflict()
    assert len((await ledger.view("p")).leased) == 1


@pytest.mark.parametrize("level", ["SERIALIZABLE", "REPEATABLE READ"])
@pytest.mark.parametrize("pool_known", [False, True])
async def test_a_grant_that_waited_on_the_pool_is_a_conflict_whatever_the_engine(
    url: URL, level: str, pool_known: bool
):
    async with engine_at(url, isolation_level=level) as engine:
        try:
            rival_engine = create_async_engine(url)
            await submit(engine, "0", "1", "2")
            ours, rival = PostgresLedger(engine, HOUR), PostgresLedger(rival_engine, HOUR)
            if pool_known:
                assert isinstance(await grant_first(ours), Permit)
            view = await ours.view("p")
            waiting: list[asyncio.Future[Permit | Conflict | Missing]] = []

            def ours_waits_on_the_pool(
                connection: Any, cursor: Any, statement: str, *a: Any
            ) -> None:
                if statement.lstrip().startswith("UPDATE wicket_tickets") and not waiting:
                    waiting.append(
                        asyncio.ensure_future(
                            ours.grant(
                                "p",
                                view.waiting[1].ticket.id,
                                version=view.version,
                                lease=LEASE,
                                fairness=view.fairness,
                            )
                        )
                    )
                    await_only(asyncio.sleep(0.2))  # ours now waits on the pool's row

            event.listen(rival_engine.sync_engine, "before_cursor_execute", ours_waits_on_the_pool)
            won = await rival.grant(
                "p",
                view.waiting[0].ticket.id,
                version=view.version,
                lease=LEASE,
                fairness=view.fairness,
            )
            assert isinstance(won, Permit)
            assert await waiting[0] == Conflict()
        finally:
            await rival_engine.dispose()


@pytest.mark.parametrize("level", ["SERIALIZABLE", "REPEATABLE READ", "AUTOCOMMIT"])
async def test_grants_answer_one_permit_and_conflicts_whatever_the_engine(url: URL, level: str):
    async with engine_at(url, isolation_level=level) as engine:
        await submit(engine, *map(str, range(21)))
        ledger = PostgresLedger(engine, HOUR)
        view = await ledger.view("p")
        answers = await asyncio.gather(
            *(
                ledger.grant(
                    "p", w.ticket.id, version=view.version, lease=LEASE, fairness=view.fairness
                )
                for w in view.waiting[:20]
            )
        )
        assert sorted(type(a).__name__ for a in answers) == ["Conflict"] * 19 + ["Permit"]
        view = await ledger.view("p")
        missing = await ledger.grant(
            "p", "nobody", version=view.version, lease=LEASE, fairness=view.fairness
        )
        assert missing == Missing()
        assert (await ledger.view("p")).version == view.version


async def test_rate_and_a_lease_keep_their_seconds_across_a_daylight_saving_change(url: URL):
    berlin = settings(url, timezone="Europe/Berlin")
    async with engine_at(url, BERLIN_SPRING, connect_args=berlin) as engine:
        await submit(engine, "1", "2")
        ledger = PostgresLedger(engine, timedelta(days=2))
        assert isinstance(await grant_first(ledger, lease=timedelta(days=1)), Permit)
        await elapse(engine, timedelta(minutes=30))
        view = await ledger.view("p")
        moments = [view.now, *view.grant_times]
        moments += [w.submitted_at for w in view.waiting]
        moments += [p.granted_at for p in view.leased] + [
            p.waiting.submitted_at for p in view.leased
        ]
        assert all(moment.tzinfo is UTC for moment in moments)  # whatever the session's zone
        assert not Rate(1, per=HOUR).admits(view)
        await elapse(engine, timedelta(hours=23))  # 23 h 30 min after the grant
        assert len((await ledger.view("p")).leased) == 1
        await elapse(engine, timedelta(minutes=30))
        assert len((await ledger.view("p")).leased) == 0


async def test_a_renewal_keeps_its_seconds_across_a_daylight_saving_change(url: URL):
    berlin = settings(url, timezone="Europe/Berlin")
    async with engine_at(url, BERLIN_SPRING - timedelta(hours=12), connect_args=berlin) as engine:
        await submit(engine, "1")
        ledger = PostgresLedger(engine, timedelta(days=2))
        permit = await grant_first(ledger, lease=timedelta(minutes=1))
        assert isinstance(permit, Permit)
        assert await ledger.renew(permit, lease=timedelta(days=1)) == Renewed()
        await elapse(engine, timedelta(hours=23, minutes=30))
        assert len((await ledger.view("p")).leased) == 1


async def test_grant_times_come_back_in_order_after_old_ones_are_deleted(url: URL):
    no_index = settings(url, enable_indexscan="off", enable_bitmapscan="off")
    async with engine_at(url, connect_args=no_index) as engine:
        await submit(engine, *map(str, range(25)))
        ledger = PostgresLedger(engine, timedelta(minutes=10))
        for _ in range(25):
            assert isinstance(await grant_first(ledger, lease=timedelta(days=1)), Permit)
            await elapse(engine, timedelta(minutes=1))
            async with engine.connect() as connection:  # new rows reuse the deleted ones' space
                connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
                await connection.execute(text("vacuum wicket_grants"))
        times = (await ledger.view("p")).grant_times
        assert len(times) == 9 and list(times) == sorted(times)


async def test_calls_cancelled_at_random_moments_leave_the_engine_usable(engine: AsyncEngine):
    await submit(engine, *map(str, range(60)))
    ledger = PostgresLedger(engine, HOUR)
    rng = random.Random(7)
    for _ in range(100):
        call = grant_first(ledger) if rng.random() < 0.5 else ledger.view("p")
        task = asyncio.ensure_future(call)
        await asyncio.sleep(rng.uniform(0, 0.006))
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await asyncio.wait_for(ledger.view("p"), 2)
        assert given_back(engine)


async def test_a_connection_the_server_ended_is_unavailable_then_the_next_call_works(
    engine: AsyncEngine, url: URL
):
    ledger = PostgresLedger(engine, HOUR)
    await ledger.view("p")
    await terminate_every_session(url)
    with pytest.raises(StoreUnavailable):
        await ledger.view("p")
    await ledger.view("p")


async def test_after_a_server_restart_one_call_is_unavailable_then_calls_work(
    engine: AsyncEngine, url: URL
):
    ledger = PostgresLedger(engine, HOUR)
    await asyncio.gather(*(ledger.view("p") for _ in range(5)))  # five pooled connections
    await terminate_every_session(url)  # stands in for a restart: a CI service cannot restart
    with pytest.raises(StoreUnavailable):
        await ledger.view("p")
    for _ in range(5):
        await ledger.view("p")


@pytest.mark.parametrize("unreachable", ["closed port", "unknown role"])
async def test_an_unreachable_server_is_unavailable_to_every_call(url: URL, unreachable: str):
    wrong = url.set(port=1) if unreachable == "closed port" else url.set(username="nobody")
    engine = create_async_engine(wrong)
    ledger = PostgresLedger(engine, HOUR)
    permit = Permit("p", Waiting(Ticket("1", "a"), EPOCH), EPOCH, 1)
    calls: list[Callable[[], Any]] = [
        lambda: ledger.view("p"),
        lambda: ledger.grant("p", "1", version=0, lease=LEASE, fairness=Fairness()),
        lambda: ledger.renew(permit, lease=LEASE),
        lambda: ledger.release(permit),
    ]
    try:
        for call in calls:
            with pytest.raises(StoreUnavailable):
                await call()
        async with AsyncSession(engine) as session:
            with pytest.raises(StoreUnavailable):
                await PostgresBacklog(session).submit("p", Ticket("1", "a"))
        async with AsyncSession(engine) as session:
            with pytest.raises(StoreUnavailable):
                await PostgresBacklog(session).withdraw("p", "1")
    finally:
        await engine.dispose()


async def test_a_missing_table_is_an_error_not_an_unavailable_store(url: URL):
    nowhere = settings(url, search_path="nowhere")
    engine = create_async_engine(url, connect_args=nowhere)
    try:
        with pytest.raises(ProgrammingError):
            await PostgresLedger(engine, HOUR).view("p")
    finally:
        await engine.dispose()


@pytest.mark.parametrize("retention", [timedelta(0), timedelta(seconds=-1)])
async def test_the_ledger_refuses_a_retention_that_is_not_positive(retention: timedelta):
    engine = create_async_engine("postgresql+asyncpg://nobody@127.0.0.1:1/none")
    try:
        with pytest.raises(ValueError, match="retention"):
            PostgresLedger(engine, retention)
    finally:
        await engine.dispose()


async def test_a_submit_the_caller_rolls_back_leaves_no_ticket(engine: AsyncEngine):
    async with AsyncSession(engine) as session:
        await PostgresBacklog(session).submit("p", Ticket("1", "ann"))
        await session.rollback()
    assert (await PostgresLedger(engine, HOUR).view("p")).waiting == ()


async def test_a_grant_on_a_pool_with_no_row_at_another_version_is_a_conflict(engine: AsyncEngine):
    await submit(engine, "1")
    ledger = PostgresLedger(engine, HOUR)
    view = await ledger.view("p")
    stale = await ledger.grant(
        "p", "1", version=5, lease=LEASE, fairness=view.fairness.after(view.waiting[0])
    )
    assert stale == Conflict()


def test_metadata_holds_the_three_tables_and_they_create_cleanly(database: URL):
    names = {"wicket_pools", "wicket_tickets", "wicket_grants"}
    assert set(metadata.tables) == names
    engine = create_engine(database)
    try:
        with engine.begin() as connection:
            connection.execute(text("create schema fresh; set local search_path = fresh"))
            metadata.create_all(connection)
            assert set(inspect(connection).get_table_names(schema="fresh")) == names
            connection.execute(text("drop schema fresh cascade"))
    finally:
        engine.dispose()


async def test_a_connection_lost_as_a_view_closes_is_unavailable_then_the_next_call_works(
    engine: AsyncEngine, url: URL
):
    ledger = PostgresLedger(engine, HOUR)
    lost: list[bool] = []

    def lose_the_connection(connection: Any, cursor: Any, statement: str, *args: Any) -> None:
        if "FROM wicket_grants" in statement and not lost:  # the view's last statement
            lost.append(True)
            await_only(terminate_every_session(url))

    event.listen(engine.sync_engine, "after_cursor_execute", lose_the_connection)
    try:
        with pytest.raises(StoreUnavailable):
            await ledger.view("p")
    finally:
        event.remove(engine.sync_engine, "after_cursor_execute", lose_the_connection)
    assert lost
    await ledger.view("p")
