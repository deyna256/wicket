"""What only the PostgreSQL store can show: other transactions, isolation, time zones, cancellation,
a server that goes away or a schema it lacks, and pools of several processes on one database. Every
test runs on asyncpg and on psycopg, on the test database whose clock the tests move by hand (see
conftest.py)."""

import asyncio
import contextlib
import random
import sys
from asyncio.subprocess import PIPE
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import msgspec
import pytest
from postgres_db import elapse, emptied, given_back, opened
from sqlalchemy import URL, event, make_url, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.util import await_

from wicket.fairness import Fairness
from wicket.limits import Concurrency, Lane, Rate
from wicket.pool import Pool
from wicket.postgres import PostgresBacklog, PostgresLedger
from wicket.priority import FairShare, Priority
from wicket.results import (
    Attended,
    Conflict,
    LeaseLost,
    Missing,
    Renewed,
    StoreUnavailable,
    Withdrawn,
)
from wicket.tickets import Permit, Ticket, Waiting

pytestmark = pytest.mark.postgres

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
LEASE = timedelta(seconds=30)
HOUR = timedelta(hours=1)
BERLIN_SPRING = datetime(2027, 3, 28, 0, 50, tzinfo=UTC)  # Berlin moves to summer time at 01:00 UTC
OTHERS = "from pg_stat_activity where datname = current_database() and pid <> pg_backend_pid()"


@pytest.fixture(params=["asyncpg", "psycopg"])
def url(request: pytest.FixtureRequest, database: URL) -> URL:
    return database.set(drivername=f"postgresql+{request.param}")


def settings(url: URL, **values: str) -> dict[str, Any]:
    """Session settings as each driver takes them in `connect_args`."""
    if url.drivername.endswith("psycopg"):
        return {"options": " ".join(f"-c {name}={value}" for name, value in values.items())}
    return {"server_settings": values}


@pytest.fixture
async def engine(url: URL) -> AsyncGenerator[AsyncEngine]:
    async with emptied(url, EPOCH) as engine:
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
        first,
        version=view.version,
        lease=lease,
        fairness=view.fairness.after(first, ()),
    )


async def until(url: URL, condition: str) -> None:
    """Asks the server `condition` on a connection of its own until it holds, a second at most."""
    async with opened(url, isolation_level="AUTOCOMMIT") as admin, admin.connect() as connection:
        for _ in range(50):
            if (await connection.execute(text(f"select {condition}"))).scalar_one():
                return
            await asyncio.sleep(0.02)
    raise AssertionError(f"the server never came to {condition}")


async def terminate_every_session(url: URL) -> None:
    """What a server restart does to the pooled connections."""
    async with opened(url, isolation_level="AUTOCOMMIT") as admin, admin.connect() as connection:
        await connection.execute(text(f"select pg_terminate_backend(pid) {OTHERS}"))
    await until(url, f"not exists (select {OTHERS})")


async def test_a_ticket_held_by_an_open_caller_transaction_is_missing_at_once(engine: AsyncEngine):
    await submit(engine, "1", "2")
    ledger = PostgresLedger(engine, HOUR)
    view = await ledger.view("p")
    one, two = sorted(view.waiting, key=lambda w: w.ticket.id)
    async with AsyncSession(engine) as caller:
        await PostgresBacklog(caller).withdraw("p", "1")  # left open
        held = await asyncio.wait_for(
            ledger.grant(
                "p", one, version=view.version, lease=LEASE, fairness=view.fairness.after(one, ())
            ),
            1,
        )
        next_one = await asyncio.wait_for(
            ledger.grant(
                "p", two, version=view.version, lease=LEASE, fairness=view.fairness.after(two, ())
            ),
            1,
        )
        assert held == Missing()
        assert isinstance(next_one, Permit)


async def test_a_replacement_committed_during_a_grant_is_missing(engine: AsyncEngine):
    await submit(engine, "1")
    ledger = PostgresLedger(engine, HOUR)
    stale = await ledger.view("p")
    replaced: list[bool] = []

    async def replace_ticket() -> None:
        async with AsyncSession(engine) as producer:
            backlog = PostgresBacklog(producer)
            assert await backlog.withdraw("p", "1") == Withdrawn()
            await backlog.submit("p", Ticket("1", "replacement"))
            await producer.commit()

    def replace_before_take(connection: Any, cursor: Any, statement: str, *args: Any) -> None:
        if statement.lstrip().startswith("UPDATE wicket_tickets") and not replaced:
            replaced.append(True)
            await_(asyncio.wait_for(replace_ticket(), 1))

    event.listen(engine.sync_engine, "before_cursor_execute", replace_before_take)
    try:
        answer = await ledger.grant(
            "p",
            stale.waiting[0],
            version=stale.version,
            lease=LEASE,
            fairness=stale.fairness.after(stale.waiting[0], ()),
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", replace_before_take)
    view = await ledger.view("p")
    assert replaced and answer == Missing()
    assert (view.version, view.fairness, view.grants, view.leased) == (
        stale.version,
        stale.fairness,
        stale.grants,
        (),
    )
    assert view.waiting[0].ticket == Ticket("1", "replacement")
    assert view.waiting[0].grants == 0


async def test_invalid_stored_traits_are_rejected(engine: AsyncEngine):
    await submit(engine, "1")
    async with engine.begin() as connection:
        await connection.execute(
            text("update wicket_tickets set traits = cast(:traits as jsonb)"),
            {"traits": '[{"name": "tier", "value": 99}]'},
        )
    with pytest.raises(msgspec.ValidationError, match=r"not in 0\.\.1"):
        await PostgresLedger(engine, HOUR).view("p")


async def test_traits_stored_as_a_dictionary_are_rejected(engine: AsyncEngine):
    await submit(engine, "1")
    async with engine.begin() as connection:
        await connection.execute(
            text("update wicket_tickets set traits = cast(:traits as jsonb)"),
            {"traits": '{"tier": 0.5}'},
        )
    with pytest.raises(msgspec.ValidationError):
        await PostgresLedger(engine, HOUR).view("p")


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
            granted = await_(
                rival.grant(
                    "p",
                    chosen,
                    version=first.version,
                    lease=LEASE,
                    fairness=first.fairness.after(chosen, ()),
                )
            )
            assert isinstance(granted, Permit)

    async with opened(url) as rival_engine:
        rival = PostgresLedger(rival_engine, HOUR)
        first = await ledger.view("p")
        event.listen(engine.sync_engine, "before_cursor_execute", grant_in_between)
        try:
            view = await ledger.view("p")
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", grant_in_between)
    assert fired
    assert (view.version, view.leased, view.grants) == (first.version, (), ())


@pytest.mark.parametrize("level", ["SERIALIZABLE", "REPEATABLE READ"])
@pytest.mark.parametrize("pool_known", [False, True])
async def test_a_grant_that_waited_on_the_pool_is_a_conflict_whatever_the_engine(
    url: URL, level: str, pool_known: bool
):
    async with emptied(url, EPOCH, isolation_level=level) as engine, opened(url) as rival_engine:
        await submit(engine, "0", "1", "2")
        ours, rival = PostgresLedger(engine, HOUR), PostgresLedger(rival_engine, HOUR)
        if pool_known:
            assert isinstance(await grant_first(ours), Permit)
        view = await ours.view("p")
        waiting: list[asyncio.Future[Permit | Conflict | Missing]] = []

        def ours_waits_on_the_pool(connection: Any, cursor: Any, statement: str, *a: Any) -> None:
            if statement.lstrip().startswith("UPDATE wicket_tickets") and not waiting:
                waiting.append(
                    asyncio.ensure_future(
                        ours.grant(
                            "p",
                            view.waiting[1],
                            version=view.version,
                            lease=LEASE,
                            fairness=view.fairness,
                        )
                    )
                )
                await_(until(url, f"exists (select {OTHERS} and wait_event_type = 'Lock')"))

        event.listen(rival_engine.sync_engine, "before_cursor_execute", ours_waits_on_the_pool)
        won = await rival.grant(
            "p",
            view.waiting[0],
            version=view.version,
            lease=LEASE,
            fairness=view.fairness,
        )
        assert isinstance(won, Permit)
        assert await waiting[0] == Conflict()


@pytest.mark.parametrize("level", ["SERIALIZABLE", "REPEATABLE READ", "AUTOCOMMIT"])
async def test_grants_answer_one_permit_and_conflicts_whatever_the_engine(url: URL, level: str):
    async with emptied(url, EPOCH, isolation_level=level, pool_size=25) as engine:
        await submit(engine, *map(str, range(21)))
        ledger = PostgresLedger(engine, HOUR)
        view = await ledger.view("p")
        answers = await asyncio.gather(
            *(
                ledger.grant("p", w, version=view.version, lease=LEASE, fairness=view.fairness)
                for w in view.waiting[:20]
            )
        )
        assert sorted(type(a).__name__ for a in answers) == ["Conflict"] * 19 + ["Permit"]
        view = await ledger.view("p")
        missing = await ledger.grant(
            "p",
            msgspec.structs.replace(view.waiting[0], ticket=Ticket("nobody", "a")),
            version=view.version,
            lease=LEASE,
            fairness=view.fairness,
        )
        assert missing == Missing()
        assert (await ledger.view("p")).version == view.version


async def test_every_moment_comes_back_at_utc_whatever_the_session_zone(url: URL):
    berlin = settings(url, timezone="Europe/Berlin")
    async with emptied(url, EPOCH, connect_args=berlin) as engine:
        await submit(engine, "1", "2")
        ledger = PostgresLedger(engine, HOUR)
        granted = await grant_first(ledger)
        view = await ledger.view("p")
    assert isinstance(granted, Permit)
    (leased,), (waiting,) = view.leased, view.waiting
    moments = [
        granted.granted_at,
        granted.waiting.submitted_at,
        view.now,
        *(grant.at for grant in view.grants),
    ]
    moments += [leased.granted_at, leased.waiting.submitted_at, waiting.submitted_at]
    assert {moment.tzinfo for moment in moments} == {UTC}


async def test_rate_and_a_lease_keep_their_seconds_across_a_daylight_saving_change(url: URL):
    berlin = settings(url, timezone="Europe/Berlin")
    async with emptied(url, BERLIN_SPRING, connect_args=berlin) as engine:
        await submit(engine, "1", "2")
        ledger = PostgresLedger(engine, timedelta(days=2))
        assert isinstance(await grant_first(ledger, lease=timedelta(days=1)), Permit)
        await elapse(engine, timedelta(minutes=30))
        assert not Rate(1, per=HOUR).admits(await ledger.view("p"))
        await elapse(engine, timedelta(hours=23))  # 23 h 30 min after the grant
        assert len((await ledger.view("p")).leased) == 1
        await elapse(engine, timedelta(minutes=30))
        assert len((await ledger.view("p")).leased) == 0


async def test_a_renewal_keeps_its_seconds_across_a_daylight_saving_change(url: URL):
    berlin = settings(url, timezone="Europe/Berlin")
    async with emptied(url, BERLIN_SPRING - timedelta(hours=12), connect_args=berlin) as engine:
        await submit(engine, "1")
        ledger = PostgresLedger(engine, timedelta(days=2))
        permit = await grant_first(ledger, lease=timedelta(minutes=1))
        assert isinstance(permit, Permit)
        assert await ledger.renew(permit, lease=timedelta(days=1)) == Renewed()
        await elapse(engine, timedelta(hours=23, minutes=30))
        assert len((await ledger.view("p")).leased) == 1


@pytest.mark.parametrize("cancel", [False, True], ids=["commit", "cancel"])
async def test_a_delayed_renewal_cannot_revive_capacity_used_by_another_grant(
    engine: AsyncEngine, url: URL, cancel: bool
):
    await submit(engine, "1", "2")
    ledger = PostgresLedger(engine, HOUR)
    permit = await grant_first(ledger)
    assert isinstance(permit, Permit)
    await elapse(engine, timedelta(seconds=29))
    updated, resume = asyncio.Event(), asyncio.Event()

    def pause_after_update(connection: Any, cursor: Any, statement: str, *args: Any) -> None:
        if statement.lstrip().startswith("UPDATE wicket_tickets"):
            updated.set()
            await_(resume.wait())

    async with opened(url) as renewal_engine:
        renewer = PostgresLedger(renewal_engine, HOUR)
        event.listen(renewal_engine.sync_engine, "after_cursor_execute", pause_after_update)
        try:
            async with asyncio.TaskGroup() as tasks:
                renewal = tasks.create_task(renewer.renew(permit, lease=LEASE))
                try:
                    await updated.wait()
                    await elapse(engine, timedelta(seconds=2))
                    stale = await ledger.view("p")
                    assert Concurrency(1).admits(stale)
                    chosen = next(
                        w for w in stale.waiting if w.ticket.id != permit.waiting.ticket.id
                    )
                    grant = tasks.create_task(
                        ledger.grant(
                            "p",
                            chosen,
                            version=stale.version,
                            lease=LEASE,
                            fairness=stale.fairness.after(chosen, ()),
                        )
                    )
                    blocked = tasks.create_task(
                        until(url, f"exists (select {OTHERS} and wait_event_type = 'Lock')")
                    )
                    # The old implementation grants immediately; the fixed one waits for renewal.
                    await asyncio.wait((grant, blocked), return_when=asyncio.FIRST_COMPLETED)
                    blocked.cancel()
                    if cancel:
                        renewal.cancel()
                finally:
                    resume.set()
        finally:
            event.remove(renewal_engine.sync_engine, "after_cursor_execute", pause_after_update)
        assert given_back(renewal_engine)

    fresh = await ledger.view("p")
    if cancel:
        assert renewal.cancelled()
        answer = grant.result()
        assert isinstance(answer, Permit)
        assert fresh.leased == (answer,)
        assert fresh.version == stale.version + 1
    else:
        assert renewal.result() == Renewed()
        assert grant.result() == Conflict()
        assert fresh == msgspec.structs.replace(
            stale, version=stale.version + 1, waiting=(chosen,), leased=(permit,)
        )
    assert not Concurrency(1).admits(fresh)


async def test_a_renewal_checks_expiry_after_waiting_for_the_pool(engine: AsyncEngine, url: URL):
    await submit(engine, "1")
    ledger = PostgresLedger(engine, HOUR)
    permit = await grant_first(ledger)
    assert isinstance(permit, Permit)
    await elapse(engine, timedelta(seconds=29))
    async with asyncio.TaskGroup() as tasks, engine.begin() as connection:
        await connection.execute(text("select name from wicket_pools where name = 'p' for update"))
        renewal = tasks.create_task(ledger.renew(permit, lease=LEASE))
        await until(url, f"exists (select {OTHERS} and wait_event_type = 'Lock')")
        await elapse(engine, timedelta(seconds=2))
        expired = await ledger.view("p")
    assert renewal.result() == LeaseLost()
    assert await ledger.view("p") == expired


async def test_a_renewal_checks_expiry_after_waiting_for_the_ticket(engine: AsyncEngine, url: URL):
    await submit(engine, "1")
    ledger = PostgresLedger(engine, HOUR)
    permit = await grant_first(ledger)
    assert isinstance(permit, Permit)
    await elapse(engine, timedelta(seconds=29))
    async with asyncio.TaskGroup() as tasks, engine.begin() as connection:
        await connection.execute(text("select id from wicket_tickets where id = '1' for update"))
        renewal = tasks.create_task(ledger.renew(permit, lease=LEASE))
        await until(
            url,
            f"exists (select {OTHERS} and wait_event_type = 'Lock'"
            " and query like '%wicket_tickets%')",
        )
        await elapse(engine, timedelta(seconds=2))
        expired = await ledger.view("p")
    assert renewal.result() == LeaseLost()
    assert await ledger.view("p") == expired


async def test_a_lost_permit_does_not_wait_for_the_ticket_of_its_new_holder(engine: AsyncEngine):
    await submit(engine, "1")
    ledger = PostgresLedger(engine, HOUR)
    permit = await grant_first(ledger)
    assert isinstance(permit, Permit)
    await elapse(engine, LEASE)
    assert isinstance(await grant_first(ledger), Permit)
    async with engine.begin() as connection:
        await connection.execute(text("select id from wicket_tickets where id = '1' for update"))
        assert await asyncio.wait_for(ledger.renew(permit, lease=LEASE), 5) == LeaseLost()


async def test_grants_come_back_in_order_after_old_ones_are_deleted(url: URL):
    no_index = settings(url, enable_indexscan="off", enable_bitmapscan="off")
    async with emptied(url, EPOCH, connect_args=no_index) as engine:
        await submit(engine, *map(str, range(25)))
        ledger = PostgresLedger(engine, timedelta(minutes=10))
        for _ in range(25):
            assert isinstance(await grant_first(ledger, lease=timedelta(days=1)), Permit)
            await elapse(engine, timedelta(minutes=1))
            async with engine.connect() as connection:  # new rows reuse the deleted ones' space
                connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
                await connection.execute(text("vacuum wicket_grants"))
        times = tuple(grant.at for grant in (await ledger.view("p")).grants)
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
    permit = Permit("p", Waiting(Ticket("1", "a"), EPOCH, UUID(int=1)), EPOCH, 1)
    async with opened(wrong) as engine:
        ledger = PostgresLedger(engine, HOUR)
        calls: list[Callable[[], Any]] = [
            lambda: ledger.view("p"),
            lambda: ledger.grant("p", permit.waiting, version=0, lease=LEASE, fairness=Fairness()),
            lambda: ledger.renew(permit, lease=LEASE),
            lambda: ledger.release(permit),
        ]
        for call in calls:
            with pytest.raises(StoreUnavailable):
                await call()
        async with AsyncSession(engine) as session:
            with pytest.raises(StoreUnavailable):
                await PostgresBacklog(session).submit("p", Ticket("1", "a"))
        async with AsyncSession(engine) as session:
            with pytest.raises(StoreUnavailable):
                await PostgresBacklog(session).withdraw("p", "1")


async def test_a_missing_table_is_an_error_not_an_unavailable_store(url: URL):
    nowhere = settings(url, search_path="nowhere")
    async with opened(url, connect_args=nowhere) as engine:
        with pytest.raises(ProgrammingError):
            await PostgresLedger(engine, HOUR).view("p")


@pytest.mark.parametrize("retention", [timedelta(0), timedelta(seconds=-1)])
async def test_the_ledger_refuses_a_retention_that_is_not_positive(retention: timedelta):
    async with opened(make_url("postgresql+asyncpg://nobody@127.0.0.1:1/none")) as engine:
        with pytest.raises(ValueError, match="retention"):
            PostgresLedger(engine, retention)


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
        "p",
        view.waiting[0],
        version=5,
        lease=LEASE,
        fairness=view.fairness.after(view.waiting[0], ()),
    )
    assert stale == Conflict()


async def test_a_connection_lost_as_a_view_closes_is_unavailable_then_the_next_call_works(
    engine: AsyncEngine, url: URL
):
    ledger = PostgresLedger(engine, HOUR)
    lost: list[bool] = []

    def lose_the_connection(connection: Any, cursor: Any, statement: str, *args: Any) -> None:
        if "FROM wicket_grants" in statement and not lost:  # the view's last statement
            lost.append(True)
            await_(terminate_every_session(url))

    event.listen(engine.sync_engine, "after_cursor_execute", lose_the_connection)
    try:
        with pytest.raises(StoreUnavailable):
            await ledger.view("p")
    finally:
        event.remove(engine.sync_engine, "after_cursor_execute", lose_the_connection)
    assert lost
    await ledger.view("p")


async def test_pools_of_several_processes_never_exceed_the_limit(url: URL):
    async with emptied(url, EPOCH) as engine, opened(url) as other:
        await submit(engine, *map(str, range(12)))
        pools = [
            Pool(
                PostgresLedger(serving, HOUR),
                "p",
                priority=Priority((FairShare(), 1)),
                limits=(Concurrency(2),),
                lease=HOUR,
                poll=timedelta(milliseconds=20),
            )
            for serving in (engine, other, other)  # three "processes", two of them on one engine
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
        left = (await PostgresLedger(engine, HOUR).view("p")).waiting
    assert peak == 2
    assert left == ()


@pytest.mark.parametrize("outcome", ["commit", "rollback"])
async def test_a_ticket_held_by_a_caller_transaction_holds_back_another_lane_only_while_held(
    engine: AsyncEngine, outcome: str
):
    # a1 sorts before b1, so with equal tags and submit times alice's ticket is the winner
    async with AsyncSession(engine) as session:
        await PostgresBacklog(session).submit("p", Ticket("a1", "alice", lane="m"))
        await PostgresBacklog(session).submit("p", Ticket("b1", "bob", lane="g"))
        await session.commit()
    ledger, order = PostgresLedger(engine, HOUR), []

    async def work(lane: str) -> None:
        serving = Pool(
            ledger,
            "p",
            lane=Lane(lane),
            priority=Priority((FairShare(), 1)),
            lease=HOUR,
            poll=timedelta(milliseconds=20),
        )
        async with serving.permit() as permit:
            order.append(permit.waiting.ticket.id)

    async with AsyncSession(engine) as caller:
        await PostgresBacklog(caller).withdraw("p", "a1")  # holds a1's row, left open
        mentor = asyncio.create_task(work("m"))
        while not (await ledger.view("p")).present:  # mentor is present before generation looks
            await asyncio.sleep(0.01)
        generation = asyncio.create_task(work("g"))
        await asyncio.sleep(0.3)
        assert order == []  # mentor gets Missing, generation yields to a1
        await (caller.commit() if outcome == "commit" else caller.rollback())
    if outcome == "commit":
        await asyncio.wait_for(generation, 2)
        mentor.cancel()
        await asyncio.gather(mentor, return_exceptions=True)
        assert order == ["b1"]
    else:
        await asyncio.wait_for(asyncio.gather(mentor, generation), 2)
        assert order == ["a1", "b1"]  # a1's own lane takes it first, then generation goes on


WORKER = """
import asyncio, sys
from datetime import timedelta
from sqlalchemy.ext.asyncio import create_async_engine
from wicket import FairShare, Lane, Pool, Priority, Rate
from wicket.postgres import PostgresLedger

async def main(url: str, lane: str) -> None:
    engine, minute = create_async_engine(url), timedelta(minutes=1)
    pool = Pool(
        PostgresLedger(engine, timedelta(hours=1)), "p",
        lane=Lane(lane, Rate(3, per=minute)), limits=(Rate(5, per=minute),),
        priority=Priority((FairShare(), 1)), lease=timedelta(hours=1),
        poll=timedelta(milliseconds=20),
    )
    while True:
        async with pool.permit() as permit:
            print(permit.waiting.ticket.id, flush=True)

asyncio.run(main(*sys.argv[1:]))
"""


async def test_lanes_in_several_os_processes_never_exceed_the_pool_or_lane_limits(url: URL):
    # the test clock stands still, so every grant falls in one minute of both rates
    async with emptied(url, EPOCH) as engine:
        async with AsyncSession(engine) as session:
            for n in range(6):
                for lane in "mg":
                    ticket = Ticket(f"{lane}{n}", f"t{n % 2}", lane=lane)
                    await PostgresBacklog(session).submit("p", ticket)
            await session.commit()
        address = url.render_as_string(hide_password=False)
        workers = [
            await asyncio.create_subprocess_exec(
                sys.executable, "-c", WORKER, address, lane, stdout=PIPE
            )
            for lane in "mmg"  # two processes of one lane, one of the other
        ]
        ledger = PostgresLedger(engine, HOUR)
        async with asyncio.timeout(30):
            while len((await ledger.view("p")).grants) < 5:
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)  # time enough for a sixth grant, if the limits let one through
        for worker in workers:
            worker.terminate()
        outputs = [(await worker.communicate())[0].decode().split() for worker in workers]
        grants = (await ledger.view("p")).grants
    started = [id for output in outputs for id in output]
    assert len(started) == len(set(started)) == len(grants) == 5
    assert sum(id.startswith("m") for id in started) <= 3
    assert sum(id.startswith("g") for id in started) <= 3


COUNT_LANE_WRITES = (
    "create table wicket_test_lane_writes (lane text not null)",
    "create function wicket_test_note_lane_write() returns trigger language plpgsql as"
    " $$ begin insert into wicket_test_lane_writes values (new.lane); return new; end $$",
    "create trigger wicket_test_lane_writes after insert or update on wicket_lanes"
    " for each row execute function wicket_test_note_lane_write()",
)
UNCOUNT_LANE_WRITES = (
    "drop trigger wicket_test_lane_writes on wicket_lanes",
    "drop function wicket_test_note_lane_write()",
    "drop table wicket_test_lane_writes",
)


@pytest.mark.parametrize("outcome", ["commit", "rollback"])
async def test_attend_waiting_for_a_first_insert_uses_the_clock_after_the_wait(
    engine: AsyncEngine, url: URL, outcome: str
):
    ttl = timedelta(seconds=2)
    ledger = PostgresLedger(engine, HOUR)
    async with engine.connect() as holding:
        transaction = await holding.begin()
        old_token = await holding.scalar(
            text(
                "insert into wicket_lanes (pool, lane, present_until) values ('p', 'm', "
                "clock_timestamp() + interval '10 seconds') returning token"
            )
        )
        attending = asyncio.create_task(ledger.attend("p", "m", version=0, ttl=ttl))
        try:
            await until(
                url,
                "exists (select 1 from pg_stat_activity where datname = current_database()"
                " and wait_event_type = 'Lock' and query like 'INSERT INTO wicket_lanes%')",
            )
            await elapse(engine, timedelta(milliseconds=750))
            if outcome == "commit":
                await transaction.commit()
            else:
                await transaction.rollback()
            await asyncio.wait_for(attending, 2)
        finally:
            if not attending.done():
                attending.cancel()
                await asyncio.gather(attending, return_exceptions=True)
    view = await ledger.view("p")
    assert view.present[0].token != old_token
    if outcome == "commit":
        assert view.present[0].until == EPOCH + timedelta(seconds=10)
    else:
        assert view.present[0].until == view.now + ttl


@pytest.mark.parametrize("changed", [True, False])
async def test_an_attend_waiting_for_a_lane_row_uses_the_clock_after_the_wait(
    engine: AsyncEngine, url: URL, changed: bool
):
    ttl = timedelta(seconds=1)
    ledger = PostgresLedger(engine, HOUR)
    await ledger.attend("p", "m", version=0, ttl=ttl)
    async with engine.connect() as holding:
        transaction = await holding.begin()
        if changed:
            await holding.execute(
                text("update wicket_lanes set present_until = '-infinity' where lane = 'm'")
            )
        else:
            await holding.execute(text("select 1 from wicket_lanes where lane = 'm' for update"))
        attending = asyncio.create_task(ledger.attend("p", "m", version=0, ttl=ttl))
        try:
            await until(
                url,
                "exists (select 1 from pg_stat_activity where datname = current_database()"
                " and wait_event_type = 'Lock' and query like '%wicket_lanes%')",
            )
            await elapse(engine, timedelta(seconds=2))
            await transaction.commit()
            await asyncio.wait_for(attending, 2)
        finally:
            attending.cancel()
            await asyncio.gather(attending, return_exceptions=True)
    view = await ledger.view("p")
    assert len(view.present) == 1
    assert view.present[0].until == view.now + ttl


async def test_a_leave_waiting_for_a_concurrent_attend_does_not_remove_it(
    engine: AsyncEngine, url: URL
):
    ledger = PostgresLedger(engine, HOUR)
    await ledger.attend("p", "m", version=0, ttl=timedelta(seconds=10))
    stale = (await ledger.view("p")).present[0]
    async with engine.connect() as holding:
        transaction = await holding.begin()
        # Simulate an uncommitted attend that has replaced the token.
        token = await holding.scalar(
            text(
                "update wicket_lanes set token = gen_random_uuid()"
                " where pool = 'p' and lane = 'm' returning token"
            )
        )
        leaving = asyncio.create_task(ledger.leave("p", stale))
        try:
            await until(
                url,
                "exists (select 1 from pg_stat_activity where datname = current_database()"
                " and wait_event_type = 'Lock' and query like 'UPDATE wicket_lanes%')",
            )
            await transaction.commit()
            await asyncio.wait_for(leaving, 2)
        finally:
            leaving.cancel()
            await asyncio.gather(leaving, return_exceptions=True)
    presence = (await ledger.view("p")).present
    assert len(presence) == 1 and presence[0].token == token != stale.token


async def test_workers_leaving_at_once_end_the_presence_once(engine: AsyncEngine):
    ledger = PostgresLedger(engine, HOUR)
    await ledger.attend("p", "m", version=0, ttl=timedelta(seconds=2))
    presence = (await ledger.view("p")).present[0]
    async with engine.begin() as connection:
        for statement in COUNT_LANE_WRITES:
            await connection.execute(text(statement))
    try:
        await asyncio.gather(*(ledger.leave("p", presence) for _ in range(10)))
        await ledger.leave("p", presence)
        async with engine.connect() as connection:
            writes = await connection.scalar(text("select count(*) from wicket_test_lane_writes"))
    finally:
        async with engine.begin() as connection:
            for statement in UNCOUNT_LANE_WRITES:
                await connection.execute(text(statement))
    assert (await ledger.view("p")).present == ()
    assert writes == 1


async def test_a_delayed_attend_cannot_refresh_after_its_lanes_grant(engine: AsyncEngine, url: URL):
    ledger, ttl = PostgresLedger(engine, HOUR), timedelta(seconds=2)
    async with AsyncSession(engine) as session:
        await PostgresBacklog(session).submit("p", Ticket("m1", "ann", lane="mentor"))
        await session.commit()
    await ledger.attend("p", "mentor", version=0, ttl=ttl)
    stale = await ledger.view("p")
    presence = stale.present[0]
    async with engine.connect() as holding:
        transaction = await holding.begin()
        await holding.execute(text("select 1 from wicket_lanes where lane='mentor' for update"))
        attending = asyncio.create_task(ledger.attend("p", "mentor", version=0, ttl=ttl))
        try:
            await until(
                url,
                "exists (select 1 from pg_stat_activity where datname=current_database()"
                " and wait_event_type='Lock' and query like '%wicket_lanes%FOR UPDATE%')",
            )
            # Grant must not wait for the lane row; the later attendance must reject the old view.
            granted = await asyncio.wait_for(
                ledger.grant(
                    "p",
                    stale.waiting[0],
                    version=stale.version,
                    lease=LEASE,
                    fairness=stale.fairness,
                ),
                3,
            )
            assert isinstance(granted, Permit)
            await transaction.commit()
            assert await asyncio.wait_for(attending, 3) == Conflict()
        finally:
            if not attending.done():
                attending.cancel()
                await asyncio.gather(attending, return_exceptions=True)
    assert (await ledger.view("p")).present == (presence,)


async def test_a_view_that_missed_a_pending_renewal_cannot_mark_its_lane_ready(
    engine: AsyncEngine, url: URL
):
    ledger, lease = PostgresLedger(engine, HOUR), timedelta(seconds=10)
    async with AsyncSession(engine) as session:
        await PostgresBacklog(session).submit("p", Ticket("m1", "ann", lane="mentor"))
        await session.commit()
    permit = await grant_first(ledger, lease=lease)
    assert isinstance(permit, Permit)
    await elapse(engine, timedelta(seconds=9))
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "create function test_hold_renewal() returns trigger language plpgsql as $$"
                " begin perform pg_advisory_xact_lock(789132,41); return new; end $$"
            )
        )
        await connection.execute(
            text(
                "create trigger test_hold_renewal after update on wicket_tickets"
                " for each row execute function test_hold_renewal()"
            )
        )
    renewing = attending = None
    try:
        async with engine.connect() as holding:
            await holding.execute(text("select pg_advisory_lock(789132,41)"))
            try:
                renewing = asyncio.create_task(ledger.renew(permit, lease=lease))
                await until(
                    url,
                    "exists (select 1 from pg_stat_activity where datname=current_database()"
                    " and wait_event_type='Lock' and query like 'UPDATE wicket_tickets%')",
                )
                await elapse(engine, timedelta(seconds=2))
                stale = await ledger.view("p")
                assert stale.leased == ()  # the old lease expired; its renewal is not committed
                attending = asyncio.create_task(
                    ledger.attend("p", "mentor", version=stale.version, ttl=timedelta(seconds=2))
                )
                await until(
                    url,
                    "exists (select 1 from pg_stat_activity where datname=current_database()"
                    " and wait_event_type='Lock' and query like 'SELECT wicket_pools.version%')",
                )
            finally:
                await holding.execute(text("select pg_advisory_unlock(789132,41)"))
            assert await asyncio.wait_for(renewing, 3) == Renewed()
            assert await asyncio.wait_for(attending, 3) == Conflict()
    finally:
        for task in (renewing, attending):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (renewing, attending) if task is not None), return_exceptions=True
        )
        async with engine.begin() as connection:
            await connection.execute(text("drop trigger test_hold_renewal on wicket_tickets"))
            await connection.execute(text("drop function test_hold_renewal()"))
    assert (await ledger.view("p")).present == ()


async def test_cancelling_an_attend_waiting_for_the_pool_releases_its_lane(
    engine: AsyncEngine, url: URL
):
    ledger, ttl = PostgresLedger(engine, HOUR), timedelta(seconds=10)
    await ledger.attend("p", "new", version=0, ttl=ttl)
    await ledger.leave("p", (await ledger.view("p")).present[0])
    await ledger.attend("p", "warm", version=0, ttl=ttl)
    before = await ledger.view("p")
    async with engine.begin() as holding:
        await holding.execute(text("select name from wicket_pools where name='p' for update"))
        attending = asyncio.create_task(ledger.attend("p", "new", version=0, ttl=ttl))
        try:
            await until(
                url,
                "exists (select 1 from pg_stat_activity where datname=current_database()"
                " and wait_event_type='Lock' and query like 'SELECT wicket_pools.version%')",
            )
            attending.cancel()
            assert isinstance(
                (await asyncio.gather(attending, return_exceptions=True))[0], asyncio.CancelledError
            )
            async with engine.connect() as probe:
                result = await probe.execute(
                    text(
                        "select lane from wicket_lanes where pool='p' and lane='new'"
                        " for update nowait"
                    )
                )
                assert result.scalar_one() == "new"
        finally:
            if not attending.done():
                attending.cancel()
                await asyncio.gather(attending, return_exceptions=True)
    assert await ledger.view("p") == before
    assert await ledger.attend("p", "new", version=0, ttl=ttl) == Attended()
    assert {presence.lane for presence in (await ledger.view("p")).present} == {"warm", "new"}
    assert given_back(engine)


@pytest.mark.parametrize("outcome", ["cancel", "error"])
async def test_a_failed_async_commit_hook_gives_back_the_connection(
    engine: AsyncEngine, url: URL, outcome: str
):
    await submit(engine, "1")
    ledger = PostgresLedger(engine, HOUR)
    await ledger.attend("p", "warm", version=0, ttl=LEASE)  # a committed pool row to lock
    before = await ledger.view("p")
    entering, resume = asyncio.Event(), asyncio.Event()

    def pause_commit(connection: Any) -> None:
        entering.set()
        connection.connection.dbapi_connection.run_async(lambda driver: resume.wait())
        if outcome == "error":
            raise RuntimeError("commit hook failed")

    async with opened(url) as work_engine:
        event.listen(work_engine.sync_engine, "commit", pause_commit)
        work = PostgresLedger(work_engine, HOUR)
        task = asyncio.create_task(
            work.grant(
                "p",
                before.waiting[0],
                version=before.version,
                lease=LEASE,
                fairness=before.fairness,
            )
        )
        try:
            await asyncio.wait_for(entering.wait(), 5)
            if outcome == "cancel":
                task.cancel()
            else:
                resume.set()
            answer = (await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5))[0]
            if outcome == "cancel":
                assert isinstance(answer, asyncio.CancelledError)
            else:
                assert isinstance(answer, RuntimeError) and str(answer) == "commit hook failed"
        finally:
            resume.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            event.remove(work_engine.sync_engine, "commit", pause_commit)
        assert given_back(work_engine)
        async with engine.begin() as probe:
            row = await probe.scalar(
                text("select name from wicket_pools where name = 'p' for update nowait")
            )
            assert row == "p"
        assert await ledger.view("p") == before
        assert await work.view("p") == before


@pytest.mark.parametrize("cancel", [False, True], ids=["commit", "cancel"])
async def test_an_attend_races_with_the_first_grant(engine: AsyncEngine, url: URL, cancel: bool):
    await submit(engine, "1")
    ledger = PostgresLedger(engine, HOUR)
    stale = await ledger.view("p")
    updated, resume = asyncio.Event(), asyncio.Event()

    def pause(connection, cursor, statement, *args):
        if statement.lstrip().startswith("INSERT INTO wicket_pools"):
            updated.set()
            await_(resume.wait())

    async with opened(url) as grant_engine:
        event.listen(grant_engine.sync_engine, "after_cursor_execute", pause)
        granting = asyncio.create_task(
            PostgresLedger(grant_engine, HOUR).grant(
                "p", stale.waiting[0], version=0, lease=LEASE, fairness=stale.fairness
            )
        )
        attending = None
        try:
            await updated.wait()
            attending = asyncio.create_task(ledger.attend("p", "", version=0, ttl=LEASE))
            await until(
                url,
                "exists (select 1 from pg_stat_activity where datname=current_database()"
                " and wait_event_type='Lock' and query like 'INSERT INTO wicket_pools%')",
            )
            if cancel:
                granting.cancel()
            resume.set()
            result = (await asyncio.gather(granting, return_exceptions=True))[0]
            attended = await asyncio.wait_for(attending, 3)
        finally:
            resume.set()
            for task in (granting, attending):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (granting, attending) if task), return_exceptions=True
            )
            event.remove(grant_engine.sync_engine, "after_cursor_execute", pause)
        assert given_back(grant_engine)
    view = await ledger.view("p")
    if cancel:
        assert isinstance(result, asyncio.CancelledError)
        assert attended == Attended() and len(view.present) == 1 and view.version == 0
    else:
        assert isinstance(result, Permit)
        assert attended == Conflict() and view.present == () and view.version == 1


async def test_a_first_attend_creates_the_pool_before_a_grant(engine: AsyncEngine, url: URL):
    await submit(engine, "1")
    ledger = PostgresLedger(engine, HOUR)
    stale = await ledger.view("p")
    updated, resume = asyncio.Event(), asyncio.Event()

    def pause(connection, cursor, statement, *args):
        if statement.lstrip().startswith("UPDATE wicket_lanes"):
            updated.set()
            await_(resume.wait())

    async with opened(url) as attending_engine:
        event.listen(attending_engine.sync_engine, "after_cursor_execute", pause)
        attending = asyncio.create_task(
            PostgresLedger(attending_engine, HOUR).attend("p", "", version=0, ttl=LEASE)
        )
        granting = None
        try:
            await updated.wait()
            granting = asyncio.create_task(
                ledger.grant("p", stale.waiting[0], version=0, lease=LEASE, fairness=stale.fairness)
            )
            await until(
                url,
                "exists (select 1 from pg_stat_activity where datname=current_database()"
                " and wait_event_type='Lock' and query like 'INSERT INTO wicket_pools%')",
            )
            resume.set()
            assert await attending == Attended()
            assert isinstance(await granting, Permit)
        finally:
            resume.set()
            for task in (attending, granting):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (attending, granting) if task), return_exceptions=True
            )
            event.remove(attending_engine.sync_engine, "after_cursor_execute", pause)
        assert given_back(attending_engine)
    assert (await ledger.view("p")).version == 1


@pytest.mark.parametrize("operation", ["grant", "renew", "attend"])
async def test_cancelling_a_blocked_commit_releases_locks_and_rolls_back(
    engine: AsyncEngine, url: URL, operation: str
):
    await submit(engine, "1")
    ledger = PostgresLedger(engine, HOUR)
    permit = await grant_first(ledger) if operation == "renew" else None
    if operation == "renew":
        await elapse(engine, timedelta(seconds=1))
    else:
        await ledger.attend("p", "warm", version=0, ttl=LEASE)
        if operation == "attend":
            await ledger.attend("p", "new", version=0, ttl=LEASE)
            presence = next(p for p in (await ledger.view("p")).present if p.lane == "new")
            await ledger.leave("p", presence)
    before = await ledger.view("p")
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "create function test_wait_commit() returns trigger language plpgsql as $$"
                " begin perform pg_advisory_xact_lock(984312, 6); return new; end $$"
            )
        )
        for table in ("wicket_pools", "wicket_lanes"):
            await connection.execute(
                text(
                    f"create constraint trigger test_wait_commit after insert or update on {table}"
                    " deferrable initially deferred for each row"
                    " execute function test_wait_commit()"
                )
            )
    try:
        async with engine.connect() as holding:
            await holding.execute(text("select pg_advisory_lock(984312, 6)"))
            try:
                async with opened(url) as work_engine:
                    work = PostgresLedger(work_engine, HOUR)
                    if operation == "renew":
                        assert isinstance(permit, Permit)
                        call = work.renew(permit, lease=LEASE)
                    elif operation == "grant":
                        call = work.grant(
                            "p",
                            before.waiting[0],
                            version=before.version,
                            lease=LEASE,
                            fairness=before.fairness,
                        )
                    else:
                        call = work.attend("p", "new", version=before.version, ttl=LEASE)
                    task = asyncio.create_task(call)
                    try:
                        await until(
                            url,
                            f"exists (select {OTHERS} and wait_event_type = 'Lock'"
                            " and query like 'COMMIT%')",
                        )
                        task.cancel()
                        answer = (
                            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
                        )[0]
                        assert isinstance(answer, asyncio.CancelledError)
                    finally:
                        if not task.done():
                            task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                    assert given_back(work_engine)
                    async with engine.begin() as probe:
                        statement = "select name from wicket_pools where name='p' for update nowait"
                        assert await probe.scalar(text(statement)) == "p"
                        if operation == "attend":
                            statement = (
                                "select lane from wicket_lanes where pool = 'p' and lane = 'new'"
                                " for update nowait"
                            )
                            assert await probe.scalar(text(statement)) == "new"
            finally:
                await holding.execute(text("select pg_advisory_unlock(984312, 6)"))
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("set local lock_timeout = '5s'"))
            for table in ("wicket_pools", "wicket_lanes"):
                await connection.execute(text(f"drop trigger test_wait_commit on {table}"))
            await connection.execute(text("drop function test_wait_commit()"))
    assert await ledger.view("p") == before
    assert await ledger.attend("p", "", version=before.version, ttl=LEASE) == Attended()
    if operation == "renew":
        await elapse(engine, timedelta(seconds=29.5))
        assert (await ledger.view("p")).leased == ()
