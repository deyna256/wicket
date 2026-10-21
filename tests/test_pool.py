"""Pool on the in-memory store. The event loop's time goes on at once over every wait (looptime),
and the store's clock follows it, so leases, renewals and polls share one time line."""

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from wicket.fairness import Fairness
from wicket.limits import Concurrency, Limit, Rate
from wicket.memory import MemoryBacklog, MemoryLedger, MemoryState
from wicket.pool import Pool
from wicket.priority import FairShare, Priority
from wicket.results import Conflict, LeaseLost, Missing, Released, Renewed
from wicket.tickets import Permit, Ticket, Version
from wicket.view import PoolView

pytestmark = pytest.mark.looptime

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
LEASE = timedelta(seconds=30)
POLL = timedelta(seconds=1)
FAIR = Priority((FairShare(), 1))


def now() -> float:
    return asyncio.get_running_loop().time()


def store() -> tuple[MemoryBacklog, MemoryLedger]:
    state = MemoryState(clock=lambda: EPOCH + timedelta(seconds=now()))
    return MemoryBacklog(state), MemoryLedger(state)


class Scripted:
    """A ledger that gives scripted answers first and the real ledger's after; it counts views and
    renewals."""

    def __init__(
        self,
        ledger: MemoryLedger,
        *,
        grants: Sequence[Conflict | Missing] = (),
        renews: Sequence[Renewed | LeaseLost | Exception] = (),
    ) -> None:
        self.ledger = ledger
        self.grants = list(grants)
        self.renews = list(renews)
        self.views = 0
        self.renewals = 0

    async def view(self, pool: str) -> PoolView:
        self.views += 1
        return await self.ledger.view(pool)

    async def grant(
        self, pool: str, ticket_id: str, *, version: Version, lease: timedelta, fairness: Fairness
    ) -> Permit | Conflict | Missing:
        if self.grants:
            return self.grants.pop(0)
        return await self.ledger.grant(
            pool, ticket_id, version=version, lease=lease, fairness=fairness
        )

    async def renew(self, permit: Permit, *, lease: timedelta) -> Renewed | LeaseLost:
        self.renewals += 1
        if self.renews:
            answer = self.renews.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer
        return await self.ledger.renew(permit, lease=lease)

    async def release(self, permit: Permit) -> Released | LeaseLost:
        return await self.ledger.release(permit)


def pool(ledger: MemoryLedger | Scripted, limits: tuple[Limit, ...] = ()) -> Pool:
    return Pool(ledger, "p", priority=FAIR, limits=limits, lease=LEASE, poll=POLL)


async def test_one_worker_after_another_serves_the_tenants_in_turn():
    backlog, ledger = store()
    for n in range(3):
        await backlog.submit("p", Ticket(f"a{n}", "a"))
        await backlog.submit("p", Ticket(f"b{n}", "b"))
    order = []
    for _ in range(6):
        async with pool(ledger).permit() as permit:
            order.append(permit.waiting.ticket.tenant)
    assert order == ["a", "b"] * 3


async def test_workers_reading_at_once_get_every_ticket_without_sleeping():
    backlog, ledger = store()
    for n in range(8):
        await backlog.submit("p", Ticket(str(n), f"t{n}"))
    shared, got, start = pool(ledger), [], now()

    async def worker() -> None:
        async with shared.permit() as permit:
            got.append(permit.waiting.ticket.id)
            await asyncio.sleep(10)

    await asyncio.gather(*(worker() for _ in range(8)))
    assert sorted(got) == [str(n) for n in range(8)]
    assert now() - start == pytest.approx(10)  # no poll: only the bodies' 10 s passed


async def test_a_conflict_reads_again_at_once():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("1", "a"))
    scripted, start = Scripted(ledger, grants=[Conflict(), Conflict()]), now()
    async with pool(scripted).permit() as permit:
        assert permit.waiting.ticket.id == "1"
    assert (scripted.views, now() - start) == (3, pytest.approx(0))


async def test_a_missing_ticket_is_passed_over_for_the_next_of_the_same_view():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("held", "a"))
    await asyncio.sleep(1)
    await backlog.submit("p", Ticket("next", "b"))
    scripted = Scripted(ledger, grants=[Missing()])
    async with pool(scripted).permit() as permit:
        assert permit.waiting.ticket.id == "next"
    assert scripted.views == 1


async def test_with_nothing_to_grant_a_worker_waits_and_takes_a_ticket_submitted_later():
    backlog, ledger = store()
    shared, start = pool(ledger), now()

    async def later() -> None:
        await asyncio.sleep(5)
        await backlog.submit("p", Ticket("1", "a"))

    submitting = asyncio.create_task(later())
    async with shared.permit() as permit:
        assert permit.waiting.ticket.id == "1"
    await submitting
    assert 5 <= now() - start <= 5 + POLL.total_seconds()


async def test_the_workers_of_one_pool_read_once_per_poll_between_them():
    _, ledger = store()
    scripted = Scripted(ledger)
    shared = pool(scripted)

    async def worker() -> None:
        async with shared.permit():
            pass

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(10):
            await asyncio.gather(*(worker() for _ in range(8)))
    assert scripted.views <= 10 / (POLL.total_seconds() / 2) + 1  # one sleeper at a time, not eight


async def test_limits_keep_a_worker_waiting():
    backlog, ledger = store()
    for id in "12":
        await backlog.submit("p", Ticket(id, "a"))
    limited = pool(ledger, limits=(Concurrency(1),))
    async with limited.permit():
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(5), limited.permit():
                pass


async def test_a_rate_longer_than_the_store_keeps_history_stops_the_worker():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("1", "a"))
    with pytest.raises(ValueError, match="retention"):
        async with pool(ledger, limits=(Rate(1, per=timedelta(days=1)),)).permit():
            pass


async def test_two_pools_with_the_same_ticket_id_lease_their_own():
    backlog, ledger = store()
    for name in ("a", "b"):
        await backlog.submit(name, Ticket("1", "t"))
    a, b = (Pool(ledger, name, priority=FAIR, lease=LEASE, poll=POLL) for name in ("a", "b"))
    async with a.permit() as in_a, b.permit() as in_b:
        assert (in_a.pool, in_b.pool) == ("a", "b")


async def test_a_block_that_ends_normally_releases_its_ticket():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("1", "a"))
    async with pool(ledger).permit():
        pass
    view = await ledger.view("p")
    assert (view.waiting, view.leased) == ((), ())


def test_a_pool_refuses_a_lease_or_poll_that_is_not_positive():
    ledger = MemoryLedger(MemoryState())
    with pytest.raises(ValueError, match="lease"):
        Pool(ledger, "p", priority=FAIR, lease=timedelta(0), poll=POLL)
    with pytest.raises(ValueError, match="poll"):
        Pool(ledger, "p", priority=FAIR, lease=LEASE, poll=timedelta(0))
