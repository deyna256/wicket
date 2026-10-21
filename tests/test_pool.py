"""Pool on the in-memory store. The event loop's time goes on at once over every wait (looptime),
and the store's clock follows it, so leases, renewals and polls share one time line."""

import asyncio
import contextlib
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from wicket.fairness import Fairness
from wicket.limits import Concurrency, Limit, Rate
from wicket.memory import MemoryBacklog, MemoryLedger, MemoryState
from wicket.pool import Pool
from wicket.priority import FairShare, Priority
from wicket.results import (
    Conflict,
    LeaseLost,
    LeaseLostError,
    Missing,
    Released,
    Renewed,
    StoreUnavailable,
)
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


async def submitted() -> tuple[MemoryBacklog, MemoryLedger]:
    backlog, ledger = store()
    await backlog.submit("p", Ticket("1", "a"))
    return backlog, ledger


async def attempt_of_next_grant(ledger: MemoryLedger) -> int:
    async with pool(ledger).permit() as permit:
        return permit.attempt


async def test_renewals_keep_a_body_longer_than_its_lease():
    _, ledger = await submitted()
    shared = pool(ledger)

    async def rival() -> None:
        with pytest.raises(TimeoutError):  # the ticket stays leased the whole time
            async with asyncio.timeout(90):
                async with shared.permit():
                    pass

    scripted = Scripted(ledger)
    async with pool(scripted).permit() as permit:
        await asyncio.gather(asyncio.sleep(100), rival())
    assert permit.attempt == 1
    assert (await ledger.view("p")).waiting == ()
    assert 9 <= scripted.renewals <= 10  # every lease / 3


async def test_a_lost_lease_cancels_the_block_and_the_ticket_comes_back():
    _, ledger = await submitted()
    with pytest.raises(LeaseLostError, match="ticket '1' in pool 'p'"):
        async with pool(Scripted(ledger, renews=[LeaseLost()])).permit():
            await asyncio.sleep(1000)
    assert await attempt_of_next_grant(ledger) == 2


async def test_a_store_away_briefly_is_retried_and_the_block_goes_on():
    _, ledger = await submitted()
    away = [StoreUnavailable("down")] * 3
    async with pool(Scripted(ledger, renews=away)).permit():
        await asyncio.sleep(100)
    assert (await ledger.view("p")).waiting == ()


async def test_a_store_away_until_the_lease_would_end_cancels_the_block_before_it_ends():
    _, ledger = await submitted()
    start = now()
    with pytest.raises(LeaseLostError):
        async with pool(Scripted(ledger, renews=[StoreUnavailable("down")] * 100)).permit():
            await asyncio.sleep(1000)
    assert now() - start < LEASE.total_seconds()


async def test_an_error_of_the_body_releases_nothing():
    _, ledger = await submitted()
    with pytest.raises(RuntimeError, match="the work failed"):
        async with pool(ledger).permit():
            raise RuntimeError("the work failed")
    assert await attempt_of_next_grant(ledger) == 2


async def test_a_cancellation_from_outside_releases_nothing():
    _, ledger = await submitted()

    async def worker() -> None:
        async with pool(ledger).permit():
            await asyncio.sleep(1000)

    task = asyncio.create_task(worker())
    await asyncio.sleep(1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await attempt_of_next_grant(ledger) == 2


async def test_a_body_that_swallows_the_cancellation_still_loses_its_lease():
    _, ledger = await submitted()

    async def swallowing() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(1000)

    with pytest.raises(LeaseLostError):
        async with pool(Scripted(ledger, renews=[LeaseLost()])).permit():
            await swallowing()
    await asyncio.sleep(1)  # the task goes on: the cancellation was used up
    assert await attempt_of_next_grant(ledger) == 2


async def test_a_release_after_the_lease_ends_raises_nothing_while_nobody_took_the_ticket():
    _, ledger = await submitted()
    keeps_nothing = [Renewed()] * 100  # renewals that answer but do not reach the store
    async with pool(Scripted(ledger, renews=keeps_nothing)).permit():
        await asyncio.sleep(40)  # past the lease; nobody took the ticket
    assert (await ledger.view("p")).waiting == ()


async def test_a_late_release_while_another_worker_holds_the_ticket_raises_nothing():
    _, ledger = await submitted()
    taken, done = asyncio.Event(), asyncio.Event()

    async def rival() -> None:
        async with pool(ledger).permit() as again:
            assert again.attempt == 2
            taken.set()
            await done.wait()

    async with pool(Scripted(ledger, renews=[Renewed()] * 100)).permit():
        await asyncio.sleep(LEASE.total_seconds())  # the lease ends unrenewed
        holder = asyncio.create_task(rival())
        await taken.wait()
    assert [p.attempt for p in (await ledger.view("p")).leased] == [2]  # the rival keeps it
    done.set()
    await holder


class Hanging(Scripted):
    """A ledger whose renewals never answer."""

    async def renew(self, permit: Permit, *, lease: timedelta) -> Renewed | LeaseLost:
        await asyncio.Event().wait()
        return Renewed()


class AwayFrom25(Scripted):
    """Real renewals until 25 s, then a store that cannot be reached."""

    async def renew(self, permit: Permit, *, lease: timedelta) -> Renewed | LeaseLost:
        if now() >= 25:
            raise StoreUnavailable("down")
        return await self.ledger.renew(permit, lease=lease)


class Yielding(Scripted):
    """Lets other coroutines run before each view and grant, as a real store's I/O does, so the
    pools of several "processes" interleave."""

    async def view(self, pool: str) -> PoolView:
        await asyncio.sleep(0)
        return await super().view(pool)

    async def grant(
        self, pool: str, ticket_id: str, *, version: Version, lease: timedelta, fairness: Fairness
    ) -> Permit | Conflict | Missing:
        await asyncio.sleep(0)
        return await super().grant(pool, ticket_id, version=version, lease=lease, fairness=fairness)


class Unreachable(Scripted):
    """A ledger whose first view cannot reach the store."""

    async def view(self, pool: str) -> PoolView:
        if self.views == 0:
            self.views += 1
            raise StoreUnavailable("down")
        return await super().view(pool)


async def test_a_renewal_that_hangs_cancels_the_block_before_the_lease_ends():
    _, ledger = await submitted()
    start = now()
    with pytest.raises(LeaseLostError):
        async with pool(Hanging(ledger)).permit():
            await asyncio.sleep(1000)
    assert now() - start < LEASE.total_seconds()


async def test_a_store_away_is_retried_until_the_last_renewed_lease_would_end():
    _, ledger = await submitted()
    start = now()
    with pytest.raises(LeaseLostError):
        async with pool(AwayFrom25(ledger)).permit():
            await asyncio.sleep(1000)
    assert 20 + LEASE.total_seconds() - 2 <= now() - start < 20 + LEASE.total_seconds()


async def test_an_error_of_the_ledger_while_renewing_stops_the_block_and_propagates():
    _, ledger = await submitted()
    with pytest.raises(RuntimeError, match="adapter bug"):
        async with pool(Scripted(ledger, renews=[RuntimeError("adapter bug")])).permit():
            await asyncio.sleep(1000)
    assert await attempt_of_next_grant(ledger) == 2


async def test_a_lost_lease_leaves_the_task_uncancelled():
    _, ledger = await submitted()

    async def swallowing() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(1000)

    async def failing() -> None:
        try:
            await asyncio.sleep(1000)
        except asyncio.CancelledError:
            raise RuntimeError("cleanup failed") from None

    for body in (lambda: asyncio.sleep(1000), swallowing, failing):
        await asyncio.sleep(LEASE.total_seconds())  # a lease lost before has ended
        with pytest.raises(LeaseLostError):
            async with pool(Scripted(ledger, renews=[LeaseLost()])).permit():
                await body()
        task = asyncio.current_task()
        assert task is not None and task.cancelling() == 0


async def test_a_cancellation_from_outside_wins_over_a_lost_lease():
    _, ledger = await submitted()
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(LEASE.total_seconds() / 3):  # fires with the first renewal
            async with pool(Scripted(ledger, renews=[LeaseLost()])).permit():
                await asyncio.sleep(1000)


async def test_workers_of_several_processes_never_exceed_the_limit():
    backlog, ledger = store()
    for n in range(12):
        await backlog.submit("p", Ticket(str(n), f"t{n % 3}"))
    shared = Yielding(ledger)
    pools = [pool(shared, limits=(Concurrency(2),)) for _ in range(3)]  # three "processes"
    running = peak = 0

    async def worker(serving: Pool) -> None:
        nonlocal running, peak
        for _ in range(2):
            async with serving.permit():
                running += 1
                peak = max(peak, running)
                await asyncio.sleep(5)
                running -= 1

    await asyncio.gather(*(worker(pools[n % 3]) for n in range(6)))
    assert peak == 2
    assert (await ledger.view("p")).waiting == ()


async def test_a_store_unreachable_while_waiting_propagates_and_frees_the_pool():
    _, ledger = await submitted()
    shared = pool(Unreachable(ledger))
    with pytest.raises(StoreUnavailable):
        async with shared.permit():
            pass
    async with shared.permit() as permit:  # the lock was let go
        assert permit.attempt == 1
