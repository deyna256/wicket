"""Pool on the in-memory store. The event loop's time goes on at once over every wait (looptime),
and the store's clock follows it, so leases, renewals and polls share one time line."""

import asyncio
import contextlib
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from wicket.fairness import Fairness
from wicket.limits import Concurrency, Lane, Limit, Rate
from wicket.memory import MemoryBacklog, MemoryLedger, MemoryState
from wicket.pool import Pool
from wicket.priority import FairShare, Priority, TraitFactor
from wicket.results import (
    Conflict,
    LeaseLost,
    LeaseLostError,
    Missing,
    Released,
    Renewed,
    StoreUnavailable,
)
from wicket.tickets import Permit, Ticket, Trait, Version, Waiting
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
        self.attendance: list[str] = []  # "lane" for an attend, "-lane" for a leave

    async def view(self, pool: str) -> PoolView:
        self.views += 1
        return await self.ledger.view(pool)

    async def grant(
        self, pool: str, waiting: Waiting, *, version: Version, lease: timedelta, fairness: Fairness
    ) -> Permit | Conflict | Missing:
        if self.grants:
            return self.grants.pop(0)
        return await self.ledger.grant(
            pool, waiting, version=version, lease=lease, fairness=fairness
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

    async def attend(self, pool: str, lane: str, *, ttl: timedelta) -> None:
        self.attendance.append(lane)
        await self.ledger.attend(pool, lane, ttl=ttl)

    async def leave(self, pool: str, lane: str) -> None:
        self.attendance.append(f"-{lane}")
        await self.ledger.leave(pool, lane)


def pool(
    ledger: MemoryLedger | Scripted,
    limits: tuple[Limit, ...] = (),
    lane: Lane = Lane(""),  # noqa: B008
    poll: timedelta = POLL,
    priority: Priority = FAIR,
) -> Pool:
    return Pool(ledger, "p", priority=priority, lane=lane, limits=limits, lease=LEASE, poll=poll)


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
        self, pool: str, waiting: Waiting, *, version: Version, lease: timedelta, fairness: Fairness
    ) -> Permit | Conflict | Missing:
        await asyncio.sleep(0)
        return await super().grant(pool, waiting, version=version, lease=lease, fairness=fairness)


class Unreachable(Scripted):
    """A ledger whose first view cannot reach the store."""

    async def view(self, pool: str) -> PoolView:
        if self.views == 0:
            self.views += 1
            raise StoreUnavailable("down")
        return await super().view(pool)


class CancelledFromOutside(Scripted):
    """A ledger whose first renewal finds the worker cancelled from outside, then answers that the
    lease is lost."""

    def __init__(self, ledger: MemoryLedger) -> None:
        super().__init__(ledger)
        self.worker: asyncio.Task | None = None

    async def renew(self, permit: Permit, *, lease: timedelta) -> Renewed | LeaseLost:
        assert self.worker is not None
        self.worker.cancel()
        return LeaseLost()


class SlowRenewals(Scripted):
    """A ledger whose renewals take 5 s; it logs when they start and stop, and releases."""

    def __init__(self, ledger: MemoryLedger) -> None:
        super().__init__(ledger)
        self.log: list[str] = []

    async def renew(self, permit: Permit, *, lease: timedelta) -> Renewed | LeaseLost:
        self.log.append("renew")
        try:
            await asyncio.sleep(5)
            return await self.ledger.renew(permit, lease=lease)
        finally:
            self.log.append("renew stopped")

    async def release(self, permit: Permit) -> Released | LeaseLost:
        self.log.append("release")
        return await self.ledger.release(permit)


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
    scripted = CancelledFromOutside(ledger)

    async def worker() -> None:
        async with pool(scripted).permit():
            await asyncio.sleep(1000)

    scripted.worker = asyncio.create_task(worker())
    with pytest.raises(asyncio.CancelledError):
        await scripted.worker


async def test_a_cancellation_from_outside_wins_even_when_the_body_swallows_both():
    _, ledger = await submitted()
    scripted = CancelledFromOutside(ledger)

    async def worker() -> None:
        async with pool(scripted).permit():
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(1000)

    scripted.worker = asyncio.create_task(worker())
    with pytest.raises(asyncio.CancelledError):
        await scripted.worker


@pytest.mark.parametrize("swallow", [False, True])
async def test_a_cancellation_landing_while_the_renewal_stops_is_counted_once(swallow: bool):
    _, ledger = await submitted()

    async def worker() -> None:
        async with pool(Scripted(ledger, renews=[LeaseLost()])).permit():
            try:
                await asyncio.sleep(1000)
            except asyncio.CancelledError:
                task = asyncio.current_task()
                assert task is not None
                asyncio.get_running_loop().call_soon(task.cancel)  # from outside, on the next tick
                if not swallow:
                    raise

    task = asyncio.create_task(worker())
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelling() == 1


async def test_a_timeout_of_the_ledger_itself_is_its_error_not_a_lost_lease():
    _, ledger = await submitted()
    with pytest.raises(TimeoutError, match="socket"):
        async with pool(Scripted(ledger, renews=[TimeoutError("socket")])).permit():
            await asyncio.sleep(1000)
    assert await attempt_of_next_grant(ledger) == 2


async def test_the_release_waits_for_a_renewal_in_flight_to_stop():
    _, ledger = await submitted()
    slow = SlowRenewals(ledger)
    async with pool(slow).permit():
        await asyncio.sleep(12)  # the first renewal starts at 10 s and is still running
    assert slow.log == ["renew", "renew stopped", "release"]


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


class LateGrant(Scripted):
    """A ledger that answers a successful grant `delay` seconds after the store made it."""

    def __init__(self, ledger: MemoryLedger, delay: float) -> None:
        super().__init__(ledger)
        self.delay = delay

    async def grant(
        self, pool: str, waiting: Waiting, *, version: Version, lease: timedelta, fairness: Fairness
    ) -> Permit | Conflict | Missing:
        answer = await super().grant(pool, waiting, version=version, lease=lease, fairness=fairness)
        if isinstance(answer, Permit):
            await asyncio.sleep(self.delay)
        return answer


async def test_the_first_renewal_is_due_from_the_grant_not_its_response():
    _, ledger = await submitted()
    late = LateGrant(ledger, 25)
    async with pool(late).permit():
        await asyncio.sleep(25)  # past the lease's end: only a renewal at once keeps it
    assert late.renewals >= 1


@pytest.mark.parametrize("delay", [30, 31])  # at the lease's end, and after it
async def test_a_grant_answered_once_its_lease_could_have_ended_never_enters_the_block(delay: int):
    _, ledger = await submitted()
    late, entered = LateGrant(ledger, delay), False
    with pytest.raises(LeaseLostError):
        async with pool(late).permit():
            entered = True  # never suspends, so no renewal could stop it
    assert not entered
    assert late.renewals == 0
    assert await attempt_of_next_grant(ledger) == 2


async def work(serving: Pool, order: list[str], seconds: float) -> None:
    async with serving.permit() as permit:
        order.append(permit.waiting.ticket.id)
        await asyncio.sleep(seconds)


async def test_a_pool_grants_only_tickets_of_its_lane():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("g1", "bob", lane="g"))
    await backlog.submit("p", Ticket("m1", "ann", lane="m"))
    async with pool(ledger, lane=Lane("m")).permit() as permit:
        assert permit.waiting.ticket.id == "m1"


async def test_when_the_pool_is_full_the_next_slot_goes_to_whoever_is_due_in_any_lane():
    backlog, ledger = store()
    for id, tenant, lane in [("m1", "alice", "m"), ("m2", "alice", "m"), ("g1", "bob", "g")]:
        await backlog.submit("p", Ticket(id, tenant, lane=lane))
    full = (Concurrency(1),)
    mentor = pool(ledger, full, Lane("m"), poll=timedelta(seconds=0.1))
    generation = pool(ledger, full, Lane("g"), poll=timedelta(seconds=5))
    order: list[str] = []
    first = asyncio.create_task(work(mentor, order, 10))  # alice-m1 at once: nobody else waits
    await asyncio.sleep(1)
    await asyncio.gather(first, work(mentor, order, 10), work(generation, order, 10))
    assert order == ["m1", "g1", "m2"]  # mentor polls first after the release, yet yields to bob


async def test_a_lane_at_its_own_cap_does_not_hold_back_another():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("m1", "alice", lane="m"))
    await asyncio.sleep(1)
    await backlog.submit("p", Ticket("m2", "carol", lane="m"))  # due before bob: submitted first
    await asyncio.sleep(1)
    await backlog.submit("p", Ticket("g1", "bob", lane="g"))
    capped = Lane("m", Concurrency(1))
    order: list[str] = []
    holder = asyncio.create_task(work(pool(ledger, lane=capped), order, 60))
    waiter = asyncio.create_task(work(pool(ledger, lane=capped), order, 0))
    await asyncio.sleep(0.5)
    start = now()
    await work(pool(ledger, lane=Lane("g")), order, 0)
    assert now() - start == pytest.approx(0)  # mentor absent itself: nobody to yield to
    assert order == ["m1", "g1"]
    holder.cancel()
    waiter.cancel()
    await asyncio.gather(holder, waiter, return_exceptions=True)


async def test_the_presence_of_a_worker_gone_away_expires_and_the_others_go_on():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("g0", "bob", lane="g"))
    await backlog.submit("p", Ticket("m1", "carol", lane="m"))
    await asyncio.sleep(1)
    await backlog.submit("p", Ticket("g1", "bob", lane="g"))
    full = (Concurrency(1),)
    order: list[str] = []
    busy = asyncio.create_task(work(pool(ledger, full, Lane("g")), order, 1.5))  # takes g0
    await asyncio.sleep(0)
    gone = asyncio.create_task(work(pool(ledger, full, Lane("m")), order, 0))  # present, then dies
    await asyncio.sleep(1.2)
    gone.cancel()
    await asyncio.gather(gone, return_exceptions=True)
    await asyncio.gather(busy, work(pool(ledger, full, Lane("g")), order, 0))
    # yielded to carol only until her lane's presence expired (by 4 s), then one poll at most
    assert order == ["g0", "g1"] and now() < 6


async def test_a_pool_without_lanes_and_a_lane_with_nothing_waiting_write_no_presence():
    backlog, ledger = store()
    for n in range(3):
        await backlog.submit("p", Ticket(f"a{n}", "a"))
    plain = Scripted(ledger)
    for _ in range(3):
        async with pool(plain).permit():
            pass
    await backlog.submit("p", Ticket("g1", "bob", lane="g"))
    idle = Scripted(ledger)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(3), pool(idle, lane=Lane("m")).permit():
            pass
    assert (plain.attendance, idle.attendance) == ([], [])


async def test_after_a_missing_ticket_the_lane_chooses_again_among_its_own():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("c0", "carol", lane="m"))
    async with pool(ledger, lane=Lane("m")).permit():
        pass  # carol was served: her tag is now the largest
    for id, tenant, lane in [("held", "alice", "m"), ("c1", "carol", "m"), ("g1", "bob", "g")]:
        await backlog.submit("p", Ticket(id, tenant, lane=lane))
        await asyncio.sleep(1)
    await ledger.attend("p", "g", ttl=timedelta(hours=1))
    held = Scripted(ledger, grants=[Missing()])  # alice's ticket, the winner, is held elsewhere
    async with pool(held, lane=Lane("m")).permit() as permit:
        assert permit.waiting.ticket.id == "c1"  # not a yield to bob, who waits on alice too


async def test_a_presence_removed_by_a_stale_leave_is_back_within_one_poll():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("g0", "bob", lane="g"))
    await backlog.submit("p", Ticket("g1", "bob", lane="g"))
    await backlog.submit("p", Ticket("m1", "carol", lane="m"))
    full = (Concurrency(1),)
    order: list[str] = []
    busy = asyncio.create_task(work(pool(ledger, full, Lane("g")), order, 60))
    await asyncio.sleep(0)
    waiting = asyncio.create_task(work(pool(ledger, full, Lane("m")), order, 0))
    await asyncio.sleep(0.5)
    await ledger.leave("p", "m")
    await asyncio.sleep(POLL.total_seconds())
    assert "m" in [presence.lane for presence in (await ledger.view("p")).present]
    for task in (busy, waiting):
        task.cancel()
    await asyncio.gather(busy, waiting, return_exceptions=True)


async def test_a_lane_keeps_its_turn_while_another_lanes_tickets_are_taken_as_they_arrive():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("m1", "alice", lane="m"))
    await backlog.submit("p", Ticket("g0", "bob", lane="g"))
    once = (Rate(1, per=timedelta(seconds=10)),)
    order: list[str] = []

    async def generation() -> None:
        for n in range(1, 4):
            await work(pool(ledger, once, Lane("g")), order, 0)
            await asyncio.sleep(10)  # bob's next ticket comes just as the rate frees up
            await backlog.submit("p", Ticket(f"g{n}", "bob", lane="g"))

    bob = asyncio.create_task(generation())
    await asyncio.sleep(0.5)  # g0 is taken before alice's worker starts waiting
    alice = asyncio.create_task(work(pool(ledger, once, Lane("m")), order, 0))
    await asyncio.sleep(25)
    # generation never has a ticket waiting when mentor looks, yet its grants show it: mentor stays
    # present, and bob's next ticket yields to alice
    assert order[:2] == ["g0", "m1"]
    for task in (bob, alice):
        task.cancel()
    await asyncio.gather(bob, alice, return_exceptions=True)


async def test_a_redelivery_counts_in_the_pool_and_lane_limits():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("m1", "ann", lane="m"))
    await backlog.submit("p", Ticket("m2", "ann", lane="m"))
    limits, lane = (
        (Rate(3, per=timedelta(minutes=1)),),
        Lane("m", Rate(2, per=timedelta(minutes=1))),
    )
    with pytest.raises(RuntimeError):
        async with pool(ledger, limits, lane).permit():
            raise RuntimeError("the work failed")  # no release: m1 comes back after its lease
    await asyncio.sleep(LEASE.total_seconds())
    async with pool(ledger, limits, lane).permit() as again:
        assert (again.waiting.ticket.id, again.attempt) == ("m1", 2)
    async with pool(ledger, limits, lane).permit() as third:
        assert third.waiting.ticket.id == "m2"
    assert now() >= 60  # the lane's two grants a minute held m2 back, the redelivery among them
    assert [grant.lane for grant in (await ledger.view("p")).grants][-3:] == ["m", "m", "m"]


class CannotAttend(Scripted):
    """A ledger whose presences cannot reach the store."""

    async def attend(self, pool: str, lane: str, *, ttl: timedelta) -> None:
        raise StoreUnavailable("down")


async def test_a_store_unreachable_while_attending_propagates():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("m1", "ann", lane="m"))
    await backlog.submit("p", Ticket("g1", "bob", lane="g"))
    with pytest.raises(StoreUnavailable):
        async with pool(CannotAttend(ledger), lane=Lane("m")).permit():
            pass


async def test_lanes_ranking_each_other_first_never_grant_so_they_share_one_priority():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("a0", "alice", lane="m"))
    async with pool(ledger, lane=Lane("m")).permit():
        pass  # alice was served: by fair share bob is due, by tier alice is
    await backlog.submit("p", Ticket("a1", "alice", (Trait("tier", 1.0),), lane="m"))
    await backlog.submit("p", Ticket("b1", "bob", lane="g"))
    for lane in "mg":  # both lanes already present, as when their workers have waited a while
        await ledger.attend("p", lane, ttl=timedelta(hours=1))
    by_tier = Priority((TraitFactor("tier"), 1))
    order: list[str] = []
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(10):
            await asyncio.gather(
                work(pool(ledger, lane=Lane("m")), order, 0),
                work(pool(ledger, lane=Lane("g"), priority=by_tier), order, 0),
            )
    assert order == []


async def test_a_waiting_lane_writes_its_presence_about_once_a_poll_not_on_every_read():
    backlog, ledger = store()
    await backlog.submit("p", Ticket("g0", "bob", lane="g"))
    await backlog.submit("p", Ticket("m0", "ann", lane="m"))
    busy = asyncio.create_task(work(pool(ledger, (Concurrency(1),), Lane("g")), [], 60))
    await asyncio.sleep(0)
    waiting = Scripted(ledger)
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(30), pool(waiting, (Concurrency(1),), Lane("m")).permit():
            pass
    assert 0 < len(waiting.attendance) < waiting.views
    busy.cancel()
    await asyncio.gather(busy, return_exceptions=True)


async def test_tenants_of_a_lane_without_a_waiting_worker_keep_their_turns():
    backlog, ledger = store()
    for n in range(3):
        await backlog.submit("p", Ticket(f"a{n}", "alice", lane="m"))
    await asyncio.sleep(1)
    for n in range(3):
        await backlog.submit("p", Ticket(f"c{n}", "carol", lane="m"))
    for n in range(60):
        await backlog.submit("p", Ticket(f"b{n}", "bob", lane="g"))
    order: list[str] = []

    async def generation() -> None:
        while True:
            await work(pool(ledger, lane=Lane("g")), [], 1)

    async def mentor() -> None:
        for _ in range(4):
            await work(pool(ledger, lane=Lane("m")), order, 10)  # absent while it works

    bob = asyncio.create_task(generation())
    await mentor()
    bob.cancel()
    await asyncio.gather(bob, return_exceptions=True)
    assert order == ["a0", "c0", "a1", "c1"]  # by submission alone: a0, a1, a2, c0
