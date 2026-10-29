"""A pool: named capacity whose workers wait for permits, granted by priority within limits."""

import asyncio
import random
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta

from wicket.limits import Lane, Limit
from wicket.priority import Priority
from wicket.results import (
    Conflict,
    LeaseLost,
    LeaseLostError,
    Missing,
    NoTicket,
    Renewed,
    StoreUnavailable,
)
from wicket.store import Ledger
from wicket.tickets import Permit, Waiting
from wicket.view import PoolView

_UNNAMED = Lane("")  # the lane of a pool whose work is of one kind


class Pool:
    """A plain class: its lock orders this pool's `permit()` calls, which wait for a grant one at
    a time, reading, granting and sleeping. These grants cannot conflict with each other;
    background renewals and other instances serving the same pool can still invalidate a view.
    It serves one lane of the pool and grants only its tickets; `limits` count every lane."""

    def __init__(
        self,
        ledger: Ledger,
        name: str,
        *,
        priority: Priority,
        lane: Lane = _UNNAMED,
        limits: tuple[Limit, ...] = (),
        lease: timedelta,
        poll: timedelta,
    ) -> None:
        if lease <= timedelta(0):
            raise ValueError(f"lease must be positive, not {lease}")
        if poll <= timedelta(0):
            raise ValueError(f"poll must be positive, not {poll}")
        self._ledger = ledger
        self._name = name
        self._priority = priority
        self._lane = lane
        self._limits = limits
        self._lease = lease
        self._poll = poll
        self._turn = asyncio.Lock()

    @asynccontextmanager
    async def permit(self) -> AsyncGenerator[Permit, None]:
        """Wait for a grant and lease the ticket to the block, renewing the lease while it runs.
        Only a block that ends normally releases the ticket; an error, a cancellation or a lost
        lease (LeaseLostError) leave it to be delivered again once its lease ends. A grant whose
        response comes after its lease could have ended raises LeaseLostError before the block."""
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("permit() runs in an asyncio task")
        async with self._turn:
            permit, since = await self._granted()
        renewal = _Renewal(self._ledger, permit, self._lease, since, task)
        if asyncio.get_running_loop().time() >= since + self._lease.total_seconds():
            # the response came after the lease could have ended: never enter
            raise renewal.error
        keeper = asyncio.create_task(renewal.keep())
        cancelling = task.cancelling()
        try:
            yield permit
        except BaseException as error:
            keeper.cancel()
            only_ours = renewal.lost and task.uncancel() <= cancelling  # before any await
            await asyncio.wait([keeper])
            if only_ours:  # only the renewal cancelled it
                raise renewal.error from error
            raise
        keeper.cancel()
        lost = renewal.lost  # the body caught the cancellation and ended normally
        outside = lost and task.uncancel() > cancelling  # it swallowed one from outside as well
        await asyncio.wait([keeper])
        if outside:
            raise asyncio.CancelledError
        if lost:
            raise renewal.error
        await self._ledger.release(permit)  # LeaseLost: re-leased or withdrawn since; nothing to do

    async def _granted(self) -> tuple[Permit, float]:
        """The permit, and the event loop's time just before the grant that made it."""
        while True:
            view = await self._ledger.view(self._name)
            alone = view.in_lanes(self._lane.name) == view
            if not alone:  # another lane may choose while this grant is on its way: be seen first
                await self._attend(view)
            match await self._attempt(view):
                case (Permit() as permit, float() as since):
                    return permit, since
                case Conflict():
                    continue  # a grant or renewal changed the version: read again at once
                case NoTicket():
                    if alone:  # waiting at a limit: be seen by a lane that comes later
                        await self._attend(view)
                    seconds = self._poll.total_seconds()
                    await asyncio.sleep(random.uniform(seconds / 2, seconds))

    async def _attend(self, view: PoolView) -> None:
        """Keep this lane present while it has a ticket its own limits let it take; leave once it
        has none. A presence never changes the version."""
        own = view.in_lanes(self._lane.name)
        candidate = bool(own.waiting) and self._lane.admits(view)
        if candidate:
            if all(presence.until <= view.now + self._poll for presence in own.present):
                await self._ledger.attend(self._name, self._lane.name, ttl=2 * self._poll)
        elif own.present:
            await self._ledger.leave(self._name, self._lane.name)

    async def _attempt(self, view: PoolView) -> tuple[Permit, float] | Conflict | NoTicket:
        if not (self._lane.admits(view) and all(limit.admits(view) for limit in self._limits)):
            return NoTicket()
        lane = self._lane.name
        tenants = {w.ticket.tenant for w in view.waiting}
        choice = view.in_lanes(lane, *(presence.lane for presence in view.present))
        while True:
            match self._priority.first(choice):
                case NoTicket() as nothing:
                    return nothing
                case Waiting() as chosen if chosen.ticket.lane != lane:
                    return NoTicket()  # another lane's turn: its worker takes it
                case Waiting() as chosen:
                    since = asyncio.get_running_loop().time()
                    answer = await self._ledger.grant(
                        self._name,
                        chosen,
                        version=view.version,
                        lease=self._lease,
                        fairness=view.fairness.after(chosen, tenants),
                    )
                    match answer:
                        case Permit() as permit:
                            return permit, since
                        case Conflict() as conflict:
                            return conflict
                        case Missing():
                            # gone or held: the next of this lane only, or every lane could yield
                            # to a ticket nobody can take
                            choice = choice.in_lanes(lane).without(chosen.ticket.id)


class _Renewal:
    """Keeps a permit's lease while its block runs, and cancels the block once the lease is lost.
    A plain class: it notes that the lease was lost, and the error the block leaves with."""

    def __init__(
        self, ledger: Ledger, permit: Permit, lease: timedelta, since: float, owner: asyncio.Task
    ) -> None:
        self._ledger = ledger
        self._permit = permit
        self._lease = lease
        self._since = since  # event loop time from which the store's lease surely runs
        self._owner = owner
        self.lost = False
        self.error: Exception = LeaseLostError(
            f"the lease of ticket {permit.waiting.ticket.id!r} in pool {permit.pool!r} was lost; "
            "it will be delivered again"
        )

    async def keep(self) -> None:
        try:
            await self._renewing()
        except Exception as error:  # the adapter's bug: stop the block and let it propagate
            self.error = error
            self._lose()

    async def _renewing(self) -> None:
        loop, seconds = asyncio.get_running_loop(), self._lease.total_seconds()
        while True:
            # due from when the lease surely began, not from when its answer came
            await asyncio.sleep(max(0.0, self._since + seconds / 3 - loop.time()))
            while True:
                tried, last_try = loop.time(), self._since + seconds - seconds / 30
                deadline = asyncio.timeout_at(last_try)
                try:
                    async with deadline:
                        answer = await self._ledger.renew(self._permit, lease=self._lease)
                except TimeoutError:
                    if not deadline.expired():
                        raise  # the ledger's own timeout: its bug, keep() carries it on
                    self._lose()  # a renewal that hangs until the lease is about to end
                    return
                except StoreUnavailable:
                    if loop.time() + seconds / 30 >= self._since + seconds:
                        self._lose()
                        return
                    await asyncio.sleep(seconds / 30)
                    continue
                match answer:
                    case Renewed():
                        self._since = tried
                        break
                    case LeaseLost():
                        self._lose()
                        return

    def _lose(self) -> None:
        self.lost = True
        self._owner.cancel()
