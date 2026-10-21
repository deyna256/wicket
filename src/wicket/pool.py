"""A pool: named capacity whose workers wait for permits, granted by priority within limits."""

import asyncio
import random
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta

from wicket.limits import Limit
from wicket.priority import Priority
from wicket.results import Conflict, Missing, NoTicket
from wicket.store import Ledger
from wicket.tickets import Permit, Waiting
from wicket.view import PoolView


class Pool:
    """A plain class: its lock orders the `permit()` calls of this process, which wait for a grant
    one at a time, reading, granting and sleeping, so only processes conflict with each other."""

    def __init__(
        self,
        ledger: Ledger,
        name: str,
        *,
        priority: Priority,
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
        self._limits = limits
        self._lease = lease
        self._poll = poll.total_seconds()
        self._turn = asyncio.Lock()

    @asynccontextmanager
    async def permit(self) -> AsyncGenerator[Permit, None]:
        """Wait for a grant and lease the ticket to the block. Only a block that ends normally
        releases it; otherwise it is delivered again once its lease ends."""
        async with self._turn:
            permit, _ = await self._granted()
        yield permit
        await self._ledger.release(permit)  # LeaseLost: re-leased or withdrawn since; nothing to do

    async def _granted(self) -> tuple[Permit, float]:
        """The permit, and the event loop's time just before the grant that made it."""
        while True:
            match await self._attempt(await self._ledger.view(self._name)):
                case (Permit() as permit, float() as since):
                    return permit, since
                case Conflict():
                    continue  # another grant came first: read again at once
                case NoTicket():
                    await asyncio.sleep(random.uniform(self._poll / 2, self._poll))

    async def _attempt(self, view: PoolView) -> tuple[Permit, float] | Conflict | NoTicket:
        if not all(limit.admits(view) for limit in self._limits):
            return NoTicket()
        while True:
            match self._priority.first(view):
                case NoTicket() as nothing:
                    return nothing
                case Waiting() as chosen:
                    since = asyncio.get_running_loop().time()
                    answer = await self._ledger.grant(
                        self._name,
                        chosen.ticket.id,
                        version=view.version,
                        lease=self._lease,
                        fairness=view.fairness.after(chosen),
                    )
                    match answer:
                        case Permit() as permit:
                            return permit, since
                        case Conflict() as conflict:
                            return conflict
                        case Missing():
                            view = view.without(chosen.ticket.id)  # withdrawn or held: the next one
