"""The in-memory store: tickets kept in this process, for many coroutines of one event loop. Its
methods never await, so each runs whole and needs no lock."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import msgspec

from wicket.fairness import Fairness
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
from wicket.tickets import Permit, Ticket, Version, Waiting
from wicket.view import PoolView


class _Lease(msgspec.Struct, frozen=True):
    permit: Permit
    end: datetime


class _Pool:
    """One pool's tickets, leases, grant history, fairness and version."""

    def __init__(self) -> None:
        self.version: Version = 0
        self.fairness = Fairness()
        self.tickets: dict[str, Waiting] = {}  # every ticket, leased or not, with its grants so far
        self.leases: dict[str, _Lease] = {}
        self.grant_times: list[datetime] = []

    def leased(self, ticket_id: str, now: datetime) -> bool:
        return ticket_id in self.leases and self.leases[ticket_id].end > now

    def holds(self, permit: Permit) -> bool:
        id = permit.waiting.ticket.id
        return id in self.leases and self.leases[id].permit.token == permit.token


class MemoryState:
    """The tickets of every pool, shared by one MemoryBacklog and one MemoryLedger. `clock` is the
    store's clock, which tests set; grants are kept for `retention`."""

    def __init__(
        self,
        retention: timedelta = timedelta(hours=1),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if retention <= timedelta(0):
            raise ValueError(f"retention must be positive, not {retention}")
        self.retention = retention
        self.clock = clock
        self._pools: dict[str, _Pool] = {}

    def _pool(self, name: str) -> _Pool:
        return self._pools.setdefault(name, _Pool())


class MemoryBacklog(msgspec.Struct, frozen=True):
    state: MemoryState

    async def submit(self, pool: str, ticket: Ticket) -> None:
        tickets = self.state._pool(pool).tickets
        if ticket.id not in tickets:
            copy = msgspec.json.decode(
                msgspec.json.encode(ticket), type=Ticket
            )  # as a durable store reads it back
            tickets[ticket.id] = Waiting(copy, self.state.clock(), uuid4())

    async def withdraw(self, pool: str, ticket_id: str) -> Withdrawn | AlreadyLeased | Gone:
        held = self.state._pool(pool)
        if ticket_id not in held.tickets:
            return Gone()
        if held.leased(ticket_id, self.state.clock()):
            return AlreadyLeased()
        del held.tickets[ticket_id]
        held.leases.pop(ticket_id, None)
        return Withdrawn()


class MemoryLedger(msgspec.Struct, frozen=True):
    state: MemoryState

    async def view(self, pool: str) -> PoolView:
        held, now = self.state._pool(pool), self.state.clock()
        held.grant_times = [at for at in held.grant_times if at > now - self.state.retention]
        return PoolView(
            now=now,
            retention=self.state.retention,
            waiting=tuple(w for id, w in held.tickets.items() if not held.leased(id, now)),
            leased=tuple(lease.permit for lease in held.leases.values() if lease.end > now),
            grant_times=tuple(held.grant_times),
            fairness=held.fairness,
            version=held.version,
        )

    async def grant(
        self, pool: str, waiting: Waiting, *, version: Version, lease: timedelta, fairness: Fairness
    ) -> Permit | Conflict | Missing:
        held, now = self.state._pool(pool), self.state.clock()
        if version != held.version:
            return Conflict()
        ticket_id = waiting.ticket.id
        if ticket_id not in held.tickets or held.leased(ticket_id, now):
            return Missing()
        current = held.tickets[ticket_id]
        if current.submission_id != waiting.submission_id:
            return Missing()
        held.version += 1
        permit = Permit(pool, current, now, held.version)
        held.tickets[ticket_id] = msgspec.structs.replace(current, grants=current.grants + 1)
        held.leases[ticket_id] = _Lease(permit, now + lease)
        held.grant_times.append(now)
        held.fairness = fairness
        return permit

    async def renew(self, permit: Permit, *, lease: timedelta) -> Renewed | LeaseLost:
        held, now, id = self.state._pool(permit.pool), self.state.clock(), permit.waiting.ticket.id
        if not (held.holds(permit) and held.leased(id, now)):
            return LeaseLost()
        held.leases[id] = _Lease(held.leases[id].permit, now + lease)
        held.version += 1
        return Renewed()

    async def release(self, permit: Permit) -> Released | LeaseLost:
        held, id = self.state._pool(permit.pool), permit.waiting.ticket.id
        if not held.holds(permit):
            return LeaseLost()
        del held.tickets[id]
        del held.leases[id]
        return Released()
