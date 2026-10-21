"""The two ports of a store: producers write to a Backlog, a pool works through a Ledger. Every
call raises StoreUnavailable when the store cannot be reached; the adapter's next call tries again
from scratch."""

from datetime import timedelta
from typing import Protocol

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
from wicket.tickets import Permit, Ticket, Version
from wicket.view import PoolView


class Backlog(Protocol):
    async def submit(self, pool: str, ticket: Ticket) -> None:
        """Put the ticket in line; an id that is waiting or leased changes nothing."""
        ...

    async def withdraw(self, pool: str, ticket_id: str) -> Withdrawn | AlreadyLeased | Gone: ...


class Ledger(Protocol):
    async def view(self, pool: str) -> PoolView: ...

    async def grant(
        self, pool: str, ticket_id: str, *, version: Version, lease: timedelta, fairness: Fairness
    ) -> Permit | Conflict | Missing:
        """Lease the ticket if the pool is still at `version` and the ticket is waiting, writing
        the grant's time and `fairness` with it; never wait on a ticket another transaction
        holds."""
        ...

    async def renew(self, permit: Permit, *, lease: timedelta) -> Renewed | LeaseLost: ...

    async def release(self, permit: Permit) -> Released | LeaseLost: ...
