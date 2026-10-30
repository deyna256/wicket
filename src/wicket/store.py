"""The two ports of a store: producers write to a Backlog, a pool works through a Ledger. Every
call raises StoreUnavailable when the store cannot be reached; the adapter's next call tries again
from scratch."""

from datetime import timedelta
from typing import Protocol

from wicket.fairness import Fairness
from wicket.results import (
    AlreadyLeased,
    Attended,
    Conflict,
    Gone,
    LeaseLost,
    Missing,
    Released,
    Renewed,
    Withdrawn,
)
from wicket.tickets import Permit, Ticket, Version, Waiting
from wicket.view import PoolView, Presence


class Backlog(Protocol):
    async def submit(self, pool: str, ticket: Ticket) -> None:
        """Put the ticket in line with a new UUIDv4 submission_id. An id that is waiting or
        leased changes nothing, including its submission_id; an id removed earlier starts anew."""
        ...

    async def withdraw(self, pool: str, ticket_id: str) -> Withdrawn | AlreadyLeased | Gone: ...


class Ledger(Protocol):
    async def view(self, pool: str) -> PoolView: ...

    async def grant(
        self, pool: str, waiting: Waiting, *, version: Version, lease: timedelta, fairness: Fairness
    ) -> Permit | Conflict | Missing:
        """Lease this submission if the pool is still at `version` and it is waiting. Match
        both ticket.id and submission_id atomically with the grant; a replacement is Missing.
        Write the grant's time and `fairness` together; a failed grant changes nothing.
        Never wait on a ticket another transaction holds."""
        ...

    async def renew(self, permit: Permit, *, lease: timedelta) -> Renewed | LeaseLost:
        """Extend a matching, unexpired lease and increment the pool version atomically with it.
        Serialize with grants so a view that missed the renewal cannot admit more work.
        Keep the permit token, fairness and grant history; a lost lease changes nothing."""
        ...

    async def release(self, permit: Permit) -> Released | LeaseLost: ...

    async def attend(
        self, pool: str, lane: str, *, version: Version, ttl: timedelta
    ) -> Attended | Conflict:
        """Pass a version from a view of this pool. If it is not in the future and the lane has
        not had a grant or renewal since it, make the lane present until at least now + ttl with a
        new token. Otherwise return Conflict without changing the presence. Another lane may have
        changed; a presence never changes the pool version."""
        ...

    async def leave(self, pool: str, presence: Presence) -> None:
        """Remove this presence only if its token still matches; never change the pool's version."""
        ...
