"""A ticket, the ticket in line, and the permit that leases it."""

from collections.abc import Mapping
from datetime import datetime

import msgspec

type Version = int


class Ticket(msgspec.Struct, frozen=True):
    """A claim on a pool's capacity: the caller's row id, the tenant whose share it counts in, and
    the traits factors read. msgspec runs the check on decoding too, so a stored ticket is
    checked."""

    id: str
    tenant: str
    traits: Mapping[str, float] = {}

    def __post_init__(self) -> None:
        for name, value in self.traits.items():
            if not 0 <= value <= 1:  # also refuses NaN
                raise ValueError(f"trait {name!r} of ticket {self.id!r} is {value}, not in 0..1")


class Waiting(msgspec.Struct, frozen=True):
    """A ticket in line, with when it was submitted and how many times it was granted before."""

    ticket: Ticket
    submitted_at: datetime
    grants: int = 0


class Permit(msgspec.Struct, frozen=True):
    """A leased ticket. `token` is the pool's version after the grant: it only grows, so the caller
    can reject a late write that carries an older one."""

    pool: str
    waiting: Waiting
    granted_at: datetime
    token: Version

    @property
    def attempt(self) -> int:
        return self.waiting.grants + 1
