"""A ticket's traits, the ticket in line, and the permit that leases it."""

from datetime import datetime
from uuid import UUID

import msgspec

type Version = int


class Trait(msgspec.Struct, frozen=True):
    """One named number in 0..1 describing a ticket; checked on construction and decoding."""

    name: str
    value: float

    def __post_init__(self) -> None:
        if not 0 <= self.value <= 1:  # also refuses NaN
            raise ValueError(f"trait {self.name!r} is {self.value}, not in 0..1")


class Ticket(msgspec.Struct, frozen=True):
    """A claim on a pool's capacity: the caller's row id, the tenant whose share it counts in, an
    immutable tuple of uniquely named traits, and the lane of the pool whose workers take it."""

    id: str
    tenant: str
    traits: tuple[Trait, ...] = ()
    lane: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.traits, tuple) or any(
            not isinstance(trait, Trait) for trait in self.traits
        ):
            raise ValueError(f"traits of ticket {self.id!r} must be a tuple of Trait values")
        if len({trait.name for trait in self.traits}) != len(self.traits):
            raise ValueError(f"trait names of ticket {self.id!r} must be unique")


class Waiting(msgspec.Struct, frozen=True):
    """One submission of a ticket, with its store-generated UUID, time and prior grant count.
    The UUID stays until removal; submitting the same ticket again after removal gets a new one."""

    ticket: Ticket
    submitted_at: datetime
    submission_id: UUID
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
