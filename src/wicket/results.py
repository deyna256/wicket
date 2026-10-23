"""What the ports and the decision answer: values to match on, and the two errors."""

import msgspec


class Withdrawn(msgspec.Struct, frozen=True):
    """The ticket was waiting and is gone."""


class AlreadyLeased(msgspec.Struct, frozen=True):
    """The ticket is leased; stopping its work is the caller's."""


class Gone(msgspec.Struct, frozen=True):
    """The pool has no ticket with that id."""


class Conflict(msgspec.Struct, frozen=True):
    """Another grant came first: the view's version is stale."""


class Missing(msgspec.Struct, frozen=True):
    """The version is current, but the submission cannot be granted: gone, replaced, leased,
    or held by another transaction."""


class Renewed(msgspec.Struct, frozen=True):
    """The lease runs on."""


class Released(msgspec.Struct, frozen=True):
    """The work is done and the ticket is gone."""


class LeaseLost(msgspec.Struct, frozen=True):
    """The lease ended, or the ticket was leased again or withdrawn."""


class NoTicket(msgspec.Struct, frozen=True):
    """Nothing can be granted now."""


class LeaseLostError(Exception):
    """A permit's block was cancelled because its lease was lost; the ticket comes back."""


class StoreUnavailable(Exception):
    """The store could not be reached; the adapter's next call tries again from scratch."""
