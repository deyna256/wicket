"""A ticket refuses traits outside 0..1, built or decoded; a permit counts its attempts."""

import math
from datetime import UTC, datetime

import msgspec
import pytest

from wicket.tickets import Permit, Ticket, Waiting

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


def test_a_ticket_keeps_traits_in_range():
    assert Ticket("1", "ann", {"tier": 0.0, "urgent": 1.0}).traits == {"tier": 0.0, "urgent": 1.0}


def test_a_ticket_has_no_traits_unless_given():
    assert Ticket("1", "ann").traits == {}


@pytest.mark.parametrize("value", [-0.1, 1.5, math.nan])
def test_a_ticket_refuses_a_trait_outside_zero_to_one(value: float):
    with pytest.raises(ValueError, match="trait 'tier' of ticket '1'"):
        Ticket("1", "ann", {"tier": value})


def test_a_decoded_ticket_refuses_a_trait_outside_zero_to_one():
    with pytest.raises(msgspec.ValidationError, match=r"not in 0\.\.1"):
        msgspec.json.decode(b'{"id": "1", "tenant": "ann", "traits": {"tier": 5}}', type=Ticket)


def test_a_first_grant_is_attempt_one_and_a_redelivery_counts_on():
    first = Permit("p", Waiting(Ticket("1", "ann"), EPOCH), EPOCH, 1)
    again = Permit("p", Waiting(Ticket("1", "ann"), EPOCH, grants=2), EPOCH, 7)
    assert (first.attempt, again.attempt) == (1, 3)
