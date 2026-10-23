"""Traits and tickets stay valid and immutable, built or decoded; permits count attempts."""

import math
from datetime import UTC, datetime
from typing import Any

import msgspec
import pytest

from wicket.tickets import Permit, Ticket, Trait, Waiting

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize("value", [0.0, 0.5, 1.0])
def test_a_trait_keeps_its_name_and_value(value: float):
    trait = Trait("tier", value)
    assert (trait.name, trait.value) == ("tier", value)


@pytest.mark.parametrize("value", [-0.1, 1.5, math.nan, math.inf, -math.inf])
def test_a_trait_refuses_a_value_outside_zero_to_one(value: float):
    with pytest.raises(ValueError, match=r"trait 'tier'.*not in 0\.\.1"):
        Trait("tier", value)


@pytest.mark.parametrize("field,value", [("name", "urgent"), ("value", 99.0)])
def test_a_trait_cannot_change(field: str, value: str | float):
    trait = Trait("tier", 0.5)
    with pytest.raises(AttributeError):
        setattr(trait, field, value)
    assert trait == Trait("tier", 0.5)


def test_a_ticket_has_no_traits_unless_given():
    assert Ticket("1", "ann").traits == ()


@pytest.mark.parametrize("traits", [{}, {"tier": 0.5}, [Trait("tier", 0.5)], ({"tier": 0.5},)])
def test_a_ticket_refuses_mutable_traits_and_values_that_are_not_traits(traits: Any):
    with pytest.raises(ValueError, match="must be a tuple of Trait values"):
        Ticket("1", "ann", traits)


@pytest.mark.parametrize("value", [0.5, 1.0])
def test_a_ticket_refuses_duplicate_trait_names(value: float):
    with pytest.raises(ValueError, match="trait names of ticket '1' must be unique"):
        Ticket("1", "ann", (Trait("tier", 0.5), Trait("tier", value)))


@pytest.mark.parametrize("codec", [msgspec.json, msgspec.msgpack])
@pytest.mark.parametrize("traits", [(), (Trait("urgent", 1.0), Trait("tier", 0.5))])
def test_tickets_round_trip_without_hooks(codec: Any, traits: tuple[Trait, ...]):
    ticket = Ticket("1", "ann", traits)
    decoded: Ticket = codec.decode(codec.encode(ticket), type=Ticket)
    assert decoded == ticket
    with pytest.raises(AttributeError):
        decoded.traits = (Trait("other", 1.0),)  # ty: ignore[invalid-assignment]
    if traits:
        with pytest.raises(AttributeError):
            decoded.traits[0].value = 99.0  # ty: ignore[invalid-assignment]


def test_conversion_does_not_keep_mutable_input():
    ticket = Ticket("1", "ann", (Trait("tier", 0.5),))
    data = msgspec.to_builtins(ticket)
    decoded = msgspec.convert(data, Ticket)
    data["traits"][0]["value"] = 99.0
    data["traits"][0]["name"] = "other"
    assert decoded == ticket


@pytest.mark.parametrize(
    "traits",
    [
        [{"name": "tier", "value": 5}],
        [{"name": "tier", "value": 0.5}, {"name": "tier", "value": 1.0}],
    ],
)
def test_decoding_checks_trait_values_and_unique_names(traits: list[dict[str, str | float]]):
    data = msgspec.json.encode({"id": "1", "tenant": "ann", "traits": traits})
    with pytest.raises(msgspec.ValidationError, match=r"not in 0\.\.1|must be unique"):
        msgspec.json.decode(data, type=Ticket)


def test_a_first_grant_is_attempt_one_and_a_redelivery_counts_on():
    first = Permit("p", Waiting(Ticket("1", "ann"), EPOCH), EPOCH, 1)
    again = Permit("p", Waiting(Ticket("1", "ann"), EPOCH, grants=2), EPOCH, 7)
    assert (first.attempt, again.attempt) == (1, 3)
