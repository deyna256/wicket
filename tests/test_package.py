"""The package's public names are what it exports."""

import wicket


def test_every_public_name_exists():
    assert all(hasattr(wicket, name) for name in wicket.__all__)


def test_the_names_a_caller_needs_are_public():
    names = {"Ticket", "Waiting", "Permit", "Version", "PoolView", "Fairness"}
    names |= {"Factor", "FairShare", "Age", "Trait", "Priority", "Limit", "Concurrency", "Rate"}
    names |= {"Backlog", "Ledger", "MemoryState", "MemoryBacklog", "MemoryLedger", "Pool"}
    names |= {"Withdrawn", "AlreadyLeased", "Gone", "Conflict", "Missing", "Renewed", "Released"}
    names |= {"LeaseLost", "NoTicket", "LeaseLostError", "StoreUnavailable"}
    assert names <= set(wicket.__all__)
