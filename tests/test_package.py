"""The package's public names are what it exports, and the extra stays out of the core."""

import importlib
import subprocess
import sys

import pytest

import wicket
import wicket.postgres
from wicket.postgres import metadata


def test_every_public_name_exists():
    assert all(hasattr(wicket, name) for name in wicket.__all__)


def test_the_names_a_caller_needs_are_public():
    names = {"Ticket", "Trait", "Waiting", "Permit", "Version", "PoolView", "Fairness"}
    names |= {"Grant", "Presence", "Lane"}
    names |= {
        "Factor",
        "FairShare",
        "Age",
        "TraitFactor",
        "Priority",
        "Limit",
        "Concurrency",
        "Rate",
    }
    names |= {"Backlog", "Ledger", "MemoryState", "MemoryBacklog", "MemoryLedger", "Pool"}
    names |= {
        "Attended",
        "Withdrawn",
        "AlreadyLeased",
        "Gone",
        "Conflict",
        "Missing",
        "Renewed",
        "Released",
    }
    names |= {"LeaseLost", "NoTicket", "LeaseLostError", "StoreUnavailable"}
    assert names <= set(wicket.__all__)


def test_the_postgres_tables_a_caller_migrates_are_the_four_wicket_ones():
    assert set(metadata.tables) == {
        "wicket_pools",
        "wicket_tickets",
        "wicket_grants",
        "wicket_lanes",
    }


def test_the_postgres_module_exports_its_store_and_tables():
    assert set(wicket.postgres.__all__) == {"PostgresBacklog", "PostgresLedger", "metadata"}


def test_the_core_imports_nothing_of_the_postgres_extra():
    script = (
        "import sys, wicket; print(sorted({'sqlalchemy', 'asyncpg', 'psycopg'} & set(sys.modules)))"
    )
    loaded = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    assert loaded.stdout.strip() == "[]"


def test_the_postgres_module_without_sqlalchemy_says_which_extra_to_install(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setitem(sys.modules, "sqlalchemy", None)
    monkeypatch.delitem(sys.modules, "wicket.postgres")
    with pytest.raises(ModuleNotFoundError, match=r"wicket\[postgres\]"):
        importlib.import_module("wicket.postgres")
