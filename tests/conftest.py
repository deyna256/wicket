"""The PostgreSQL test database, set up after collection and before the first test, so that starting
a container does not count against a test's timeout, and only if a collected test is marked
`postgres`: the server WICKET_POSTGRES_URL names, or a postgres:18-alpine container, or neither, and
the PostgreSQL tests are skipped. On that server the session makes a database of its own, with the
tables and a clock the tests move by hand, and drops it at the end."""

import os
import secrets
from contextlib import suppress
from typing import Any

import pytest
from sqlalchemy import URL, create_engine, make_url, text

from wicket.postgres import metadata

EPOCH = "2026-01-01 00:00:00+00"
CLOCK = """
create table wicket_test_clock (at timestamptz not null);
insert into wicket_test_clock values ('{epoch}');
create function public.now() returns timestamptz language sql stable
    as 'select at from public.wicket_test_clock';
create function public.clock_timestamp() returns timestamptz language sql stable
    as 'select at from public.wicket_test_clock';
"""

found: dict[str, Any] = {}  # what the session found: "server", "database", "container" or "skip"


def pytest_collection_finish(session: pytest.Session) -> None:
    if not any(item.get_closest_marker("postgres") for item in session.items):
        return
    named = os.environ.get("WICKET_POSTGRES_URL")
    if named is None:
        try:
            from testcontainers.community.postgres import PostgresContainer

            container = PostgresContainer("postgres:18-alpine", driver="psycopg")
            found["container"] = container
            container.start()
        except Exception as error:  # no Docker, or no image to be had
            with suppress(Exception):
                found.pop("container").stop()
            found["skip"] = f"no PostgreSQL: set WICKET_POSTGRES_URL or start Docker ({error})"
            return
        named = container.get_connection_url()
    server = make_url(named).set(drivername="postgresql+psycopg")
    database = f"wicket_test_{secrets.token_hex(4)}"
    admin = create_engine(server, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as connection:
            connection.execute(text(f"create database {database}"))
            found["server"], found["database"] = server, server.set(database=database)
            # pg_catalog listed last, so the test clock's now() and clock_timestamp() come first
            connection.execute(
                text(f"alter database {database} set search_path = public, pg_catalog")
            )
    except Exception as error:
        pytest.exit(
            f"WICKET_POSTGRES_URL names a server that cannot be used: {error}", returncode=1
        )
    finally:
        admin.dispose()
    setup = create_engine(server.set(database=database))
    try:
        with setup.begin() as connection:
            metadata.create_all(connection)
            connection.execute(text(CLOCK.format(epoch=EPOCH)))
    finally:
        setup.dispose()


def pytest_sessionfinish(session: pytest.Session) -> None:
    try:
        if "database" in found:
            admin = create_engine(found["server"], isolation_level="AUTOCOMMIT")
            try:
                with admin.connect() as connection:
                    name = found["database"].database
                    connection.execute(text(f"drop database if exists {name} with (force)"))
            finally:
                admin.dispose()
    finally:
        if "container" in found:
            found["container"].stop()


@pytest.fixture(scope="session")
def database() -> URL:
    """The test database (psycopg's URL; set another driver with `.set(drivername=...)`)."""
    if "skip" in found:
        pytest.skip(str(found["skip"]))
    if "database" not in found:
        pytest.fail("the test database was not set up: mark the test with @pytest.mark.postgres")
    return found["database"]
