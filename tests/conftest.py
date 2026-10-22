"""The PostgreSQL test database, set up before the first test so that starting a container does not
count against its timeout: the server WICKET_POSTGRES_URL names, or a postgres:18-alpine container,
or neither, and the PostgreSQL tests are skipped. On that server the session makes a database of
its own, with the tables and a clock the tests move by hand, and drops it at the end."""

import os
import secrets
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


def pytest_sessionstart(session: pytest.Session) -> None:
    named = os.environ.get("WICKET_POSTGRES_URL")
    if named is None:
        try:
            from testcontainers.community.postgres import PostgresContainer

            container = PostgresContainer("postgres:18-alpine", driver="psycopg").start()
        except Exception as error:  # no Docker, or no image to be had
            found["skip"] = f"no PostgreSQL: set WICKET_POSTGRES_URL or start Docker ({error})"
            return
        found["container"] = container
        named = container.get_connection_url()
    server = make_url(named).set(drivername="postgresql+psycopg")
    database = f"wicket_test_{secrets.token_hex(4)}"
    try:
        admin = create_engine(server, isolation_level="AUTOCOMMIT")
        with admin.connect() as connection:
            connection.execute(text(f"create database {database}"))
            # pg_catalog listed last, so the test clock's now() and clock_timestamp() come first
            connection.execute(
                text(f"alter database {database} set search_path = public, pg_catalog")
            )
        admin.dispose()
    except Exception as error:
        pytest.exit(
            f"WICKET_POSTGRES_URL names a server that cannot be used: {error}", returncode=1
        )
    found["server"], found["database"] = server, server.set(database=database)
    setup = create_engine(server.set(database=database))
    with setup.begin() as connection:
        metadata.create_all(connection)
        connection.execute(text(CLOCK.format(epoch=EPOCH)))
    setup.dispose()


def pytest_sessionfinish(session: pytest.Session) -> None:
    if "database" in found:
        admin = create_engine(found["server"], isolation_level="AUTOCOMMIT")
        with admin.connect() as connection:
            name = found["database"].database
            connection.execute(text(f"drop database if exists {name} with (force)"))
        admin.dispose()
    if "container" in found:
        found["container"].stop()


@pytest.fixture(scope="session")
def database() -> URL:
    """The test database (psycopg's URL; set another driver with `.set(drivername=...)`)."""
    if "skip" in found:
        pytest.skip(str(found["skip"]))
    return found["database"]
