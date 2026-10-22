"""What only the PostgreSQL store can show: other transactions, isolation, time zones, cancellation,
a server that goes away, and the schema. Every test runs on asyncpg and on psycopg, on the test
database whose clock the tests move by hand (see conftest.py)."""

from sqlalchemy import URL, create_engine, inspect, text

from wicket.postgres import metadata


def test_metadata_holds_the_three_tables_and_they_create_cleanly(database: URL):
    names = {"wicket_pools", "wicket_tickets", "wicket_grants"}
    assert set(metadata.tables) == names
    engine = create_engine(database)
    with engine.begin() as connection:
        connection.execute(text("create schema fresh; set local search_path = fresh"))
        metadata.create_all(connection)
        assert set(inspect(connection).get_table_names(schema="fresh")) == names
        connection.execute(text("drop schema fresh cascade"))
    engine.dispose()
