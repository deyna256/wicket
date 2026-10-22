"""The PostgreSQL test database (see conftest.py) as the store contract and test_postgres.py use it:
engines that are always disposed, and the clock the tests move by hand."""

import contextlib
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import QueuePool

from wicket.postgres import metadata


@contextlib.asynccontextmanager
async def opened(url: URL, **options: Any) -> AsyncGenerator[AsyncEngine]:
    """An engine on `url`, disposed at the end."""
    engine = create_async_engine(url, **options)
    try:
        yield engine
    finally:
        await engine.dispose()


@contextlib.asynccontextmanager
async def emptied(url: URL, at: datetime, **options: Any) -> AsyncGenerator[AsyncEngine]:
    """An engine on the emptied test database, its clock at `at`; every connection given back."""
    async with opened(url, **options) as engine:
        async with engine.begin() as connection:
            for table in metadata.sorted_tables:
                await connection.execute(table.delete())
            await connection.execute(text("update wicket_test_clock set at = :at"), {"at": at})
        yield engine
        assert given_back(engine)


def given_back(engine: AsyncEngine) -> bool:
    pool = engine.pool
    return isinstance(pool, QueuePool) and pool.checkedout() == 0


async def elapse(engine: AsyncEngine, time: timedelta) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text("update wicket_test_clock set at = at + make_interval(secs => :s)"),
            {"s": time.total_seconds()},
        )
