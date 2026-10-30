# 7. The PostgreSQL store is built on SQLAlchemy and ships as an extra

**Status:** Accepted

## Context

A service whose workers run in several processes needs one queue they all see, tickets that
survive a restart, and a ticket written in the same transaction as the caller's own row, so that
the two commit together or not at all. The caller owns that transaction; wicket must only be able
to write inside it, which needs it to speak the caller's database layer. For Python services that
is usually SQLAlchemy.

## Decision

- **SQLAlchemy's asyncio Core**, in the extra `wicket[postgres]` (`sqlalchemy[asyncio]>=2.1.6`, the
  first release with every asyncio cancellation fix up to
  [SQLAlchemy issue #13699](https://github.com/sqlalchemy/sqlalchemy/issues/13699)). The core keeps
  msgspec as its only dependency ([decision 6](0006-dependencies.md)).
- **`PostgresBacklog(session)`** works on the caller's `AsyncSession` and never commits.
  **`PostgresLedger(engine, retention)`** runs each call in a short transaction of its own on a
  connection from the engine. The driver is the caller's, through its URL: asyncpg or psycopg.
- **The tables are `MetaData` for the caller's migrations**, named `wicket_pools`, `wicket_tickets`,
  `wicket_grants` and `wicket_lanes`. wicket ships no migrations and creates no tables.
- **Traits are a JSONB array of name/value records**, preserving their tuple order.
- **Lanes:** `lane` on `wicket_tickets` and `wicket_grants`, and a `wicket_lanes` row per pool and
  lane, never deleted. Each pool stores `lane_changes`, a JSONB object mapping a lane to the version
  of its last grant or renewal. `attend` locks the lane row, inserting an ended row if needed, then
  takes a shared pool-row lock and checks that lane's version against its view before extending the
  time and assigning a new UUID. It reads the clock only after the lock wait. A grant or renewal
  updates the marker using the lane of the actual ticket, in the same transaction as its version.
  `leave` ends the presence only if its UUID still matches, so an old view cannot hide a later attendance
  ([decision 8](0008-lanes.md)).
- **The store's clock only**: a view reads `now()`, every other call `clock_timestamp()`, and times
  are read back as UTC.
- **Submission identity is a UUID column with `gen_random_uuid()` as its default.** A grant matches
  it while locking the chosen ticket with `SKIP LOCKED`; withdrawals need no pool-row lock
  ([decision 2](0002-pure-decision-over-a-store.md)).
- **Grant and renewal lock the pool before its ticket.** Renewal increments the existing pool
  version in `READ COMMITTED`, then locks the ticket if its token and lease still match. It checks
  expiry again against `clock_timestamp()` after any wait for that lock and updates the lease. It
  commits both changes together or rolls back if the lease is lost.
  A grant using a view that missed the renewal waits for it and returns `Conflict` if it commits.
  PostgreSQL [rechecks the update condition after a concurrent writer commits](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED).
- **Failed commit hooks:** if a hook fails or is cancelled and leaves an inactive SQLAlchemy
  transaction on the connection, `Ledger` rolls it back before closing, so the pool resets the
  driver's transaction.

## Alternatives

- **psycopg or asyncpg directly.** One layer fewer, but the caller's SQLAlchemy transaction could
  not take the ticket, and the caller would need a second pool of connections.
- **wicket's own migrations.** Updates would apply themselves, but every caller would run two
  migration histories, and Alembic would become a dependency.
- **The caller's transaction in each call** (as River's `InsertTx`). The port would then carry a
  driver's transaction type; binding the session in the adapter's constructor keeps the port free
  of it, as repositories bound to a session do.

## Consequences

- A caller adds `wicket.postgres.metadata` to its migrations' target metadata, and reads the
  changelog for schema changes.
- Adding `lane_changes` to an existing pool needs a `jsonb NOT NULL DEFAULT '{}'::jsonb` column.
  Stop every worker and direct ledger client of the pool before the migration and keep them stopped
  until all run the new code: older versions do not update the markers and cannot share the pool.
  Discard any views or versions saved before the migration; the new markers do not cover them.
- A caller without SQLAlchemy writes its own adapter of the two ports and runs the store contract
  on it.
- Every process serving a pool passes the same `retention`.
- The PostgreSQL tests need a database: a server URL or Docker; CI runs them on Ubuntu.
