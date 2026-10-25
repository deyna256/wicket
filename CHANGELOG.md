# Changelog

Notable changes to this project. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[semantic versioning](https://semver.org/spec/v2.0.0.html). While the version is `0.x` the public API
may change in a minor release.

This file is built from fragments in `changelog.d/` when a release is made; see
[CONTRIBUTING.md](CONTRIBUTING.md#changelog). Do not edit it by hand.

<!-- towncrier release notes start -->

## [0.2.0] - 2026-10-25

### Changed

- Successful `Ledger.renew` calls now advance the pool version atomically with the lease, so grants from earlier views can return `Conflict`; existing permit tokens stay valid.
  Upgrade all workers and custom ledgers sharing a pool to enforce this guarantee; no database migration is required for this change. ([#7](https://github.com/deyna256/wicket/issues/7))
- Pass `traits=(Trait("tier", 0.5),)` to `Ticket` and use `TraitFactor("tier")` in `Priority`; dictionaries and duplicate trait names are rejected.
  Serialized traits are now arrays of name/value records; PostgreSQL still reads existing dictionary rows, but all processes sharing a pool must be upgraded before writing the new format. ([#8](https://github.com/deyna256/wicket/issues/8))
- `Waiting` now requires a store-generated UUIDv4 `submission_id`, and `Ledger.grant` takes the chosen `Waiting` instead of a ticket id.
  wicket does not apply database migrations: for existing PostgreSQL tables, stop producers and workers, apply the [submission ID migration](https://github.com/deyna256/wicket/blob/main/docs/decisions/0007-postgres-store.md#adding-submission-ids-to-an-existing-table) through the application's own migrations, upgrade the library and custom ledger implementations, then resume; new tables already include the column in their metadata. ([#9](https://github.com/deyna256/wicket/issues/9))

### Fixed

- Renewals are now scheduled from the grant's start, so a slow grant response no longer lets a valid lease expire before the first renewal. A grant whose response arrives after its lease could have ended raises `LeaseLostError` without entering the block. ([#6](https://github.com/deyna256/wicket/issues/6))
- Delayed PostgreSQL lease renewals can no longer let a stale snapshot admit work beyond the concurrency limit. ([#7](https://github.com/deyna256/wicket/issues/7))
- Keep ticket traits immutable so callers cannot bypass validation through input values, views or permits. ([#8](https://github.com/deyna256/wicket/issues/8))
- Reject stale grants after a ticket is removed and resubmitted with the same id, so the replacement cannot receive capacity or charge fairness using the old decision. ([#9](https://github.com/deyna256/wicket/issues/9))
- Priority now rejects weights whose sum cannot fit in a finite float, preventing overflow from turning different scores into FIFO ties. ([#15](https://github.com/deyna256/wicket/issues/15))


## [0.1.0] - 2026-10-22

### Added

- The core: tickets with traits, fairness between tenants by Start-time Fair Queuing, priority from weighted factors (`FairShare`, `Age`, `Trait`), the `Concurrency` and `Rate` limits, the `Backlog` and `Ledger` ports with an in-memory store, and `Pool`, whose `permit()` leases a ticket to an `async with` block and releases it only when the block ends normally. ([#1](https://github.com/deyna256/wicket/issues/1))
- A PostgreSQL store in the extra `wicket[postgres]`: `PostgresBacklog` writes tickets in your SQLAlchemy session, in the same transaction as your own rows, and `PostgresLedger` keeps one queue, its limits and its fairness for every process; add `wicket.postgres.metadata` to your migrations. ([#3](https://github.com/deyna256/wicket/issues/3))
