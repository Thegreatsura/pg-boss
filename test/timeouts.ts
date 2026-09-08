// The per-backend test budget, in one place: vitest.config.ts reads it for the global default, and
// so does every suite that raises the default for a block of its own. Both sides have to agree,
// because a per-block value replaces the global in either direction and a block that names its own
// number caps the backend that needs the most room.

export const isDistributedBackend = process.env.DB_TYPE === 'cockroachdb' || process.env.DB_TYPE === 'yugabytedb'

// PostgreSQL finishes a test's schema setup in well under a second, so 10s is generous and keeps a
// hung test cheap to find.
export const postgresTimeout = 10000

// CockroachDB and YugabyteDB pay heavy online-DDL/schema-rebuild costs per test, so they get a
// budget sized for a contended runner rather than a good one: CI brings up a three-node CockroachDB
// cluster in docker alongside the suite on a shared two-core runner, where the same tests that each
// take ~9s on one run take ~30s on the next. The budget has to cover the slow run, or the
// compatibility jobs report timeouts in place of the failures they exist to catch.
export const distributedTimeout = 120000
