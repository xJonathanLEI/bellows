# Cloudflare interoperability tests

This private Node/Vitest/Wrangler package runs the shared topology scenarios for a TypeScript producer/Durable Object -> Rust processor and a Rust producer/Durable Object -> TypeScript processor. It uses real workerd instances with the language-owned Wrangler configurations and PostgreSQL, not mock Workers.

The [TypeScript harness](../../bellows-ts/test/integration/cloudflare/README.md) owns TypeScript topology and direct publishing/publisher contracts. The [Rust harness](../../bellows/tests/integration/cloudflare/README.md) owns Rust topology and workerd contracts. Root `pnpm test` runs the three packages serially, registering each scenario once per topology and direct contracts only in their language-owned suite. The Rust contract-only command selects a subset, not extra registrations. Native interop tests and their TypeScript build in `interop-tests/build.rs` remain separate.

## Run locally

Requires Node 22+ (prefer Node 24), repository-pinned pnpm, stable Rust, and PostgreSQL 17 with schema creation/deletion privileges. No Cloudflare account is needed. From the repository root:

```bash
export WRANGLER_SEND_METRICS=false
pnpm install --frozen-lockfile
rustup target add wasm32-unknown-unknown
cargo install worker-build --version 0.8.5 --locked
pnpm --dir interop-tests test:cloudflare
```

The package's `test` script runs the same mixed-language suites. Missing database or build prerequisites fail tests rather than skipping them. No library distribution prebuild is required; Rust-owned preparation builds the needed current-source Workers and native schema initializer automatically, with a separate 240-second cold preparation budget.

Set `BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL` for both directions; otherwise they default to `postgres://postgres:postgres@localhost:5432/postgres`. See the [Rust setup](../../bellows/tests/integration/cloudflare/README.md#build-and-test) for a disposable PostgreSQL 17 container. When the neutral override is unset, the TypeScript-producer direction also honors `BELLOWS_TS_TEST_POSTGRES_URL`; the Rust-producer direction does not. Set the neutral override when using a non-default database for the entire mixed suite.

The producer language's fixture adapter initializes each isolated schema administratively with its production API: TypeScript's `initializePostgresSchema` or Rust's `initialize_postgres_schema` through the native Cargo example. Neither request-scoped publisher initializes schemas. Every Worker and administrative connection in a fixture receives the same URL and schema. The native Rust tests use the local default database, not these overrides, and require temporary-database privileges. The full TypeScript suite also needs temporary-database privileges, using its legacy override or the local default.

## Shared support

- `postgres-fixture.ts` owns the neutral fixture lifecycle, real bindings/storage inspection, SQL gates and execution timestamps, response and scheduled-event drainage, diagnostics, deadlines, and cleanup. Callers supply the URL and schema-initialization callback. Scheduled tests use `WorkerHandle.scheduled` and check its runtime outcome.
- `topology.ts` and `singleton-topology.ts` cover published recovery and singleton startup/recurrence across all four topologies. SQL timestamps check execution timing; gates verify mixed-task fan-out.
- `publisher-contracts.ts` supplies immediate/future adapter and sweeper streaming contracts, registered once per language-owned suite. Gated responses verify publication/dispatch boundaries, pagination without acknowledgement barriers, cleanup, and partial failures. Direct publishing-backend contracts remain separate and have no dispatch bindings.
- Only `cloudflare.integration.test.ts` imports both implementations' setup adapters and registers the two mixed suites. Neutral support modules import neither library and do not build or register suites on import.

Real automatic alarms execute future tasks and follow retry/lease hints without another external dispatch or manual alarm call. Reconstruction replaces the delegate while retaining real storage, including pending and in-flight records; it is not an induced platform eviction. Long-watchdog expiry, stale responses, failed persistence, and 300-ID unrestricted fan-out remain deterministic unit-test scenarios rather than sixty-second waits.

`runScheduled()` invokes scheduled events immediately. Singleton recovery tests retain bootstrap suppression while clearing schedules. Assert stable row IDs, not sequences: `INSERT ... ON CONFLICT` can consume sequence values.

Suites stay sequential because the local Hyperdrive override is process-wide. Responses must be fully consumed within two seconds; database polls are bounded to three seconds, startup to eight seconds, and shutdown to five seconds. Cleanup clears fixture-owned retained schedules, releases locks, drains responses, scheduled events, and request clients, closes workerd, drops only the fixture-owned schema, closes administrative connections, and restores the previous Hyperdrive environment value even on failure. Unexpected runtime logs and cleanup errors fail tests. Use the documented test server's `max_connections=600` for simultaneous gated sweep claims/business operations.

## Recovery contract

Both producers register minute Cron. See [TypeScript setup](../../bellows-ts/README.md#sweeper), [Rust's rejecting scheduled wrapper](../../bellows/tests/integration/cloudflare/README.md#scheduled-sweeper), and the [shared recovery semantics](../../README.md#minute-cron-postgresql-recovery).

See the shared [bulk protocol](../../README.md#logical-identity-and-bulk-protocol), [scheduling guarantees](../../README.md#durable-scheduling-and-limits), and [fixture routes](../../bellows-ts/test/integration/cloudflare/README.md#protocol-and-limits).

Local Hyperdrive connects directly to PostgreSQL, not hosted pooling or caching. Tests inject real scheduled events without waiting a minute; they do not verify hosted Cron delivery, and minute cadence is not a strict 60-second recovery SLA. Preserve TypeScript's `nodejs_compat`; never add it to Rust configurations. Contract Workers and fixture-only control routes must not be deployed. Follow the [opt-in hosted verification cautions](../../bellows-ts/test/integration/cloudflare/README.md#opt-in-hosted-verification) before adapting the application projects.
