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

- `postgres-fixture.ts` owns the neutral fixture lifecycle, real bindings/storage inspection, SQL gates and execution timestamps, response drainage, diagnostics, deadlines, and cleanup. Callers supply the URL and schema-initialization callback.
- `topology.ts` supplies shared scenarios for future publication, renewed lease hints, scheduled/immediate failures, successful self-rescheduling, transient repair, response loss, real-storage reconstruction, and simultaneous alarm-driven execution across definitions. SQL timestamps prove no early execution, and fan-out keeps all business operations gated until every task starts. Existing early acceptance, exact routing, ownership/redelivery, blocked-query deadline precision, validation/cleanup, and insert-failure coverage remain intact. IDs `9007199254740991`, `9007199254740992`, `9007199254740993`, and `9223372036854775807` use exact SQL strings: the safe boundary processes, while unsupported IDs remain committed and unclaimed with exact error receipts, no side effects, and closed clients.
- `publisher-contracts.ts` supplies immediate/future adapter contracts, registered once in each language-owned suite and never in mixed suites. A real Hyperdrive publisher and gated DO receiver prove one insert, immediate ID/name-only dispatch, shutdown before full success/error response consumption, exact receipts, and explicit redispatch without republishing. Direct publishing-backend contracts remain separate and have no dispatch bindings.
- Only `cloudflare.integration.test.ts` imports both implementations' setup adapters and registers the two mixed suites. The three neutral support exports import neither library and do not build or register suites on import.

Real automatic alarms execute future tasks and follow retry/lease hints without another external dispatch or manual alarm call. Reconstruction replaces the delegate while retaining real storage, including pending and in-flight records; it is not an induced platform eviction. Long-watchdog expiry, stale responses, failed persistence, and 300-ID unrestricted fan-out remain deterministic unit-test scenarios rather than sixty-second waits.

Suites stay sequential because the local Hyperdrive override is process-wide. Responses must be fully consumed within two seconds; database polls are bounded to three seconds, startup to eight seconds, and shutdown to five seconds. Cleanup clears fixture-owned retained schedules, releases locks, drains responses and request clients, closes workerd, drops only the fixture-owned schema, closes administrative connections, and restores the previous Hyperdrive environment value even on failure. Unexpected runtime logs and cleanup errors fail tests.

The showcased producers use separate typed `createPostgresPublisher` / `PostgresPublisher` delegates after business validation; they own HTTP mapping, not Bellows SQL, encoding, connections, or dispatch orchestration. `publishFuture(env, payload, availableFromMs)` and `publish_future(&env, payload, available_from)` publish availability to PostgreSQL but dispatch immediately, carrying only the ID and name. All definitions share one `global` dispatcher and one `PROCESSOR` binding. See the [shared protocol](../../bellows-ts/test/integration/cloudflare/README.md#protocol-and-limits) for routes and named dispatch.

The adapters configure per call, publish once, validate the exact ID, await shutdown, and consume the complete dispatch response. String receipts confirm acceptance, not completion. Close/dispatch errors retain a receipt for trusted redispatch with the original definition name; `task-id` errors retain unsupported IDs that need another recovery action. Receipt absence on database failure does not prove rollback. Both implementations use the same small protocol and private processor delegates.

The dispatcher launches external requests in memory, then checks the warming alarm without task writes or schedule scans. It only sets the alarm when absent or later than `now + 30 seconds`. A valid `retryAt` starts durable tracking until a fully consumed, successful matching-ID `done`. Its shared alarm selects the earliest pending/watchdog deadline or independent 30-second heartbeat, launching every due distinct ID without a Bellows concurrency limit, subject to platform limits. Infrastructure uncertainty uses one-to-thirty-second exponential backoff, in memory for unsaved tasks and durably for saved schedules. A 60-second watchdog supersedes hung/interrupted scheduled attempts without guaranteeing business cancellation. PostgreSQL remains authoritative. See [durable scheduling guarantees](../../README.md#durable-scheduling-and-limits).

Local Hyperdrive connects directly to PostgreSQL, not hosted pooling or caching. Durability starts with a persisted scheduling hint, not DO acceptance. Earlier loss requires explicit redispatch or application recovery; there is no PostgreSQL discovery, Cron, outbox, automatic republishing, added callback delivery, or guaranteed cleanup after abrupt termination. Alarms and attempts are at-least-once, not exactly-once side effects. Preserve TypeScript's `nodejs_compat`; never add it to Rust configurations. Contract Workers and fixture-only control routes must not be deployed. Follow the [opt-in hosted verification cautions](../../bellows-ts/test/integration/cloudflare/README.md#opt-in-hosted-verification) before adapting the application projects.
