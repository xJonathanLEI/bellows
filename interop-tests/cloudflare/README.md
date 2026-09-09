# Cloudflare interoperability tests

This private Node/Vitest/Wrangler package owns **20 scenarios**: ten for a TypeScript producer/Durable Object -> Rust processor, and ten for a Rust producer/Durable Object -> TypeScript processor. It runs real workerd instances with the language-owned Wrangler configurations and PostgreSQL, not mock Workers.

The [TypeScript harness](../../bellows-ts/test/integration/cloudflare/README.md) owns **16 cases**: ten topology, four direct publishing-backend, and two publisher-adapter contracts. The [Rust harness](../../bellows/tests/integration/cloudflare/README.md) owns **31 cases**: ten topology and 21 Rust workerd contracts, including five direct publishing-backend and two publisher-adapter contracts. Root `pnpm test` runs the three packages serially and covers all **67 Cloudflare cases exactly once**. The Rust contract-only command selects a subset, not extra registrations. The existing native interop tests and their intentional TypeScript build in `interop-tests/build.rs` are separate and unchanged.

## Run locally

Requires Node 22+ (prefer Node 24), repository-pinned pnpm, stable Rust, and PostgreSQL 17 with schema creation/deletion privileges. No Cloudflare account is needed. From the repository root:

```bash
export WRANGLER_SEND_METRICS=false
pnpm install --frozen-lockfile
rustup target add wasm32-unknown-unknown
cargo install worker-build --version 0.8.5 --locked
pnpm --dir interop-tests test:cloudflare
```

The package's `test` script runs the same 20 cases. Missing database or build prerequisites fail tests rather than skipping them. No library distribution prebuild is required; Rust-owned preparation builds the needed current-source Workers and native schema initializer automatically, with a separate 240-second cold preparation budget.

Set `BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL` for both directions; otherwise they default to `postgres://postgres:postgres@localhost:5432/postgres`. See the [Rust setup](../../bellows/tests/integration/cloudflare/README.md#build-and-test) for a disposable PostgreSQL 17 container. When the neutral override is unset, the TypeScript-producer direction also honors `BELLOWS_TS_TEST_POSTGRES_URL`; the Rust-producer direction does not. Set the neutral override when using a non-default database for the entire mixed suite.

The producer language's fixture adapter initializes each isolated schema administratively with its production API: TypeScript's `initializePostgresSchema` or Rust's `initialize_postgres_schema` through the native Cargo example. Neither request-scoped publisher initializes schemas. Every Worker and administrative connection in a fixture receives the same URL and schema. The native Rust tests use the local default database, not these overrides, and require temporary-database privileges. The full TypeScript suite also needs temporary-database privileges, using its legacy override or the local default.

## Shared support

- `postgres-fixture.ts` owns the neutral fixture lifecycle, real bindings, SQL gates, response drainage, diagnostics, deadlines, and cleanup. Callers supply the URL and schema-initialization callback.
- `topology.ts` supplies ten shared scenarios: early acceptance, duplicates/concurrency, ownership/redelivery, failure/retry, validation/cleanup, a SQL constraint failure without retry, and four exact-ID boundaries. It asserts task name `cloudflare_greeting` and JSON payload `{ name }` directly. IDs `9007199254740991`, `9007199254740992`, `9007199254740993`, and `9223372036854775807` use exact SQL strings: the safe boundary processes, while unsupported IDs remain committed and unclaimed with exact error receipts, no side effects, and closed clients.
- `publisher-contracts.ts` supplies two shared adapter contracts, registered once in each language-owned suite and never in mixed suites. A real Hyperdrive publisher and gated DO receiver prove one insert, shutdown before full success/error response consumption, sanitized exact receipts, and explicit redispatch without republishing. Existing direct publishing-backend contracts remain separate and have no dispatch bindings.
- Only `cloudflare.integration.test.ts` imports both implementations' setup adapters and registers the two mixed suites. The three neutral support exports import neither library and do not build or register suites on import.

Suites stay sequential because the local Hyperdrive override is process-wide. Responses must be fully consumed within two seconds; database polls are bounded to three seconds, startup to eight seconds, and shutdown to five seconds. Cleanup releases locks, drains responses and request clients, closes workerd, drops only the fixture-owned schema, closes administrative connections, and restores the previous Hyperdrive environment value even on failure. Unexpected runtime logs and cleanup errors fail tests.

The showcased producers use typed `createPostgresPublisher` / `PostgresPublisher` calls after business validation; they own HTTP mapping, not Bellows SQL, encoding, connections, or dispatch orchestration. The adapters configure per call, publish once, validate the exact ID, await shutdown, and consume the complete dispatch response. String receipts confirm acceptance, not completion. Close/dispatch errors retain a receipt for trusted redispatch; `task-id` errors retain unsupported IDs that need another recovery action. Receipt absence on database failure does not prove rollback. Both implementations use the same small protocol and private processor delegates.

Local Hyperdrive connects directly to PostgreSQL, not hosted pooling or caching. Publication and dispatch are not atomic; there is no automatic republishing, durable recovery, or future-request scheduling. The retained dispatcher's in-memory state and heartbeat do not provide daemon-equivalent discovery or eviction recovery, and side effects plus completion are not exactly-once. Preserve TypeScript's `nodejs_compat`; never add it to Rust configurations. Both languages' contract Workers are test-only, not deployable. Follow the [opt-in hosted verification cautions](../../bellows-ts/test/integration/cloudflare/README.md#opt-in-hosted-verification) for any manual deployment of the application projects.
