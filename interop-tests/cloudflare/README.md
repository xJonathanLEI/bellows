# Cloudflare interoperability tests

This private Node/Vitest/Wrangler package owns **ten scenarios**: five for a TypeScript producer/Durable Object -> Rust processor, and five for a Rust producer/Durable Object -> TypeScript processor. It runs real workerd instances with the language-owned Wrangler configurations and PostgreSQL, not mock Workers.

The [TypeScript harness](../../bellows-ts/test/integration/cloudflare/README.md) owns five TypeScript -> TypeScript cases and four direct publishing contracts. The [Rust harness](../../bellows/tests/integration/cloudflare/README.md) owns five Rust -> Rust cases plus all 19 Rust workerd contracts, including five direct publishing contracts. Root `pnpm test` runs the three packages serially and covers all **43 Cloudflare cases exactly once**. The existing native interop tests and their intentional TypeScript build in `interop-tests/build.rs` are separate and unchanged.

## Run locally

Requires Node 22+ (prefer Node 24), repository-pinned pnpm, stable Rust, and PostgreSQL 17 with schema creation/deletion privileges. No Cloudflare account is needed. From the repository root:

```bash
export WRANGLER_SEND_METRICS=false
pnpm install --frozen-lockfile
rustup target add wasm32-unknown-unknown
cargo install worker-build --version 0.8.5 --locked
pnpm --dir interop-tests test:cloudflare
```

The package's `test` script runs the same ten cases. Missing database or build prerequisites fail tests rather than skipping them. No library distribution prebuild is required; Rust-owned preparation builds the needed current-source Workers and native schema initializer automatically, with a separate 240-second cold preparation budget.

Set `BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL` for both directions; otherwise they default to `postgres://postgres:postgres@localhost:5432/postgres`. See the [Rust setup](../../bellows/tests/integration/cloudflare/README.md#build-and-test) for a disposable PostgreSQL 17 container. When the neutral override is unset, the TypeScript-producer direction also honors `BELLOWS_TS_TEST_POSTGRES_URL`; the Rust-producer direction does not. Set the neutral override when using a non-default database for the entire mixed suite.

The producer's adapter initializes each isolated schema with its production API: TypeScript's `initializePostgresSchema` or Rust's `initialize_postgres_schema` through the native Cargo example. Every Worker and administrative connection in a fixture receives the same URL and schema. The native Rust tests use the local default database, not these overrides, and require temporary-database privileges. The full TypeScript suite also needs temporary-database privileges, using its legacy override or the local default.

## Shared support

- `postgres-fixture.ts` owns the neutral fixture lifecycle, real bindings, SQL gates, response drainage, diagnostics, deadlines, and cleanup. Callers supply the URL and schema-initialization callback.
- `topology.ts` supplies the five shared scenarios: early acceptance, duplicates/concurrency, ownership/redelivery, failure/retry, and validation/cleanup. It asserts task name `cloudflare_greeting` and JSON payload `{ name }` directly.
- Only `cloudflare.integration.test.ts` imports both implementations' setup adapters and registers the two mixed suites. The two neutral source exports import neither library and do not build or register suites on import.

Suites stay sequential because the local Hyperdrive override is process-wide. Responses must be fully consumed within two seconds; database polls are bounded to three seconds, startup to eight seconds, and shutdown to five seconds. Cleanup releases locks, drains responses and request clients, closes workerd, drops only the fixture-owned schema, closes administrative connections, and restores the previous Hyperdrive environment value even on failure. Unexpected runtime logs and cleanup errors fail tests.

Local Hyperdrive connects directly to PostgreSQL, not hosted pooling or caching. In-memory dispatch state does not provide durable recovery, and side effects plus completion are not exactly-once. Preserve TypeScript's `nodejs_compat`; never add it to Rust configurations. Both languages' contract Workers are test-only, not deployable. The showcased producers retain application-owned SQL; direct publishing-backend contracts do not change the topology protocol. Follow the [opt-in hosted verification cautions](../../bellows-ts/test/integration/cloudflare/README.md#opt-in-hosted-verification) for any manual deployment of the application projects.
