# Rust Cloudflare Workers with PostgreSQL

These Rust/Wasm projects implement **producer Worker -> `global` Durable Object dispatcher -> service-bound processor Worker -> PostgreSQL**. Either side can be replaced by its TypeScript counterpart.

This directory owns a private Node/Vitest/Wrangler harness for **34 cases**: thirteen Rust -> Rust topology scenarios and all 21 Rust workerd contracts. It runs actual Rust/Wasm Workers without importing or building the TypeScript library. The [TypeScript suite](../../../../bellows-ts/test/integration/cloudflare/README.md) owns 19 cases (thirteen topology, four direct publishing, two publisher-adapter); the [mixed suite](../../../../interop-tests/cloudflare/README.md) owns 26 scenarios across both mixed directions.

The topology scenarios cover early acceptance, heterogeneous routing and concurrency, ownership/redelivery, failure/retry, validation/cleanup, a real insert constraint failure without retry, and four exact-ID boundaries (`9007199254740991`, `9007199254740992`, `9007199254740993`, `9223372036854775807`). The safe boundary processes normally; unsupported IDs retain exact unclaimed rows with no side effect, client leak, or processor rejection log.

Contracts cover runtime ownership, namespace and retained service-response streaming, heartbeat/alarm behavior, and PostgreSQL cancellation/shutdown. Five direct publishing contracts cover immediate/future callback-bearing and unit tasks, SQL errors, gated inserts, and cancelled publication/close drainage without dispatch or processing bindings. Two separate publisher-adapter contracts gate success/non-2xx dispatch bodies to prove shutdown-before-completion, full body consumption, exact partial-success receipts, and explicit redispatch without another insert. Their [shared helper](../../../../interop-tests/cloudflare/publisher-contracts.ts) registers once per language, not in mixed suites. Local Hyperdrive connects directly to PostgreSQL; these tests do not exercise hosted pooling or caching.

## Projects

| Source                                                         | Purpose                                                                                    |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `producer/lib.rs`                                              | `POST /tasks` and `/full-names`, typed publishers, and the Durable Object adapter.         |
| `processor/lib.rs`                                             | Processor delegate configuration, business SQL, and abort-safe application cleanup.        |
| `task.rs`                                                      | `cloudflare_greeting` (`{ name }`) and `cloudflare_full_name` (`{ firstName, lastName }`). |
| `db.rs`, `http.rs`                                             | Application-side business/cancellation connections and producer request validation.        |
| `contracts/`, `rust-contracts.ts`                              | Test-only Workers and workerd contracts. Do not deploy.                                    |
| `contracts/publisher.rs`, `contracts/wrangler.publisher.jsonc` | Publisher adapter and gated dispatch receiver with real Hyperdrive/DO bindings.            |
| `cloudflare.integration.test.ts`                               | Rust -> Rust scenarios and contract registration.                                          |
| `build-rust.mjs`, `setup.ts`                                   | Current-source Worker builds and finite suite preparation.                                 |
| `initialize.rs`, `postgres-fixture.ts`                         | Native schema initialization and the Rust fixture adapter.                                 |

## API and connection lifecycle

Target `wasm32-unknown-unknown` with Bellows default features disabled and `features = ["cloudflare"]`. Match `worker` and `worker-build` **0.8.5**. Native workspace builds do not compile the examples' Wasm-only bodies.

- Use `bellows::cloudflare::sdk::{PostgresPublisher, PostgresPublisherConfig}` for typed immediate publication bound to a task and dispatcher. The synchronous callback runs once per publication, never at construction. Each call owns a fresh listener-free publishing backend, publishes once, retains and validates the ID, awaits shutdown, then consumes the complete `dispatch_task` response. See the [small task-bound example](../../../../README.md#producer) and [compiled handler](./producer/lib.rs).
- Use `bellows::cloudflare::sdk::{PostgresProcessor, PostgresProcessorConfig, PostgresProcessorTask}` for typed registrations routed by definition name, one task ID per request. Its synchronous configuration callback runs once per validated request, not at construction. Supply the real Hyperdrive binding's connection string, `PostgresBackendOptions`, and a `Vec<PostgresProcessorTask>`; the execution backend validates the schema, and initialization stays outside Workers.
- Await `PostgresProcessor::fetch_worker` in the event handler or delegate from an existing router. Bellows owns the `/process` protocol, random worker IDs, and a fresh listener-free execution backend. It uses `run_task_once` to claim before building the worker, pass the claimed payload, renew ownership, and await finalization.
- HTTP **200** with `{ taskId, attemptFinished: true }` means the runtime returned normally and adapter cleanup succeeded, **not task success**. No-claim attempts, handled task failures, and backend errors handled by the runtime can all return this envelope. Adapter-visible failures return a generic **500** and log only the validated ID and lifecycle stage.
- Use `bellows::time::Instant` for portable deadlines. It is `std::time::Instant` on native targets and `web_time::Instant` on Wasm. Workers use SDK execution and timers, not a Tokio runtime; public `Send`/`Sync` bounds remain unchanged.
- Keep one `bellows::cloudflare::sdk::Dispatcher` per Durable Object, constructed with `RetainedTaskDispatcher::from_bindings`. Forward handlers to `fetch_worker` and `alarm_worker`; `dispatch_task` accepts `worker::ObjectNamespace` directly.

Both definitions share one `DISPATCHER` and one `PROCESSOR` binding. Dispatch requires `{ taskId, taskName }`, not a payload; deduplication remains ID-keyed across names. See the [shared protocol](../../../../bellows-ts/test/integration/cloudflare/README.md#protocol-and-limits) for producer routes, name validation, and claim behavior.

### Publisher receipts and errors

The publisher owns Bellows resources and dispatch orchestration, not an HTTP endpoint or arbitrary business clients. Your handler validates input and awaits `publisher.publish(&env, payload)`. Configuration callbacks and publication futures have no extra `Send` requirement. Delegates may be reused without retaining connections; callback-bearing tasks publish without callback registration, and singleton tasks are rejected. There are no future/awaitable publication methods or application cleanup hooks.

The typed result is `Result<PostgresPublisherReceipt, PostgresPublisherError>`, not `worker::Result`. Success retains `task_id: String` and confirms dispatch acceptance, not completion. PostgreSQL processors require canonical positive decimal IDs no greater than `9007199254740991`; the lower-level Rust backend still returns exact `u64` receipts above this range. TypeScript instead throws `PostgresPublishedTaskIdError` for unsafe lower-level numeric receipts, retaining the exact committed string.

Errors expose `stage`, `cause`, optional `receipt`, and a separate later `backend_close_error`; the first failure remains primary. `PostgresPublisherStage::as_str()` yields `configuration`, `acquisition`, `publication`, `task-id`, `backend-close`, or `dispatch`. Backend sources are retained; SDK-affine errors use the existing SDK conversion. Top-level messages are sanitized and the adapter does not log.

Map the structured error before converting it into a generic Worker error. The producer preserves **503** `{ error: "task publication failed" }` without a receipt, or **503** `{ error: "task published, but dispatch acceptance was not confirmed", taskId }` with one. It exposes neither stage nor cause. Success is **202**, the plain ID, `text/plain; charset=utf-8`, and `cache-control: no-store`.

For a close/dispatch error, acceptance is unconfirmed, not necessarily rejected: receipts remain ID-only, so call `dispatch_task(namespace, Task::NAME, &receipt.task_id)` with the original published definition through a trusted path rather than inserting again. A `task-id` receipt is exact but unsupported by this processor and needs another recovery action. A publication error without a receipt does not establish rollback. Never automatically republish.

Await the operation within the request. Ordinary errors await shutdown after acquisition, but dropping/cancelling a future, abrupt termination, or a wasm trap cannot guarantee async cleanup. The adapter does not extend request lifetime, atomically bridge PostgreSQL and a Durable Object, participate in application transactions, schedule future requests, deliver callbacks, or provide an outbox or durable recovery.

### Processor cleanup

The event wrapper in [`processor/lib.rs`](./processor/lib.rs) shares request-local cleanup across both factories. See the [registration example](../../../../README.md#processor-and-deployment) for configuration.

Applications still own arbitrary side-effect resources. Use separate business connections, opened only after a claim, never global or DO-owned clients. Here `Arc<Mutex<Option<db::Connection>>>` keeps ownership outside the spawned worker so cleanup can await cancellation-safe `close()` after lease-loss aborts. The factory retains the runtime's existing `Send`/`Sync` and ownership requirements; the owned cleanup future can hold SDK-affine values.

`with_cleanup` registers a future awaited once whenever configuration returned, including invalid registries, unknown names, randomness/acquisition failure, and no claim. Bellows always awaits its own backend shutdown afterwards if acquisition succeeded, even when application cleanup fails. Configuration that fails before returning owns its partially created resources. Dropping a future does not undo already-sent SQL; abrupt request termination and Wasm traps remain outside this cleanup contract. TypeScript instead needs to drain tracked business promises, which its runtime cannot cancel.

Direct `PostgresExecutionBackend` from `bellows::backends::postgres_execution` plus `bellows::run_task_once` remains the lower-level option for custom integrations. Callers then own backend acquisition and awaited `close()` as well as business cleanup. Do not construct the listening `PostgresBackend` in a Worker.

Direct `PostgresPublishingBackend` plus `dispatch_task` also remains available for caller-managed publication. Import `TaskPublishingBackend`, connect inside the request, and await `close()` on success and error paths before dispatch or response; dropping clones is insufficient. See the [publishing example and capability limits](../../../../README.md#publishing-without-a-listener). Its future-publication capability does not expand the publisher adapter's immediate-only API.

**Do not add `nodejs_compat` to Rust configurations.** With the configured compatibility date, Node-style timer handles are incompatible with the SDK's numeric handles. TypeScript needs this flag for `pg`; Rust sockets do not.

PostgreSQL stores tasks; the dispatcher map is only in-memory state. Its 30-second heartbeat provides neither lease renewal nor restart/eviction recovery. The delegates add no automatic retries or daemon-equivalent discovery. Publication gaps and rediscovery remain application concerns, and side effects plus completion are not atomic or exactly-once.

## Build and test

Requires Node 22+ (prefer Node 24), repository-pinned pnpm, stable Rust, and PostgreSQL 17. No Cloudflare account is needed. From the repository root, install the prerequisites once:

```bash
export WRANGLER_SEND_METRICS=false
rustup component add rustfmt clippy
rustup target add wasm32-unknown-unknown
cargo install worker-build --version 0.8.5 --locked
pnpm install --frozen-lockfile
```

Use an existing PostgreSQL 17 server or start a disposable local one with Docker:

```bash
docker run --detach --rm --name bellows-cloudflare-postgres \
  --publish 127.0.0.1:5432:5432 --env POSTGRES_PASSWORD=postgres postgres:17
docker exec bellows-cloudflare-postgres pg_isready -U postgres -d postgres
```

Wait for `pg_isready` to report acceptance before testing. Remove this container afterward with `docker stop bellows-cloudflare-postgres`.

Build current sources and validate both application bundles without deploying:

```bash
cargo check -p bellows --no-default-features --lib --locked
cargo build -p bellows --no-default-features --features cloudflare --target wasm32-unknown-unknown --lib --locked
cargo build -p bellows-cloudflare-producer -p bellows-cloudflare-processor -p bellows-cloudflare-contracts --target wasm32-unknown-unknown --locked
pnpm --dir bellows/tests/integration/cloudflare build:cloudflare
pnpm --dir bellows/tests/integration/cloudflare exec wrangler deploy --dry-run --config producer/wrangler.jsonc
pnpm --dir bellows/tests/integration/cloudflare exec wrangler deploy --dry-run --config processor/wrangler.jsonc
```

With PostgreSQL ready, run the Rust harness, its contract-only filter, or native dispatcher tests:

```bash
pnpm --dir bellows/tests/integration/cloudflare test:cloudflare
pnpm --dir bellows/tests/integration/cloudflare test:cloudflare:contracts
cargo test -p bellows --features cloudflare --test cloudflare --locked
cargo test -p bellows --no-default-features --features cloudflare --lib --test cloudflare --locked
```

`test:cloudflare` (also the package's `test` script) runs all 34 cases. `test:cloudflare:contracts` selects all 21 contracts, a subset rather than additional registrations. For the 13 platform/SDK cases that do not need PostgreSQL, use `pnpm --dir bellows/tests/integration/cloudflare test:cloudflare -t 'Rust workerd platform and SDK contracts'`.

Set `BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL` to override `postgres://postgres:postgres@localhost:5432/postgres`. The harness needs schema creation/deletion privileges. The legacy `BELLOWS_TS_TEST_POSTGRES_URL` fallback applies only to the TypeScript adapter, not this Rust suite. Missing database or build prerequisites fail tests rather than skipping them.

Each fixture creates an isolated schema and initializes it with Rust's public `bellows::backends::postgres::initialize_postgres_schema` through the native `cloudflare_initialize_postgres` Cargo example. Preparation uses the targeted command `cargo build -p bellows --example cloudflare_initialize_postgres --no-default-features --features postgres --locked --message-format=json` and reads the executable path from Cargo's artifacts, respecting `CARGO_TARGET_DIR`. It does not invoke the native interop crate's TypeScript build. The URL reaches the initializer through its child environment, never command-line arguments. Initialization and connection shutdown are awaited before Workers start; failure still triggers schema cleanup.

The Rust and mixed suites build current Rust Workers automatically; no manual prebuild is required. The helper pins `worker-build` 0.8.5, reuses source/tool/environment-hashed bundles, and copies SDK output unmodified into `build/harness`. Cold preparation has a separate 240-second budget before short fixture hooks. Response deadlines remain two seconds including body consumption, polls three seconds, startup eight seconds, and harness shutdown five seconds. Tests use SQL gates and observed database state, not processing sleeps.

Cleanup releases locks, drains responses and request clients, closes workerd, drops only the fixture-owned schema, closes administrative connections, and restores `CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE`, including on failure. Unexpected runtime logs or cleanup errors fail tests. Generated `build/` and `.wrangler/` output is ignored. All-zero Hyperdrive IDs are local placeholders, not deployable resources.

See the [shared protocol and opt-in hosted verification instructions](../../../../bellows-ts/test/integration/cloudflare/README.md#opt-in-hosted-verification) before using the application projects outside workerd. For Rust, use `initialize_postgres_schema` through a direct administrative connection. Preserve the private processor, disabled Hyperdrive query caching, verified origin TLS, disposable resources, and language-specific compatibility flags. The contract Workers are test-only and must not be deployed.

## Complete repository validation

Native Rust tests require the default local PostgreSQL server and temporary-database privileges; the Cloudflare URL override does not configure them. The full TypeScript suite also creates temporary databases, using `BELLOWS_TS_TEST_POSTGRES_URL` or the local default. Root `pnpm test` runs the three Node test packages serially so Worker preparation cannot race, covering all **79 Cloudflare cases** once alongside the other Node tests: **19 TypeScript, 34 Rust, 26 mixed**. Full Cargo workspace builds intentionally retain the separate native interop crate's TypeScript build. Run from the repository root:

```bash
cargo fmt --all && cargo build --all --all-targets && cargo clippy --all --all-targets && cargo test --all
cargo clippy --all --all-targets --all-features --locked -- -D warnings
cargo test --all --all-features --locked
pnpm check:fix && pnpm lint && pnpm typecheck && pnpm build && pnpm test
git diff --check
```

Also lint the Wasm-only code:

```bash
cargo clippy -p bellows --no-default-features --features cloudflare --target wasm32-unknown-unknown --all-targets --locked -- -D warnings
cargo clippy -p bellows-cloudflare-producer -p bellows-cloudflare-processor -p bellows-cloudflare-contracts --target wasm32-unknown-unknown --all-targets --locked -- -D warnings
```
