# TypeScript Cloudflare Workers with PostgreSQL

This executable example runs a **producer Worker -> `global` Durable Object dispatcher -> service-bound processor Worker**. This directory owns **19 cases**: thirteen TypeScript -> TypeScript topology scenarios, four direct publishing-backend contracts, and two publisher-adapter contracts. See the independent [Rust/Wasm harness](../../../../bellows/tests/integration/cloudflare/README.md) for Rust -> Rust and workerd contracts, and the [mixed-language suite](../../../../interop-tests/cloudflare/README.md) for both cross-language directions.

PostgreSQL stores tasks. The Durable Object retains outstanding processor requests **in memory**, suppressing duplicate IDs even across different names until each response body is consumed. One processor routes two distinct definitions by name, claims the task, executes its decoded payload, renews the lease, and records completion or failure.

## Files and APIs

| File                                                    | Purpose                                                                                     |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `task.ts`                                               | `cloudflare_greeting` (`{ name }`) and `cloudflare_full_name` (`{ firstName, lastName }`).  |
| `workers/producer.ts`                                   | `POST /tasks` and `/full-names`, typed publishers, and the `TaskDispatcher` Durable Object. |
| `workers/processor.ts`                                  | Processor delegate configuration and the `processed_tasks` side effect.                     |
| `workers/publishing.ts`, `publishing-contracts.ts`      | Test-only direct publishing contracts; no dispatch or processing. Do not deploy.            |
| `workers/publisher.ts`, `wrangler.publisher.jsonc`      | Test-only publisher adapter and gated dispatch receiver. Do not deploy.                     |
| `wrangler.*.jsonc`                                      | Hyperdrive, Durable Object, and service bindings.                                           |
| `cloudflare.integration.test.ts`, `postgres-fixture.ts` | TypeScript-only workerd suite and production schema-initializer adapter.                    |

The examples import repository source. In an application, use:

- `@xjonathanlei/bellows` for task definitions, `WorkerFactory`, and `TaskSuccess`.
- `@xjonathanlei/bellows/cloudflare` for `dispatchTask` and `RetainedTaskDispatcher`.
- `@xjonathanlei/bellows/cloudflare/postgres` for `createPostgresPublisher`, `PostgresPublisherError`, `createPostgresProcessor`, and `createPostgresProcessorTask`.
- `@xjonathanlei/bellows/backends/postgres` for direct, Node-side schema initialization.
- `@xjonathanlei/bellows/backends/postgres-publishing` for listener-free typed publication.

## Publisher delegate

[`workers/producer.ts`](./workers/producer.ts) retains separate typed `createPostgresPublisher((env: ProducerEnv) => config)` delegates for both definitions, using the same `env.DISPATCHER`. Each synchronous callback maps Hyperdrive, schema, task, and dispatcher once per publication. Construction does no I/O, and delegates retain no connections between calls. After application validation, the handler awaits the matching publisher; Bellows owns storage encoding, one insert, safe-positive ID validation, awaited backend shutdown, and complete dispatch-response consumption.

See the [small typed handler](../../../README.md#publisher). This is immediate publication, not a library HTTP endpoint. Authentication, routing, business validation, and public responses remain application responsibilities. Callback-bearing tasks publish without callback registration or delivery; no future/awaitable publication or application cleanup hooks are exposed.

String receipts confirm dispatch acceptance, not task completion. `PostgresPublisherError` retains the primary stage/cause, an optional exact receipt, and any later close failure separately. Stages are `configuration`, `acquisition`, `publication`, `task-id`, `backend-close`, and `dispatch`. Receipts remain ID-only: recover a close/dispatch failure through trusted `dispatchTask(env.DISPATCHER, task.name, receipt.taskId)` with the original published definition instead of another insert. A `task-id` receipt is exact but unsupported by the processor and must not be blindly redispatched. The lower-level `PostgresPublishedTaskIdError` preserves unsafe PostgreSQL IDs as exact strings; the adapter recognizes it without rounding.

A publication error without a receipt does not prove rollback. The producer maps structured errors before any generic conversion: no receipt returns HTTP **503** with `{ error: "task publication failed" }`; a receipt returns **503** with `{ error: "task published, but dispatch acceptance was not confirmed", taskId }`. It does not expose internal stages or causes. Success remains **202** with the plain string ID, `text/plain; charset=utf-8`, and `cache-control: no-store`.

Await the call within the request; the adapter does not extend request lifetime. Ordinary errors await shutdown after acquisition, but abrupt termination cannot guarantee cleanup. It owns Bellows resources, not arbitrary business clients. There is no atomic PostgreSQL-to-DO delivery, automatic republishing, outbox, durable recovery, future-request scheduling, or application-transaction participation.

## Processor delegate and cleanup

[`workers/processor.ts`](./workers/processor.ts) default-exports `createPostgresProcessor((env: ProcessorEnv) => config)`. The synchronous callback maps `env.HYPERDRIVE.connectionString` and `env.BELLOWS_SCHEMA`, registers typed factories for both definitions, and registers application cleanup. It runs only after request validation, once per request. See the [short default-export example](../../../README.md#processor); a router can instead return `processor.fetch(request, env)` without binding `this`.

Bellows owns request parsing, name and safe-integer ID validation, random worker IDs, and execution-backend acquisition/shutdown. Each selected attempt uses a fresh listener-free backend. The runtime claims before `factory.build`, so only the database's claimed payload reaches the business worker. Schema validation belongs to the execution backend; initialization stays outside Workers.

Applications own their business resources. This processor opens a separate `pg.Client` only during claimed processing so lease renewal can progress, and awaits `client.end()` in `finally`. It also retains the business operation promise in the request's configuration scope: TypeScript does not cancel that promise on renewal loss, so a local `finally` alone cannot prove the operation has ended when the runtime returns. Registered cleanup drains the operation, including its shutdown, without reclassifying handled worker failures as adapter failures.

The delegate awaits application cleanup once whenever configuration returned, including invalid registries, unknown names, randomness/acquisition failure, and no-claim paths, then always awaits Bellows backend shutdown if acquisition succeeded. A cleanup failure cannot skip that shutdown. Configuration that throws before returning remains responsible for partially created resources; abrupt request termination is not recoverable by this contract.

For custom integrations, `PostgresExecutionBackend` from `@xjonathanlei/bellows/backends/postgres-execution` plus `runTaskOnce` remains available as the lower-level API, with caller-owned cleanup. Do **not** construct the listening `PostgresBackend` in a Worker or retain clients globally or in a Durable Object. Generic root/`cloudflare` imports stay independent of PostgreSQL and Node modules; the PostgreSQL subpath still requires `pg` compatibility.

`PostgresPublishingBackend` plus `dispatchTask` also remains available for caller-managed publication. Connect through Hyperdrive inside each request and await `close()` in `finally` before dispatch or response. See the [typed example and capability limits](../../../README.md#postgrespublishingbackend); the backend's future-publication capability is not exposed by the publisher adapter.

## Run locally

Requires Node 22+ (prefer Node 24 to avoid native SQLite experimental warnings), repository-pinned pnpm, and PostgreSQL 17. No Rust toolchain, Wasm target, `worker-build`, or Cloudflare account is needed. From the repository root:

```bash
export WRANGLER_SEND_METRICS=false
pnpm install --frozen-lockfile
```

Use an existing PostgreSQL 17 server or start a disposable local one before running tests:

```bash
docker run --detach --rm --name bellows-cloudflare-postgres \
  --publish 127.0.0.1:5432:5432 --env POSTGRES_PASSWORD=postgres postgres:17
docker exec bellows-cloudflare-postgres pg_isready -U postgres -d postgres
```

Wait for `pg_isready` to report acceptance. Remove this container afterward with `docker stop bellows-cloudflare-postgres`.

Run the focused suite:

```bash
pnpm --dir bellows-ts test:cloudflare
```

`BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL` overrides the Cloudflare database URL. This TypeScript adapter falls back to `BELLOWS_TS_TEST_POSTGRES_URL`, then `postgres://postgres:postgres@localhost:5432/postgres`. The focused suite needs schema creation/deletion privileges and initializes each isolated schema with the production `initializePostgresSchema`. The full TypeScript suite also creates temporary databases and uses `BELLOWS_TS_TEST_POSTGRES_URL` for its other PostgreSQL tests. Native Rust tests use the local default URL, not these overrides.

The thirteen topology cases cover early acceptance, heterogeneous routing and concurrency, ownership/redelivery, failure/retry, validation/cleanup, a real insert constraint failure without retry, and four exact-ID boundaries (`9007199254740991`, `9007199254740992`, `9007199254740993`, `9223372036854775807`). The safe boundary processes normally; unsupported IDs retain exact unclaimed rows with no side effect, client leak, or processor rejection log.

Four direct backend contracts cover immediate/future callback-bearing and void tasks, SQL errors, and gated publication with awaited shutdown. They use a test-only Hyperdrive Worker without a dispatcher or processor. Two separate publisher-adapter contracts use real Hyperdrive/DO bindings and gated success/non-2xx response bodies: they prove one committed row, shutdown before dispatch completion, full body consumption beyond diagnostic limits, sanitized partial-success receipts, and explicit redispatch of the retained ID without republishing. The [shared helper](../../../../interop-tests/cloudflare/publisher-contracts.ts) registers these cases once per language, not in mixed suites.

All 19 cases also run under `pnpm --dir bellows-ts test`. Neither command prepares Rust or requires a TypeScript `dist` prebuild. Root `pnpm test` is the aggregate command: it runs all three test packages serially, including the Rust and mixed suites, and therefore needs their Rust prerequisites. The aggregate covers all **79 Cloudflare cases** once: **19 TypeScript, 34 Rust, and 26 mixed**.

The [neutral fixture and scenarios](../../../../interop-tests/cloudflare/README.md#shared-support) preserve real bindings, SQL gates, and observed lease/payload/side-effect/deletion assertions. Response deadlines include full body consumption within two seconds; polls are bounded to three seconds, startup to eight seconds, and harness shutdown to five seconds. Cleanup releases locks, drains responses and request clients, closes workerd, drops only the owned schema, closes administrative connections, and restores `CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE`, including on failure. Unexpected runtime logs and cleanup errors fail tests; unavailable PostgreSQL fails rather than skipping them.

**Local Hyperdrive connects directly to PostgreSQL; it does not exercise hosted pooling or caching.**

## Types and dry-run builds

```bash
pnpm --dir bellows-ts typecheck:workers
pnpm --dir bellows-ts exec wrangler deploy --dry-run --config test/integration/cloudflare/wrangler.producer.jsonc
pnpm --dir bellows-ts exec wrangler deploy --dry-run --config test/integration/cloudflare/wrangler.processor.jsonc
```

Worker typechecking generates its runtime declarations and also runs under normal `pnpm typecheck`. Keep `nodejs_compat` for TypeScript's `pg`; **do not copy it to Rust**, whose SDK timers require numeric handles. See the Rust guide for Wasm builds and dry runs.

## Protocol and limits

- `POST /tasks` accepts a non-blank `name`; `POST /full-names` accepts non-blank `firstName` and `lastName`. Each component must be a string of at most 200 UTF-16 code units; validation does not trim values. Publication commits and its connection closes before dispatch. HTTP **202** returns the task ID, not completion.
- Both `POST /dispatch` and `POST /process` require `{ taskId: string, taskName: string }`, for example `{ "taskId": "17", "taskName": "cloudflare_full_name" }`. Dispatch forwards exactly those fields, not payloads, and accepts opaque IDs of 1–200 UTF-16 code units. Both PostgreSQL processors require 1–16 ASCII decimal digits, starting with 1–9, with a value no greater than `9007199254740991`. IDs remain strings in responses; extra request properties are ignored.
- Names come from `TaskDefinition::NAME` / `factory.task.name` and match exactly, without trimming, case folding, or a length limit. Missing, non-string, or empty names return **400** even with one registration. Registries must be non-empty with unique, non-empty names; invalid registries are **500** configuration failures. Valid unknown names return **404** `{ error: "unknown task name" }` without acquisition. Claims check both ID and persisted name before decoding; a registered-name mismatch is an ordinary no-claim attempt.
- HTTP **200** with `{ taskId, attemptFinished: true }` means the runtime returned normally and adapter cleanup succeeded, including no-claim and handled-failure attempts. Some backend errors are also handled by the runtime; this response is not business success. A persisted side effect plus deletion of the Bellows row establishes success in these tests.
- Adapter-visible configuration, randomness, acquisition, uncaught attempt, or cleanup failures return HTTP **500** with `{ "error": "task processing attempt failed" }`. Server diagnostics identify only the validated ID and lifecycle stage, not configuration or driver error strings.
- A publisher close/dispatch failure returns HTTP **503** with the existing ID and unconfirmed acceptance. Recover with trusted redispatch, not republishing. A `task-id` failure uses the same envelope but its ID is unsupported: inspect database state and choose a different recovery action. A no-receipt publication failure is an unknown outcome, not proof of rollback.
- Schema initialization belongs outside Worker requests. Qualify tables and parameterize values. Schema names use lowercase ASCII letters, digits, and underscores, starting with a letter or underscore; choose short names to avoid PostgreSQL truncation.
- The 30-second alarm is a heartbeat, not lease renewal or retry. Dispatch state is not persisted. Publication gaps, expired-task discovery, and restart/eviction recovery remain application concerns.
- Side effects and completion are separate operations, **not exactly-once**. Use idempotent side effects and protect producer access; this unauthenticated example is not production-complete.

## Opt-in hosted verification

Hosted verification is manual, requires Cloudflare permissions, and may incur charges. Use the same source projects with a disposable database and private deployment configuration:

1. Initialize an isolated schema through a **direct administrative connection**, using `initializePostgresSchema`. Create `processed_tasks` with `task_id BIGINT PRIMARY KEY`, `name TEXT NOT NULL`, and `execution_count INTEGER NOT NULL CHECK (execution_count > 0)`.
2. Disable the `bellows_tasks_notify_available` trigger **only in this callback-free, explicit-dispatch schema**. Hyperdrive does not support `LISTEN`/`NOTIFY`; leave triggers intact for listening deployments.
3. Create a real Hyperdrive configuration with **query caching disabled and verified origin TLS**. Replace the all-zero example IDs in ignored configuration copies; never commit credentials. See [Hyperdrive configuration](https://developers.cloudflare.com/hyperdrive/).
4. Set `BELLOWS_SCHEMA`, use disposable Worker names, and match the producer's `PROCESSOR` service binding to the processor name. Preserve the Durable Object migration and language-specific compatibility flags. Adjust relative entry-point/build paths if moving configuration files.
5. Keep the processor private (`workers_dev: false`, `preview_urls: false`, no public routes). Expose the producer only through an access-controlled route. Deploy the processor, then the producer.
6. Submit a greeting to `/tasks` and a full-name payload to `/full-names`. Verify within a bounded window that `processed_tasks` has each expected name and `execution_count = 1`, and that both Bellows rows are gone. Acceptance alone is insufficient.
7. Delete only the disposable deployments, Hyperdrive configuration, and database/schema; remove private configuration and credentials.

This checks hosted connectivity and task behavior—not pooling performance, cache correctness, durable recovery, or exactly-once execution.
