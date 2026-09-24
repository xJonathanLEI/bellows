# TypeScript Cloudflare Workers with PostgreSQL

This executable example runs a **producer Worker -> `global` Durable Object dispatcher -> service-bound processor Worker**. This directory owns TypeScript -> TypeScript topology scenarios, direct publishing-backend contracts, and publisher-adapter contracts. See the independent [Rust/Wasm harness](../../../../bellows/tests/integration/cloudflare/README.md) for Rust -> Rust and workerd contracts, and the [mixed-language suite](../../../../interop-tests/cloudflare/README.md) for both cross-language directions.

PostgreSQL stores payloads and decides claimability. The SQLite-backed Durable Object accepts dispatches in memory, persists processor scheduling hints, suppresses active duplicate IDs even across different names, and shares one alarm between scheduling and idle warming heartbeats. The processor routes definitions by name, claims before building, renews leases, and reports a next action only after finalization and owned cleanup.

## Files and APIs

| File                                                    | Purpose                                                                              |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `task.ts`                                               | Greeting, full-name, and controlled scheduling definitions.                          |
| `workers/producer.ts`                                   | Typed publication, minute-Cron sweeping, fixtures, and the `TaskDispatcher` wrapper. |
| `workers/processor.ts`                                  | Processor delegate configuration and the `processed_tasks` side effect.              |
| `workers/publishing.ts`, `publishing-contracts.ts`      | Test-only direct publishing contracts; no dispatch or processing. Do not deploy.     |
| `workers/publisher.ts`, `wrangler.publisher.jsonc`      | Test-only publisher adapter and gated dispatch receiver. Do not deploy.              |
| `wrangler.*.jsonc`                                      | Hyperdrive, Durable Object, and service bindings.                                    |
| `cloudflare.integration.test.ts`, `postgres-fixture.ts` | TypeScript-only workerd suite and production schema-initializer adapter.             |

The examples import repository source. In an application, use:

- `@xjonathanlei/bellows` for task definitions, `WorkerFactory`, and `TaskSuccess`.
- `@xjonathanlei/bellows/cloudflare` for `dispatchTask` and `RetainedTaskDispatcher`.
- `@xjonathanlei/bellows/cloudflare/postgres` for `createPostgresPublisher`, `PostgresPublisherError`, `createPostgresProcessor`, `createPostgresProcessorTask`, and `createPostgresSweeper`.
- `@xjonathanlei/bellows/backends/postgres` for direct, Node-side schema initialization.
- `@xjonathanlei/bellows/backends/postgres-publishing` for listener-free typed publication.

## Publisher delegate

[`workers/producer.ts`](./workers/producer.ts) retains separate typed `createPostgresPublisher((env: ProducerEnv) => config)` delegates for each definition, using the same `env.DISPATCHER`. Each synchronous callback maps Hyperdrive, schema, task, and dispatcher once per publication. Construction does no I/O, and delegates retain no connections between calls. After application validation, the handler awaits the matching publisher; Bellows owns storage encoding, one insert, safe-positive ID validation, awaited backend shutdown, and complete dispatch-response consumption.

See the [small typed handler](../../../README.md#publisher). Use `publish(env, payload)` or `publishFuture(env, payload, availableFromMs)`, with absolute Unix milliseconds. Both dispatch immediately; the processor obtains future availability from PostgreSQL rather than from dispatch metadata. Authentication, routing, business validation, and public responses remain application responsibilities. Callback-bearing tasks publish without callback registration or delivery; awaitable publication and application cleanup hooks are not exposed.

String receipts confirm dispatch acceptance, not task completion. `PostgresPublisherError` retains the primary stage/cause, an optional exact receipt, and any later close failure separately. Stages are `configuration`, `acquisition`, `publication`, `task-id`, `backend-close`, and `dispatch`. Receipts remain ID-only: recover a close/dispatch failure through trusted `dispatchTask(env.DISPATCHER, task.name, receipt.taskId)` with the original published definition instead of another insert. A `task-id` receipt is exact but unsupported by the processor and must not be blindly redispatched. The lower-level `PostgresPublishedTaskIdError` preserves unsafe PostgreSQL IDs as exact strings; the adapter recognizes it without rounding.

A publication error without a receipt does not prove rollback. The producer maps structured errors before any generic conversion: no receipt returns HTTP **503** with `{ error: "task publication failed" }`; a receipt returns **503** with `{ error: "task published, but dispatch acceptance was not confirmed", taskId }`. It does not expose internal stages or causes. Success remains **202** with the plain string ID, `text/plain; charset=utf-8`, and `cache-control: no-store`.

Await the call within the request; the adapter does not extend request lifetime. Ordinary errors await shutdown after acquisition, but abrupt termination cannot guarantee cleanup. It owns Bellows resources, not arbitrary business clients. There is no atomic PostgreSQL-to-DO delivery, automatic republishing, outbox, or publisher application-transaction participation. The separate sweeper recovers eligible rows after missed invocation.

## Scheduled sweeper

The producer composes `createPostgresSweeper(publisherConfig).scheduled` with `fetch`; [`wrangler.producer.jsonc`](./wrangler.producer.jsonc) registers `* * * * *`. The [standalone export fixture](./workers/postgres-sweeper.typecheck.ts) checks compatibility with generated Workers types. See [setup examples](../../../README.md#sweeper) and [recovery semantics](../../../../README.md#minute-cron-postgresql-recovery).

Tests inject real workerd scheduled events to verify recovery and observable failures, without waiting for Cron delivery. Coverage is shared across all four language topologies; see the [shared harness](../../../../interop-tests/cloudflare/README.md#shared-support). Hosted Cron delivery is not tested.

## Processor delegate and cleanup

[`workers/processor.ts`](./workers/processor.ts) delegates to `createPostgresProcessor((env: ProcessorEnv) => config)`. The synchronous callback maps `env.HYPERDRIVE.connectionString` and `env.BELLOWS_SCHEMA`, registers typed factories and application cleanup, and runs once after request validation. The thin test wrapper can discard one completed instruction to exercise response-loss recovery. See the [short default-export example](../../../README.md#processor); a router can return `processor.fetch(request, env)` without binding `this`.

Bellows owns request parsing, name and safe-integer ID validation, random worker IDs, and execution-backend acquisition/shutdown. Each selected attempt uses a fresh listener-free backend. The runtime claims before `factory.build`, so only the database's claimed payload reaches the business worker. Schema validation belongs to the execution backend; initialization stays outside Workers.

Applications own their business resources. This processor opens a separate `pg.Client` only during claimed processing so lease renewal can progress, and awaits `client.end()` in `finally`. It also retains the business operation promise in the request's configuration scope: TypeScript does not cancel that promise on renewal loss, so a local `finally` alone cannot prove the operation has ended when the runtime returns. Registered cleanup drains the operation, including its shutdown, without reclassifying handled worker failures as adapter failures.

The delegate awaits application cleanup once whenever configuration returned, including invalid registries, unknown names, randomness/acquisition failure, and no-claim paths, then always awaits Bellows backend shutdown if acquisition succeeded. A cleanup failure cannot skip that shutdown. Configuration that throws before returning remains responsible for partially created resources; abrupt request termination is not recoverable by this contract.

For custom integrations, `PostgresExecutionBackend` from `@xjonathanlei/bellows/backends/postgres-execution` plus `runTaskOnce` remains available as the lower-level API, with caller-owned cleanup. Do **not** construct the listening `PostgresBackend` in a Worker or retain clients globally or in a Durable Object. Generic root/`cloudflare` imports stay independent of PostgreSQL and Node modules; the PostgreSQL subpath still requires `pg` compatibility.

`PostgresPublishingBackend` plus `dispatchTask` also remains available for caller-managed publication. Connect through Hyperdrive inside each request and await `close()` in `finally` before dispatch or response. See the [typed example and capability limits](../../../README.md#postgrespublishingbackend).

## Run locally

Requires Node 22+ (prefer Node 24 to avoid native SQLite experimental warnings), repository-pinned pnpm, and PostgreSQL 17. No Rust toolchain, Wasm target, `worker-build`, or Cloudflare account is needed. From the repository root:

```bash
export WRANGLER_SEND_METRICS=false
pnpm install --frozen-lockfile
```

Use an existing PostgreSQL 17 server or start a disposable local one before running tests:

```bash
docker run --detach --rm --name bellows-cloudflare-postgres \
  --publish 127.0.0.1:5432:5432 --env POSTGRES_PASSWORD=postgres postgres:17 \
  -c max_connections=600
docker exec bellows-cloudflare-postgres pg_isready -U postgres -d postgres
```

Wait for `pg_isready` to report acceptance. The simultaneous 101-task sweep gates require connection headroom; `max_connections=600` is test-server configuration, not production sizing guidance. Remove this container afterward with `docker stop bellows-cloudflare-postgres`.

Run the focused suite:

```bash
pnpm --dir bellows-ts test:cloudflare
```

`BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL` overrides the Cloudflare database URL. This TypeScript adapter falls back to `BELLOWS_TS_TEST_POSTGRES_URL`, then `postgres://postgres:postgres@localhost:5432/postgres`. The focused suite needs schema creation/deletion privileges and initializes each isolated schema with the production `initializePostgresSchema`. The full TypeScript suite also creates temporary databases and uses `BELLOWS_TS_TEST_POSTGRES_URL` for its other PostgreSQL tests. Native Rust tests use the local default URL, not these overrides.

The shared topology scenarios cover high-level future publication, automatic lease-delayed execution after renewal, scheduled/immediate failure, successful self-rescheduling, transient failure repair, lost completion responses, and simultaneous deadlines across definitions. They retain early acceptance, exact routing, ownership/redelivery, validation/cleanup, insert failure without republishing, and exact-ID boundary coverage. SQL execution timestamps prove future work and rescheduled attempts do not execute early; all fan-out business operations remain gated until every task starts.

Real storage contracts reconstruct delegates with pending and in-flight records and preserve absolute alarms. Reconstruction is not an induced platform eviction. Long-watchdog expiry, stale responses, failed persistence, and 300-ID unrestricted fan-out use deterministic unit-test clocks; the topology suite uses real automatic alarms without a second dispatch or manual alarm invocation. Fixture-only inspection, reconstruction, response-loss, and cleanup routes must not be deployed.

Direct backend contracts cover immediate/future callback-bearing and void tasks, SQL errors, and gated publication with awaited shutdown. They use a test-only Hyperdrive Worker without a dispatcher or processor. Separate publisher-adapter contracts exercise both publication methods through real Hyperdrive/DO bindings and gated success/non-2xx bodies: one committed row, shutdown before dispatch completion, full response consumption, exact partial-success receipts, and explicit redispatch without republishing. The [shared helper](../../../../interop-tests/cloudflare/publisher-contracts.ts) registers these cases once per language, not in mixed suites.

These cases also run under `pnpm --dir bellows-ts test`. Neither command prepares Rust or requires a TypeScript `dist` prebuild. Root `pnpm test` runs all three test packages serially, including the Rust and mixed suites, and therefore needs their Rust prerequisites. Each scenario registers once per topology, and direct contracts remain language-owned.

The [neutral fixture and scenarios](../../../../interop-tests/cloudflare/README.md#shared-support) preserve real bindings, SQL gates, and observed lease/payload/side-effect/deletion assertions. Response deadlines include full body consumption within two seconds; polls are bounded to three seconds, startup to eight seconds, and harness shutdown to five seconds. Cleanup clears fixture-owned retained schedules, releases locks, drains responses, scheduled events, and request clients, closes workerd, drops only the owned schema, closes administrative connections, and restores `CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE`, including on failure. Unexpected runtime logs and cleanup errors fail tests; unavailable PostgreSQL fails rather than skipping them.

**Local Hyperdrive connects directly to PostgreSQL; it does not exercise hosted pooling or caching.**

## Types and dry-run builds

```bash
pnpm --dir bellows-ts typecheck:workers
pnpm --dir bellows-ts exec wrangler deploy --dry-run --config test/integration/cloudflare/wrangler.producer.jsonc
pnpm --dir bellows-ts exec wrangler deploy --dry-run --config test/integration/cloudflare/wrangler.processor.jsonc
```

Worker typechecking generates its runtime declarations and also runs under normal `pnpm typecheck`. Keep `nodejs_compat` for TypeScript's `pg`; **do not copy it to Rust**, whose SDK timers require numeric handles. See the Rust guide for Wasm builds and dry runs.

## Protocol and limits

- `POST /tasks` accepts a non-blank `name`; `POST /full-names` accepts non-blank `firstName` and `lastName`. Each component must be a string of at most 200 UTF-16 code units; validation does not trim values. An optional `?availableFromMs=<Unix milliseconds>` selects high-level future publication. Publication commits and its connection closes before immediate dispatch. HTTP **202** returns the ID after in-memory acceptance, not durable scheduling or completion.
- Test-only `POST /scheduled` accepts `{ name, mode, availableFromMs }`. On its first execution, `mode` selects `failure`, `success`, or `immediate` rescheduling; the second execution completes under the same ID. The fixture records SQL execution timestamps independently of Bellows state.
- Both `POST /dispatch` and `POST /process` require `{ taskId: string, taskName: string }`, for example `{ "taskId": "17", "taskName": "cloudflare_full_name" }`. Dispatch forwards exactly those fields, not payloads or scheduling metadata, and accepts opaque IDs of 1–200 UTF-16 code units. Both PostgreSQL processors require 1–16 ASCII decimal digits, starting with 1–9, with a value no greater than `9007199254740991`. IDs remain strings in responses; extra request properties are ignored.
- Names come from `TaskDefinition::NAME` / `factory.task.name` and match exactly, without trimming, case folding, or a length limit. Missing, non-string, or empty names return **400** even with one registration. Registries must be non-empty with unique, non-empty names; invalid registries are **500** configuration failures. Valid unknown names return **404** `{ error: "unknown task name" }` without acquisition. Claims check both ID and persisted name before decoding; a registered-name mismatch is an ordinary no-claim attempt.
- HTTP **200** reports `{ taskId, nextAction: { type: "done" } }` for an absent matching task or committed completion, or `{ taskId, nextAction: { type: "retryAt", atMs } }` for observed availability/leases and committed retries or self-rescheduling. `atMs` is an absolute Unix millisecond timestamp within the non-negative JavaScript Date range; past timestamps request a prompt recheck. Rust rounds hints up to millisecond precision, never adding query or cleanup latency. This describes a known next action, not business success. Responses await finalization, application cleanup, and backend shutdown.
- Runtime uncertainty and configuration, randomness, acquisition, uncaught attempt, application cleanup, or backend-close failures return HTTP **500** with `{ "error": "task processing attempt failed" }`. Server diagnostics identify only the validated ID and lifecycle stage, not configuration or driver error strings.
- A publisher close/dispatch failure returns HTTP **503** with the existing ID and unconfirmed acceptance. Recover with trusted redispatch, not republishing. A `task-id` failure uses the same envelope but its ID is unsupported: inspect database state and choose a different recovery action. A no-receipt publication failure is an unknown outcome, not proof of rollback.
- Schema initialization belongs outside Worker requests. Qualify tables and parameterize values. Schema names use lowercase ASCII letters, digits, and underscores, starting with a letter or underscore; choose short names to avoid PostgreSQL truncation.
- The shared alarm selects the earliest persisted task/watchdog deadline or independent 30-second heartbeat. Every due distinct ID launches without a Bellows concurrency limit; platform limits still apply. Active duplicates preserve the original routing; pending IDs permit explicit corrected redispatch. Only a fully consumed, successful matching-ID `done` removes tracking.
- External dispatch launches in memory before checking the warming alarm, with no task writes or schedule scans. It sets the alarm only when missing or later than `now + 30 seconds`, preserving earlier and overdue alarms. Any valid `retryAt` starts persistence and resets backoff. Infrastructure uncertainty retries with one-to-thirty-second exponential delays, in memory for unsaved tasks and durably for saved schedules. The 60-second watchdog applies only to scheduled attempts; supersession ignores stale results but does not guarantee business cancellation. PostgreSQL remains authoritative. See [durable scheduling guarantees](../../../../README.md#durable-scheduling-and-limits).
- Prompt publisher dispatch, DO alarms for known schedules, and minute-Cron PostgreSQL rediscovery complement each other. Sweeping adds recovery for earlier loss without strengthening in-memory acceptance into durable tracking, which starts with a persisted `retryAt` hint. There is no outbox, automatic republishing, atomic publication-to-dispatch transaction, or added callback delivery, and abrupt termination has no async-cleanup guarantee.
- Side effects and completion are separate operations, **not exactly-once**. Use idempotent side effects and protect producer access; this unauthenticated example is not production-complete.

## Opt-in hosted verification

Hosted verification is manual, requires Cloudflare permissions, and may incur charges. Before adapting these projects, remove all fixture-only `__test` routes and the controlled scheduling task. Use a disposable database and private deployment configuration:

1. Initialize an isolated schema through a **direct administrative connection**, using `initializePostgresSchema`. Create `processed_tasks` with `task_id BIGINT PRIMARY KEY`, `name TEXT NOT NULL`, and `execution_count INTEGER NOT NULL CHECK (execution_count > 0)`.
2. Disable the `bellows_tasks_notify_available` trigger **only in this callback-free, explicit-dispatch schema**. Hyperdrive does not support `LISTEN`/`NOTIFY`; leave triggers intact for listening deployments.
3. Create a real Hyperdrive configuration with **query caching disabled and verified origin TLS**. Replace the all-zero example IDs in ignored configuration copies; never commit credentials. See [Hyperdrive configuration](https://developers.cloudflare.com/hyperdrive/).
4. Set `BELLOWS_SCHEMA` to a schema dedicated to this processor's workload, use disposable Worker names, and match the producer's `PROCESSOR` service binding to the processor name. Preserve the Durable Object migration, language-specific compatibility flags, and producer's `triggers.crons: ["* * * * *"]`. Adjust relative entry-point/build paths if moving configuration files.
5. Keep the processor private (`workers_dev: false`, `preview_urls: false`, no public routes). Expose the producer only through an access-controlled route. Deploy the processor, then the producer.
6. Submit a greeting to `/tasks` and a future full-name payload to `/full-names?availableFromMs=<Unix milliseconds>`. Verify no early execution, then automatic execution without redispatch, `execution_count = 1`, and removal of Bellows rows and any persisted DO schedules. Acceptance alone is insufficient.
7. Publish another task through the listener-free backend without dispatching it. Observe a hosted Cron event and completion under the original ID, without another insert or manual dispatch. Inspect scheduled-event outcomes using sanitized diagnostics; do not impose a strict 60-second deadline.
8. Delete only the disposable deployments and their Cron Triggers, Hyperdrive configuration, and database/schema; remove private configuration and credentials.

If performed, this checks hosted connectivity, alarm scheduling, and Cron recovery—not pooling performance, cache correctness, induced platform eviction, a recovery-time SLA, or exactly-once execution. Local tests alone do not establish these hosted results.
