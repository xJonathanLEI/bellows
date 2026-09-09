# TypeScript Cloudflare Workers with PostgreSQL

This executable example runs a **producer Worker -> `global` Durable Object dispatcher -> service-bound processor Worker**. This directory owns the five TypeScript -> TypeScript scenarios. See the independent [Rust/Wasm harness](../../../../bellows/tests/integration/cloudflare/README.md) for Rust -> Rust and workerd contracts, and the [mixed-language suite](../../../../interop-tests/cloudflare/README.md) for both cross-language directions.

PostgreSQL stores tasks. The Durable Object retains outstanding processor requests **in memory**, suppressing duplicate IDs until each response body is consumed. The processor claims the task, executes its decoded payload, renews the lease, and records completion or failure.

## Files and APIs

| File                                                    | Purpose                                                                  |
| ------------------------------------------------------- | ------------------------------------------------------------------------ |
| `task.ts`                                               | Shared `cloudflare_greeting` task with `{ name: string }` payload.       |
| `workers/producer.ts`                                   | `POST /tasks`, SQL publication, and the `TaskDispatcher` Durable Object. |
| `workers/processor.ts`                                  | `POST /process`, task execution, and the `processed_tasks` side effect.  |
| `wrangler.*.jsonc`                                      | Hyperdrive, Durable Object, and service bindings.                        |
| `cloudflare.integration.test.ts`, `postgres-fixture.ts` | TypeScript-only workerd suite and production schema-initializer adapter. |

The examples import repository source. In an application, use:

- `@xjonathanlei/bellows` for task definitions, `runTaskOnce`, and `TaskSuccess`.
- `@xjonathanlei/bellows/cloudflare` for `dispatchTask` and `RetainedTaskDispatcher`.
- `@xjonathanlei/bellows/backends/postgres-execution` for `PostgresExecutionBackend`.
- `@xjonathanlei/bellows/backends/postgres` for direct, Node-side schema initialization.

Do **not** construct the listening `PostgresBackend` in a Worker. Worker connections come only from `env.HYPERDRIVE.connectionString`, are request-scoped, and close before returning—including failure and no-claim paths. Use a separate connection for side effects so lease renewal can progress.

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

The five scenarios cover early acceptance, duplicates/concurrency, ownership/redelivery, failure/retry, and validation/cleanup. They also run under `pnpm --dir bellows-ts test`. Neither command prepares Rust or requires a TypeScript `dist` prebuild. Root `pnpm test` is the aggregate command: it runs all three test packages serially, including the Rust and mixed suites, and therefore needs their Rust prerequisites. The aggregate covers all 34 Cloudflare cases once: five TypeScript, 19 Rust, and ten mixed.

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

- `POST /tasks` accepts a non-blank `name` of at most 200 UTF-16 code units. Publication commits and its connection closes before dispatch. HTTP **202** returns the task ID, not completion.
- Generic dispatch forwards `{ taskId: string }`, accepting opaque IDs of 1–200 UTF-16 code units. The example processor requires a canonical positive decimal ID no greater than `9007199254740991`.
- `{ taskId, attemptFinished: true }` means an attempt ended, including no claim or handled failure. A persisted side effect plus deletion of the Bellows row establishes success.
- If dispatch fails after publication, HTTP **503** includes the existing ID. Redispatch that ID through a trusted path; publishing again creates another task.
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
6. Submit one task and verify within a bounded window that `processed_tasks` has the expected payload and `execution_count = 1`, and that its Bellows task row is gone. Acceptance alone is insufficient.
7. Delete only the disposable deployments, Hyperdrive configuration, and database/schema; remove private configuration and credentials.

This checks hosted connectivity and task behavior—not pooling performance, cache correctness, durable recovery, or exactly-once execution.
