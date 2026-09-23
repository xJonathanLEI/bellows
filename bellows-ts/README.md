# @xjonathanlei/bellows

TypeScript port of `bellows`, a durable task processing framework.

## What it provides

- task definitions with serializable payloads
- a worker/dispatcher runtime
- backends for:
  - in-memory
  - SQLite
  - Postgres

## Quick example

```ts
import { InMemoryBackend } from "@xjonathanlei/bellows/backends/in-memory";
import {
  TaskSuccess,
  WorkerDispatcher,
  definePublishTask,
  type WorkerFactory,
} from "@xjonathanlei/bellows";

const echoTask = definePublishTask<{ name: string }>("echo");
const backend = new InMemoryBackend();

const factory: WorkerFactory<typeof echoTask> = {
  task: echoTask,
  build() {
    return {
      async process(taskId, payload) {
        console.log(taskId, payload.name);
        return TaskSuccess.done(undefined);
      },
    };
  },
};

const dispatcher = new WorkerDispatcher(backend, factory);
const handle = await dispatcher.launch();

await backend.publish(echoTask, { name: "Alice" });
await handle.drain();
```

## Request-driven execution

Use `runTaskOnce()` when an external host triggers a task attempt instead of launching a `WorkerDispatcher`. It accepts the smaller `TaskExecutionBackend` contract; all existing full backends work too.

The returned `TaskAttemptOutcome` is a scheduling instruction, not business success: `{ type: "done" }` means no further published-task invocation is needed, `{ type: "retryAt", availableFromMs }` carries an observed availability/lease deadline or a committed reschedule, and `{ type: "retry" }` requires the host's infrastructure retry policy. Failure without a deadline and successful singleton completion request an immediate recheck; successful published-task completion without rescheduling returns `done`. Each call still attempts only once, and lease loss does not cancel arbitrary business promises.

```ts
import { InMemoryBackend } from "@xjonathanlei/bellows/backends/in-memory";
import {
  definePublishTask,
  runTaskOnce,
  TaskSuccess,
  type PublishDispatchToken,
  type WorkerFactory,
} from "@xjonathanlei/bellows";

const echoTask = definePublishTask<{ name: string }>("echo");
const backend = new InMemoryBackend();
const factory: WorkerFactory<typeof echoTask> = {
  task: echoTask,
  build(workerId) {
    return {
      async process(taskId, payload) {
        console.log(workerId, taskId, payload.name);
        return TaskSuccess.done(undefined);
      },
    };
  },
};

const task = await backend.publish(echoTask, { name: "Alice" });
const token: PublishDispatchToken = { type: "task", taskId: task.taskId };
await runTaskOnce(backend, factory, 17, token);
```

### Cloudflare Workers

Use `createPostgresPublisher` and `createPostgresProcessor` from `@xjonathanlei/bellows/cloudflare/postgres` for the producer Worker -> retained Durable Object dispatcher -> service-bound processor Worker topology. Keep one `RetainedTaskDispatcher` from `@xjonathanlei/bellows/cloudflare` per Durable Object.

#### Publisher

Bind a publisher to one task and its dispatcher. Your application still owns authentication, routing, JSON decoding, business validation, and HTTP responses. This example handler receives a decoded name from your router; the [complete producer](./test/integration/cloudflare/workers/producer.ts) also demonstrates request validation and the Durable Object wrapper.

```ts
import { definePublishTask } from "@xjonathanlei/bellows";
import type { DurableObjectNamespaceLike } from "@xjonathanlei/bellows/cloudflare";
import {
  createPostgresPublisher,
  PostgresPublisherError,
} from "@xjonathanlei/bellows/cloudflare/postgres";

interface ProducerEnv {
  HYPERDRIVE: { connectionString: string };
  BELLOWS_SCHEMA: string;
  DISPATCHER: DurableObjectNamespaceLike;
}

const greetingTask = definePublishTask<{ name: string }>("cloudflare_greeting");
const publisher = createPostgresPublisher((env: ProducerEnv) => ({
  connectionString: env.HYPERDRIVE.connectionString,
  schema: env.BELLOWS_SCHEMA,
  task: greetingTask,
  dispatcher: env.DISPATCHER,
}));

export async function queueGreeting(
  env: ProducerEnv,
  name: string,
): Promise<Response> {
  if (!name.trim() || name.length > 200) {
    return new Response("Invalid name", { status: 400 });
  }
  try {
    const receipt = await publisher.publish(env, { name });
    return new Response(receipt.taskId, {
      status: 202,
      headers: {
        "content-type": "text/plain; charset=utf-8",
        "cache-control": "no-store",
      },
    });
  } catch (error) {
    if (!(error instanceof PostgresPublisherError)) throw error;
    return Response.json(
      error.receipt
        ? {
            error: "task published, but dispatch acceptance was not confirmed",
            taskId: error.receipt.taskId,
          }
        : { error: "task publication failed" },
      { status: 503 },
    );
  }
}
```

The annotated synchronous callback infers environment and payload types and runs once per publication, not at construction. The delegate can be reused; connections, receipts, and failures are local to each call. Both `publish` and `publishFuture` work detached from the returned object. Callback-bearing definitions publish without callback registration or an awaitable result, and singleton definitions are rejected.

For future work, await `publisher.publishFuture(env, { name }, Date.now() + 60_000)`. The third argument is absolute Unix milliseconds. Availability reaches PostgreSQL, but dispatch happens immediately: the initial processor attempt asks PostgreSQL when the task can run, and the Durable Object retains that hint for a later automatic alarm. Both dispatch hops still carry only `{ taskId, taskName }`, never payloads or scheduling metadata.

Bellows acquires a fresh listener-free `PostgresPublishingBackend`, publishes exactly once, retains the string ID, validates it, awaits `close()`, then calls `dispatchTask` for object `global` with the published definition's exact name. Success waits for complete response-body consumption and returns readonly `PostgresPublisherReceipt.taskId: string`. It confirms **durable dispatch acceptance, not processing or business success**; receipts remain ID-only for future publication too.

Both PostgreSQL processors require canonical positive decimal IDs up to `9007199254740991`. Lower-level TypeScript PostgreSQL receipts remain `{ taskId: number }` for exact safe integers. Their `PostgresPublishedTaskIdError` preserves an unsupported ID as an exact string; the adapter turns it into a `task-id` failure without rounding or deleting the row. Rust's lower-level receipts remain exact `u64` values.

`PostgresPublisherError` exposes `stage`, `cause`, optional `receipt`, and optional `backendCloseError: { cause: unknown }`. Stages are `configuration`, `acquisition`, `publication`, `task-id`, `backend-close`, and `dispatch`. The first failure stays primary even if closing later fails; arbitrary thrown values, including `undefined`, remain failures. A connection failure on the first pool query can be a `publication` failure; stages identify adapter operations rather than driver network categories.

- A close or dispatch error with a receipt means publication is known but acceptance is unconfirmed, not necessarily rejected. Receipts remain ID-only: explicitly call `dispatchTask(env.DISPATCHER, greetingTask.name, error.receipt.taskId)` with the original published definition through a trusted recovery path instead of republishing.
- A `task-id` receipt is exact but **not supported by the current processor**. It requires another recovery action, not ordinary redispatch.
- A publication error without a receipt does not prove rollback. Do not automatically retry publication.

Top-level messages are sanitized and the adapter does not log or generate HTTP responses. Causes may contain credentials or response bodies; inspect them deliberately, never expose them in public responses. Always await `publish` within the request. Normal error paths await shutdown after acquisition, but abrupt termination cannot guarantee cleanup. The operation does not extend request lifetime or own arbitrary business clients or application cleanup hooks.

Publication and dispatch are not atomic: durable dispatcher tracking starts when a scheduling hint is persisted, not at acceptance. Bellows does not recover earlier loss after object termination. There is no outbox, automatic republishing, callback-delivery addition, or application-transaction participation. Direct `PostgresPublishingBackend` plus `dispatchTask` remains available for caller-managed integrations.

#### Processor

Default-export `createPostgresProcessor` with typed task registrations to route multiple published definitions through one Worker. The annotated configuration callback infers the environment.

```ts
import { definePublishTask, TaskSuccess } from "@xjonathanlei/bellows";
import {
  createPostgresProcessor,
  createPostgresProcessorTask,
} from "@xjonathanlei/bellows/cloudflare/postgres";

interface Env {
  HYPERDRIVE: { connectionString: string };
  BELLOWS_SCHEMA: string;
}

const greetingTask = definePublishTask<{ name: string }>("cloudflare_greeting");
const fullNameTask = definePublishTask<{ firstName: string; lastName: string }>(
  "cloudflare_full_name",
);

export default createPostgresProcessor((env: Env) => ({
  connectionString: env.HYPERDRIVE.connectionString,
  schema: env.BELLOWS_SCHEMA,
  tasks: [
    createPostgresProcessorTask({
      task: greetingTask,
      build: () => ({
        async process(taskId, payload) {
          console.log(taskId, payload.name);
          return TaskSuccess.done(undefined);
        },
      }),
    }),
    createPostgresProcessorTask({
      task: fullNameTask,
      build: () => ({
        async process(taskId, payload) {
          console.log(taskId, `${payload.firstName} ${payload.lastName}`);
          return TaskSuccess.done(undefined);
        },
      }),
    }),
  ],
}));
```

Both dispatch hops require `{ taskId, taskName }`, not a payload. Registrations must be non-empty with unique, non-empty definition names, matched exactly. See the [shared protocol](./test/integration/cloudflare/README.md#protocol-and-limits) for validation and HTTP responses.

Configuration is synchronous and runs once per validated request, not at construction. Bellows generates the worker ID, acquires a fresh listener-free backend, and claims by **ID and persisted definition name** before building your worker. Only the database's claimed payload reaches `process`. An existing router can instead retain the delegate and return `processor.fetch(request, env)`; `fetch` does not depend on `this`.

The response waits for the runtime, optional `cleanup: () => Promise<void>`, and backend shutdown, even if application cleanup fails. Cleanup runs once whenever configuration returned, including invalid registries, unknown names, acquisition failure, and no claim. Applications still own arbitrary side-effect resources. Keep business connections separate and drain any tracked business promise in cleanup: lease loss does **not** cancel a pending TypeScript promise. The [SQL processor](./test/integration/cloudflare/workers/processor.ts) demonstrates this with a request-scoped operation and an awaited `pg.Client` shutdown.

HTTP **200** reports `{ taskId, nextAction: { type: "done" } }` for an absent matching task or committed completion, or `{ taskId, nextAction: { type: "retryAt", atMs } }` for observed availability/leases and committed retries or self-rescheduling. `atMs` is a finite, non-negative integer Unix millisecond timestamp within JavaScript's safe-integer and Date ranges; valid past deadlines request a prompt asynchronous recheck. This describes a known next action, not business success. Runtime uncertainty and acquisition, cleanup, or backend-close failures return sanitized **500** responses; diagnostics contain only the validated ID and lifecycle stage. Each processor request attempts once; the dispatcher schedules later requests.

Keep the processor private, initialize schemas separately through an administrative connection, and use the Hyperdrive connection string with query caching disabled and verified origin TLS. TypeScript requires `nodejs_compat` for `pg`; Rust must omit it. Do not construct the listening `PostgresBackend` in a Worker. Direct `PostgresExecutionBackend` from `@xjonathanlei/bellows/backends/postgres-execution` with `runTaskOnce()` remains the lower-level option for custom integrations, which must await their own backend shutdown. Generic root/`cloudflare` imports do not load PostgreSQL or Node modules; the PostgreSQL subpath retains its `pg` compatibility requirements without loading the full listening backend or Bellows's Node randomness module.

#### Durable dispatcher

Keep one `RetainedTaskDispatcher` per SQLite-backed Durable Object and use the named object `global`. External dispatch launches with in-memory tracking before checking the warming alarm, without task writes, schedule scans, or durable attempt allocation. It sets the alarm only when absent or later than `now + 30 seconds`, preserving earlier and overdue alarms. Conditional alarm writes and unrelated writes on this object retain Cloudflare's normal output gating. Active same-ID requests are deduplicated across names until the full response and result handling finish; active duplicates cannot change routing. Explicit redispatch of a pending ID can recheck immediately with a corrected name without acceptance-time task writes.

The single alarm selects the earliest task deadline or independent 30-second heartbeat; a running scheduled task contributes its 60-second watchdog. The heartbeat keeps the DO warm even while idle. Alarms inspect persisted state and launch all due distinct IDs without a Bellows concurrency cap, batching delay, or serial processor queue, subject to platform limits. Saved schedules survive delegate reconstruction. Watchdog supersession retries interrupted/hung scheduled attempts and ignores stale responses; best-effort transport cancellation does not cancel arbitrary business promises.

Only a fully consumed, successful, matching-ID `done` response removes tracking; unsaved completion performs no writes. Any valid `retryAt`, including a past deadline, starts durable tracking and resets infrastructure backoff. Uncertain responses, transport/body failures, and non-success statuses retry with exponential backoff from one to thirty seconds: in memory for unsaved tasks, durably for saved schedules. Scheduled tasks remain persisted while running. Committed business retries bypass that backoff, including immediate failure. PostgreSQL decides claimability; an extended lease can produce a later hint instead of building a worker.

Alarms and attempts are at-least-once, not exactly-once side effects. Use idempotent business operations. Before a scheduling hint is persisted, loss of an invocation or its response needs explicit redispatch or application recovery. There is no PostgreSQL discovery or Cron, and abrupt termination cannot guarantee async cleanup. See the [shared scheduling guarantees](../README.md#durable-scheduling-and-limits).

The [TypeScript Cloudflare–Postgres guide](./test/integration/cloudflare/README.md) exercises future publication, lease hints, business retries, successful self-rescheduling, response loss, and automatic alarms through real workerd/Hyperdrive/PostgreSQL. `pnpm --dir bellows-ts test:cloudflare` runs its topology and direct contracts without Rust tools; they also run in normal package tests. The independent [Rust harness](../bellows/tests/integration/cloudflare/README.md) owns Rust topology/SDK contracts, while the [interop suite](../interop-tests/cloudflare/README.md) runs both mixed directions. Root `pnpm test` runs all three packages serially, registering each scenario once per topology.

## Tasks

Use `definePublishTask()` for payload-carrying tasks:

```ts
const task = definePublishTask<{ name: string }>("echo");
```

Use `defineSingletonTask()` for singleton work:

```ts
const task = defineSingletonTask("singleton_echo");
```

## Backends

`TaskPublishingBackend` exposes only `publish` and `publishFuture`; `TaskExecutionBackend` exposes claim, renewal, failure, and completion. `Backend` extends both with subscription and awaitable publication. All full backends implement both capabilities. Plain publication accepts callback-bearing definitions without registering a callback; singleton definitions remain unpublishable.

### `PostgresPublishingBackend`

For producer-only applications, import the listener-free backend from its dedicated subpath. Create the schema and initialize its tables separately with `initializePostgresSchema` from `@xjonathanlei/bellows/backends/postgres` through an administrative connection. The publisher exposes no initialization, execution, subscription, or awaitable API.

This example assumes an initialized `bellows` schema and a `DATABASE_URL` environment variable:

```ts
import {
  definePublishTask,
  type TaskPublishingBackend,
} from "@xjonathanlei/bellows";
import { PostgresPublishingBackend } from "@xjonathanlei/bellows/backends/postgres-publishing";

const welcome = definePublishTask<string, string>("send_welcome_email");

async function publishWelcome(backend: TaskPublishingBackend) {
  const now = await backend.publish(welcome, "alice@example.com");
  const later = await backend.publishFuture(
    welcome,
    "bob@example.com",
    Date.now() + 60_000,
  );
  return [now, later];
}

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error("DATABASE_URL is required");
const backend = await PostgresPublishingBackend.connect(databaseUrl, {
  schema: "bellows",
});
try {
  console.log(await publishWelcome(backend));
} finally {
  await backend.close();
}
```

#### Connection ownership

| Construction                                                | Resource ownership                                                                                                                                                                                  |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `await PostgresPublishingBackend.connect(url, options)`     | Bellows creates and owns a `pg.Pool`. Await `close()` once, including on error paths.                                                                                                               |
| `PostgresPublishingBackend.fromExecutor(executor, options)` | Synchronous, query-only construction: no pool creation, connection acquisition, or initialization. The caller owns all resources. `close()` is a repeatable no-op and does not disable publication. |

`Pool`, `Client`, and `PoolClient` from `pg` satisfy the executor interface directly. For connection reuse, `PostgresPublishingBackend.fromExecutor(pool, { schema: "bellows" })` works with an existing pool and supports both calls in `publishWelcome` above. Sharing a pool is **not** transaction atomicity; each pool query may use a different connection. Bellows never begins, commits, rolls back, releases, or closes caller executors, even after validation/publication failure.

On Workers, use `env.HYPERDRIVE.connectionString` within each request with `nodejs_compat`. Await owned backend shutdown before returning a response; external executors need caller cleanup. Never retain connections across requests. The package root and generic Cloudflare entry point do not load PostgreSQL. For immediate publication followed by dispatch with an owned connection, [`createPostgresPublisher`](#publisher) retains its request-scoped lifecycle; it does not accept application transactions.

#### Caller-managed transactions

Business mutations and task inserts are atomic only when they use the **same transaction-bound executor**. Using `welcome` above and an existing application table `accounts(email TEXT NOT NULL)`, this function returns only after caller commit and client release:

```ts
import type { Pool } from "pg";

async function createAccount(pool: Pool, email: string) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const publisher = PostgresPublishingBackend.fromExecutor(client, {
      schema: "bellows",
    });
    await client.query("INSERT INTO accounts (email) VALUES ($1)", [email]);
    const receipt = await publisher.publish(welcome, email);
    // client is still ours; further transaction queries can go here.
    await client.query("COMMIT");
    return receipt;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
```

Keep the transaction-backed publisher inside the transaction's scope. `publishFuture` uses that same transaction but records availability only. No `publisher.close()` is needed: it would neither commit nor release `client`.

If you also need explicit Cloudflare dispatch, use your caller-owned `pool` and dispatcher binding **after** `createAccount` resolves:

```ts
import { dispatchTask } from "@xjonathanlei/bellows/cloudflare";

const receipt = await createAccount(pool, "alice@example.com");
await dispatchTask(dispatcher, welcome.name, String(receipt.taskId));
```

Dispatch is deliberately outside the transaction's error/rollback block: dispatch failure after commit must not trigger a misleading rollback attempt. Retain `receipt` for recovery with the original task name and ID instead of republishing.

#### ORM adapters

The publishing subpath exports `PostgresPublishingExecutor` and `PostgresPublishParameters`. An adapter forwards parameterized SQL to its transaction and normalizes the result shape; it needs no `pg.QueryResult`, pool lifecycle methods, or ORM dependency. For an illustrative transaction API returning `records` rather than `rows`:

```ts
import type {
  PostgresPublishingExecutor,
  PostgresPublishParameters,
} from "@xjonathanlei/bellows/backends/postgres-publishing";

interface OrmTransaction {
  execute(
    sql: string,
    parameters: PostgresPublishParameters,
  ): Promise<{ records: { task_id: string }[] }>;
}

function publicationExecutor(
  transaction: OrmTransaction,
): PostgresPublishingExecutor {
  return {
    async query(sql, parameters) {
      const result = await transaction.execute(sql, parameters);
      return { rows: result.records };
    },
  };
}
```

Inside your ORM transaction callback, use `PostgresPublishingBackend.fromExecutor(publicationExecutor(transaction), options)` and perform business mutations through that very transaction. Never fall back to a root ORM client or pool. Await the **outer transaction operation**, including commit, before dispatching. Do not retain a transaction-backed publisher outside that scope.

Execute Bellows's generated insert once; do not reconstruct it or interpolate application values. Parameters are `$1` task name, `$2` already-encoded JSON text, `$3` callback ID or `null`, and `$4` availability in Unix milliseconds or `null`. Return the exact textual ID from `RETURNING task_id::text AS task_id`; converting through `number` can irreversibly lose it. Bellows calls `query` on its receiver, so adapters may use `this`.

Explicit schema options are validated and qualify the task table. Omitting `schema` uses the executor's current search path. Neither external construction nor publication initializes tables, issues `SET search_path`, or falls back to `public`.

#### Receipts, notifications, and dispatch

Receipts inside a transaction are **provisional**: caller rollback or a later commit failure can leave no committed task. Both PostgreSQL publishing implementations return `{ taskId: number }` only for exact safe integers, including `Number.MAX_SAFE_INTEGER`. Otherwise they throw the same `PostgresPublishedTaskIdError`, exported from both `backends/postgres-publishing` and `backends/postgres`, with the exact ID in readonly `taskId: string`. That row may have committed in standalone publication or may still be pending in a caller transaction; receipt validation does not undo the insert or finalize the transaction. Inspect the retained ID instead of automatically republishing. A database/transport exception after sending SQL does not establish rollback.

Existing PostgreSQL trigger notifications are delivered only after commit, and not after rollback; listener-free is not notification-free. Explicit Durable Object dispatch is separate and cannot undo a committed publication if it fails. The Cloudflare processor accepts only canonical positive IDs up to `9007199254740991`; an exact ID outside that range requires another recovery path, not ordinary redispatch.

No outbox, durable dispatch guarantee, automatic retry, savepoint, or transaction finalization is added. Future publication records availability, not a scheduler or a future Worker request. Plain publication supports callback-bearing definitions without callback registration, singleton publication remains unavailable, and awaitable publication still needs the full backend's listener.

### `PostgresExecutionBackend`

Use `@xjonathanlei/bellows/backends/postgres-execution` with `runTaskOnce` for execution without publishing or subscriptions. You own acquisition and awaited `close()`; the processor delegate manages this lifecycle for its requests.

### `InMemoryBackend`

Good for tests and local development.

### `SqliteBackend`

Durable storage for single-process setups.

```ts
const backend = await SqliteBackend.connect("sqlite:///tmp/bellows.sqlite");
await backend.initialize();
```

### `PostgresBackend`

Durable storage with `LISTEN` / `NOTIFY` signaling for normal `WorkerDispatcher` daemon processing.
Use [`PostgresPublishingBackend`](#postgrespublishingbackend) instead when you only need plain publication.

```ts
const backend = await PostgresBackend.connect(
  "postgres://postgres:postgres@localhost:5432/postgres",
);
await backend.initialize();
```

## Testing

Run the test suite:

```sh
pnpm test
```
