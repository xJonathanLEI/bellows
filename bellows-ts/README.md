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

The annotated synchronous callback infers environment and payload types and runs once per publication, not at construction. The delegate can be reused; connections, receipts, and failures are local to each call. `publish` also works detached from the returned object. Its only operation is immediate publication; callback-bearing definitions publish without callback registration or an awaitable result, and singleton definitions are rejected.

Bellows acquires a fresh listener-free `PostgresPublishingBackend`, publishes exactly once, retains the string ID, validates it, awaits `close()`, then calls `dispatchTask` for object `global` with the published definition's exact name. Success waits for complete response-body consumption and returns readonly `PostgresPublisherReceipt.taskId: string`. It confirms **dispatch acceptance, not processing or business success**.

Both PostgreSQL processors require canonical positive decimal IDs up to `9007199254740991`. Lower-level TypeScript PostgreSQL receipts remain `{ taskId: number }` for exact safe integers. Their `PostgresPublishedTaskIdError` preserves an unsupported committed ID as a string; the adapter turns it into a `task-id` failure without rounding or deleting the row. Rust's lower-level receipts remain exact `u64` values.

`PostgresPublisherError` exposes `stage`, `cause`, optional `receipt`, and optional `backendCloseError: { cause: unknown }`. Stages are `configuration`, `acquisition`, `publication`, `task-id`, `backend-close`, and `dispatch`. The first failure stays primary even if closing later fails; arbitrary thrown values, including `undefined`, remain failures. A connection failure on the first pool query can be a `publication` failure; stages identify adapter operations rather than driver network categories.

- A close or dispatch error with a receipt means publication is known but acceptance is unconfirmed, not necessarily rejected. Receipts remain ID-only: explicitly call `dispatchTask(env.DISPATCHER, greetingTask.name, error.receipt.taskId)` with the original published definition through a trusted recovery path instead of republishing.
- A `task-id` receipt is exact but **not supported by the current processor**. It requires another recovery action, not ordinary redispatch.
- A publication error without a receipt does not prove rollback. Do not automatically retry publication.

Top-level messages are sanitized and the adapter does not log or generate HTTP responses. Causes may contain credentials or response bodies; inspect them deliberately, never expose them in public responses. Always await `publish` within the request. Normal error paths await shutdown after acquisition, but abrupt termination cannot guarantee cleanup. The operation does not extend request lifetime or own arbitrary business clients or application cleanup hooks.

Publication and dispatch are not atomic. There is no outbox, durable recovery, automatic republishing, future publication/request scheduling, batching, cancellation control, callback delivery, or application-transaction participation. Direct `PostgresPublishingBackend` plus `dispatchTask` remains available for caller-managed integrations; the backend's broader API does not expand the adapter's immediate-only contract.

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

HTTP **200** with `{ taskId, attemptFinished: true }` means an attempt ended, including no claim or handled failure, not business success. The adapter adds no retries or protection against abrupt request termination. PostgreSQL stores tasks; the retained Durable Object map is only in-memory dispatch state, deduplicated by ID even across different names, not durable recovery. Its 30-second heartbeat is not lease renewal, retry, or eviction recovery. This topology does not provide daemon-equivalent discovery or exactly-once side effects.

Keep the processor private, initialize schemas separately through an administrative connection, and use the Hyperdrive connection string with query caching disabled and verified origin TLS. TypeScript requires `nodejs_compat` for `pg`; Rust must omit it. Do not construct the listening `PostgresBackend` in a Worker. Direct `PostgresExecutionBackend` from `@xjonathanlei/bellows/backends/postgres-execution` with `runTaskOnce()` remains the lower-level option for custom integrations, which must await their own backend shutdown. Generic root/`cloudflare` imports do not load PostgreSQL or Node modules; the PostgreSQL subpath retains its `pg` compatibility requirements without loading the full listening backend or Bellows's Node randomness module.

The [TypeScript Cloudflare–Postgres guide](./test/integration/cloudflare/README.md) exercises heterogeneous publication, routing, claims, side effects, and completion. `pnpm --dir bellows-ts test:cloudflare` runs **19 cases** without Rust tools: thirteen topology scenarios, four direct publishing-backend contracts, and two publisher-adapter contracts. They also run in the package's normal tests. The independent [Rust harness](../bellows/tests/integration/cloudflare/README.md) owns **34 cases**, while the [interop suite](../interop-tests/cloudflare/README.md) owns **26** across both mixed directions. Root `pnpm test` runs all **79 Cloudflare cases** once.

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

The backend owns a `pg.Pool`; call and await `close()` once, including on error paths. On Workers, use `env.HYPERDRIVE.connectionString` inside each request with `nodejs_compat`, and await shutdown before returning the response. Never retain connections across requests. The package root and generic Cloudflare entry point do not load PostgreSQL.

For immediate Cloudflare publication followed by dispatch, prefer [`createPostgresPublisher`](#publisher), which owns this request-scoped lifecycle. Direct backend publication plus `dispatchTask` remains an escape hatch with caller-owned cleanup.

Publication stores a task and may emit a PostgreSQL notification; listener-free is not notification-free. Future publication records availability, not a scheduler or a future Worker request. It does not atomically dispatch to a Durable Object, join an application transaction, or retry publication. A database exception near commit does not establish that no row was written. Awaitable publication remains on the full backend because callback delivery requires its listener.

Both PostgreSQL publishing implementations return `{ taskId: number }` only for exact safe integers, including `Number.MAX_SAFE_INTEGER`. Otherwise they throw `PostgresPublishedTaskIdError`, exported from both `backends/postgres-publishing` and `backends/postgres`, with the committed row's exact ID in readonly `taskId: string`. This error does not undo publication; inspect the retained ID rather than automatically republishing. Other publication errors do not prove rollback.

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
