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

Use `dispatchTask` and `RetainedTaskDispatcher` from `@xjonathanlei/bellows/cloudflare` for dispatch. For a PostgreSQL processor, default-export `createPostgresProcessor` from the separate `@xjonathanlei/bellows/cloudflare/postgres` entry point. It accepts one published task definition and explicit task IDs; the annotated environment lets TypeScript infer the factory's payload and callback types.

```ts
import { definePublishTask, TaskSuccess } from "@xjonathanlei/bellows";
import { createPostgresProcessor } from "@xjonathanlei/bellows/cloudflare/postgres";

interface Env {
  HYPERDRIVE: { connectionString: string };
  BELLOWS_SCHEMA: string;
}

const greetingTask = definePublishTask<{ name: string }>("cloudflare_greeting");

export default createPostgresProcessor((env: Env) => ({
  connectionString: env.HYPERDRIVE.connectionString,
  schema: env.BELLOWS_SCHEMA,
  factory: {
    task: greetingTask,
    build() {
      return {
        async process(taskId, payload) {
          console.log(taskId, payload.name);
          return TaskSuccess.done(undefined);
        },
      };
    },
  },
}));
```

Configuration is synchronous and runs once per validated request, not at construction. Bellows generates the worker ID, acquires a fresh listener-free backend, and claims the task before building your worker. Only the database's claimed payload reaches `process`. An existing router can instead retain the delegate and return `processor.fetch(request, env)`; `fetch` does not depend on `this`.

The response waits for the runtime, optional `cleanup: () => Promise<void>`, and backend shutdown, even if application cleanup fails. Cleanup runs once whenever configuration returned, including acquisition failure and no claim. Applications still own arbitrary side-effect resources. Keep business connections separate and drain any tracked business promise in cleanup: lease loss does **not** cancel a pending TypeScript promise. The [SQL processor](./test/integration/cloudflare/workers/processor.ts) demonstrates this with a request-scoped operation and an awaited `pg.Client` shutdown.

HTTP **200** with `{ taskId, attemptFinished: true }` means an attempt ended, including no claim or handled failure, not business success. The adapter adds no retries or protection against abrupt request termination. PostgreSQL stores tasks; the retained Durable Object map is only in-memory dispatch state, not durable recovery. Publishing remains application-owned SQL in the example.

Keep the processor private and use the Hyperdrive connection string with query caching disabled and verified origin TLS. TypeScript requires `nodejs_compat` for `pg`; Rust must omit it. Do not construct the listening `PostgresBackend` in a Worker. Direct `PostgresExecutionBackend` from `@xjonathanlei/bellows/backends/postgres-execution` with `runTaskOnce()` remains the lower-level option for custom integrations, which must await their own backend shutdown. The generic `cloudflare` entry point does not load PostgreSQL or Node modules.

The [TypeScript Cloudflare–Postgres guide](./test/integration/cloudflare/README.md) exercises a producer Worker -> Durable Object dispatcher -> service-bound processor Worker with PostgreSQL publication, claims, side effects, and completion. `pnpm --dir bellows-ts test:cloudflare` runs five TypeScript -> TypeScript scenarios without Rust tools; they also run in the package's normal tests. The independent [Rust harness](../bellows/tests/integration/cloudflare/README.md) owns Rust -> Rust and Rust workerd contracts, while the [interop suite](../interop-tests/cloudflare/README.md) owns both mixed directions.

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
