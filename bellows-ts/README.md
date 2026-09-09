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

Use `dispatchTask` and `RetainedTaskDispatcher` from `@xjonathanlei/bellows/cloudflare` for dispatch, and `PostgresExecutionBackend` from `@xjonathanlei/bellows/backends/postgres-execution` with `runTaskOnce()` for request-driven execution. PostgreSQL stores tasks; the retained Durable Object map is only in-memory dispatch state. Do not construct the listening `PostgresBackend` in a Worker.

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
