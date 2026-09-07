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
