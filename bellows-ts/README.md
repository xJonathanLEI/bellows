# bellows-ts

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
import {
  InMemoryBackend,
  WorkerDispatcher,
  definePublishTask,
  type WorkerFactory,
} from "bellows-ts";

const echoTask = definePublishTask<{ name: string }>("echo");
const backend = new InMemoryBackend();

const factory: WorkerFactory<typeof echoTask> = {
  task: echoTask,
  build() {
    return {
      async process(taskId, payload) {
        console.log(taskId, payload.name);
      },
    };
  },
};

const dispatcher = new WorkerDispatcher(backend, factory);
const handle = await dispatcher.launch();

await backend.publish(echoTask, { name: "Alice" });
await handle.drain();
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

Durable storage with `LISTEN` / `NOTIFY` signaling.

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
