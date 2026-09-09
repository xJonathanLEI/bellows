<p align="center">
  <h1 align="center">bellows</h1>
</p>

<p align="center">
  <a href="https://crates.io/crates/bellows"><img alt="crates-badge" src="https://img.shields.io/crates/v/bellows.svg"></a>
  <a href="https://www.npmjs.com/package/@xjonathanlei/bellows"><img alt="crates-badge" src="https://img.shields.io/npm/v/@xjonathanlei/bellows"></a>
</p>

<p align="center">
  <strong>Durable task processing framework in Rust and TypeScript for applications of all sizes</strong>
</p>

## Introduction

`bellows` is a durable task processing framework with fully compatible implementations in Rust and TypeScript for building heterogeneous systems.

## Usage

```rust
// Define the task

struct SendWelcomeEmailTask;

#[derive(Debug, Serialize, Deserialize)]
struct SendWelcomeEmailPayload {
    email: String,
}

impl TaskDefinition for SendWelcomeEmailTask {
    const NAME: &str = "send_welcome_email";

    type Callback = String;
    type Trigger = PublishTrigger<SendWelcomeEmailPayload>;
}

// Define the processing logic

struct SendWelcomeEmailWorker;

impl Worker for SendWelcomeEmailWorker {
    type Task = SendWelcomeEmailTask;

    async fn process(
        self,
        _task_id: u64,
        task_payload: SendWelcomeEmailPayload,
    ) -> TaskResult<String> {
        // ...
    }
}

// Invoke the task from anywhere and optionally wait for the result

let backend = PostgresBackend::connect(DATABASE_URL).await?;
let awaitable = backend
    .publish_awaitable::<SendWelcomeEmailTask>(SendWelcomeEmailPayload {
        email: "alice@example.com".to_owned(),
    })
    .await?;
let result = awaitable.wait().await?;
println!("{result}");
```

## Request-driven execution

When an external host already knows which task to attempt, use `run_task_once` without launching a `WorkerDispatcher`. It accepts any `TaskExecutionBackend`, including all existing full backends:

```rust
use bellows::{
    Backend, PublishDispatchToken, PublishTrigger, TaskDefinition, TaskResult, TaskSuccess,
    Worker, WorkerFactory, backends::in_memory::InMemoryBackend, run_task_once,
};

struct EchoTask;

impl TaskDefinition for EchoTask {
    const NAME: &str = "echo";
    type Callback = ();
    type Trigger = PublishTrigger<String>;
}

struct EchoWorker;

impl Worker for EchoWorker {
    type Task = EchoTask;

    async fn process(self, task_id: u64, payload: String) -> TaskResult<()> {
        println!("{task_id}: {payload}");
        Ok(TaskSuccess::done(()))
    }
}

struct EchoFactory;

impl WorkerFactory for EchoFactory {
    type Worker = EchoWorker;

    fn build(&self, _worker_id: u64) -> EchoWorker {
        EchoWorker
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = InMemoryBackend::new();
    let task = backend.publish::<EchoTask>("Alice".to_owned()).await?;
    run_task_once(
        backend,
        EchoFactory,
        17,
        PublishDispatchToken::Task(task.task_id),
    )
    .await;
    Ok(())
}
```

## Built-in backends

`bellows` currently ships with:

- an in-memory backend for lightweight testing;
- a SQLite backend for durable local development and single-process deployment scenarios; and
- a Postgres backend for durable multi-process and distributed deployment scenarios.

### SQLite signaling model

> [!IMPORTANT]
>
> The SQLite backend **ONLY** supports single-process deployment.

The SQLite backend persists tasks durably, but SQLite does not provide a native notification mechanism that can wake dispatchers in other processes.

Because of that, the built-in SQLite backend uses an in-process signal channel. Clones of the same `SqliteBackend` instance receive new-task notifications immediately, but separate processes sharing the same database file do not.

That means the SQLite backend is appropriate for local development, tests, and same-process worker setups, but it should not be treated as a distributed production backend.

A planned future extension will allow the in-memory signaling to be swapped out to something like Redis to support the multi-process deployment model.

### Postgres signaling model

The Postgres backend uses native `LISTEN`/`NOTIFY` signaling. Task inserts trigger `pg_notify`, and dispatchers subscribe through a dedicated listener connection.

Because the signaling is provided by Postgres itself, this backend works naturally across multiple worker processes and across multiple machines, as long as they can all reach the same Postgres database.

This makes the Postgres backend the built-in option intended for durable distributed deployments, while SQLite remains the lightweight single-process durable option.

## Cloudflare Workers (Rust and TypeScript)

Both languages can implement the **producer Worker -> retained Durable Object dispatcher -> service-bound processor Worker** topology. PostgreSQL stores the tasks; the Durable Object only retains outstanding dispatch requests **in memory**. The processor claims a task, executes a Bellows worker using the claimed payload, renews ownership while processing, and awaits failure/completion recording.

- [Rust example and harness](./bellows/tests/integration/cloudflare/README.md)
- [TypeScript example and harness](./bellows-ts/test/integration/cloudflare/README.md)
- [Mixed-language harness and shared fixtures](./interop-tests/cloudflare/README.md)

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
