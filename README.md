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
    PublishDispatchToken, PublishTrigger, TaskDefinition, TaskPublishingBackend, TaskResult, TaskSuccess,
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

Backend capabilities are separate in both languages:

| Capability              | API                                                             |
| ----------------------- | --------------------------------------------------------------- |
| `TaskPublishingBackend` | Immediate and future publication, returning a task receipt.     |
| `TaskExecutionBackend`  | Claim, renew, fail, and finish tasks.                           |
| `Backend`               | Both capabilities, plus subscription and awaitable publication. |

Plain publication supports callback-bearing definitions without registering a callback; singleton definitions are not publishable. Awaitable publication remains on the full backend because callback delivery needs its signal channel (a PostgreSQL listener). In Rust, import `TaskPublishingBackend` for concrete `publish` / `publish_future` calls, even on a full backend.

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

### Publishing without a listener

Use `PostgresPublishingBackend` when your producer only needs typed publication. It exposes no initialization, execution, subscription, or awaitable API. Create the schema and initialize tables separately with [`initialize_postgres_schema`](./bellows/src/backends/postgres.rs) through an administrative connection. This example assumes the existing `bellows` schema is initialized:

```rust
use bellows::{
    PublishTrigger, TaskDefinition, TaskPublishingBackend,
    backends::{
        PublishTaskError,
        postgres_publishing::{PostgresBackendOptions, PostgresPublishingBackend},
    },
    time::Instant,
};
use std::time::Duration;

struct Welcome;
impl TaskDefinition for Welcome {
    const NAME: &str = "send_welcome_email";
    type Callback = String;
    type Trigger = PublishTrigger<String>;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = PostgresPublishingBackend::connect_with_options(
        &std::env::var("DATABASE_URL")?,
        PostgresBackendOptions {
            schema: Some("bellows".into()),
        },
    )
    .await?;
    let publication = async {
        let now = backend.publish::<Welcome>("alice@example.com".into()).await?;
        let later = backend
            .publish_future::<Welcome>(
                "bob@example.com".into(),
                Instant::now() + Duration::from_secs(60),
            )
            .await?;
        Ok::<_, PublishTaskError>((now, later))
    }
    .await;
    // Await shutdown even when publication failed.
    backend.close().await?;
    let (now, later) = publication?;
    println!("Published {} and {}", now.task_id, later.task_id);
    Ok(())
}
```

#### Connection ownership

| Publication path                                                      | Resource ownership                                                                                                                                                            |
| --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connect` / `connect_with_options`, then `publish` / `publish_future` | Bellows owns a native SQLx pool or a Workers connection and driver. Await `close()`; closing any clone closes the shared resources.                                           |
| `publish_with_executor` / `publish_future_with_executor`              | Each associated function borrows a caller executor for one operation, without constructing a backend. The caller alone manages its transaction, connection, pool, and driver. |

On Workers, enable only `cloudflare` and use request-scoped Hyperdrive connections. Await owned backend shutdown on success and error paths before responding; dropping clones does not close the driver. Caller-owned `tokio_postgres::Client` and `tokio_postgres::Transaction` adapters leave driver cleanup to the caller. Never retain connections across requests. See the [TypeScript equivalent](./bellows-ts/README.md#postgrespublishingbackend) for `fromExecutor()` and its no-op external `close()`.

#### Caller-managed transactions

To make business mutations and task inserts atomic, execute both through the **same transaction**. Native executors support SQLx `PgPool`, `&PgPool`, `PgConnection`, and `Transaction<'_, Postgres>`. Using `Welcome` from above and an existing application table `accounts(email TEXT NOT NULL)`:

```rust
async fn create_account(
    pool: &sqlx::PgPool,
    email: &str,
) -> Result<bellows::backends::PublishedTask, bellows::backends::BoxBackendError> {
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO accounts (email) VALUES ($1)")
        .bind(email)
        .execute(&mut *transaction)
        .await?;

    let receipt = PostgresPublishingBackend::publish_with_executor::<Welcome>(
        &mut transaction,
        email.to_owned(),
        PostgresBackendOptions {
            schema: Some("bellows".into()),
        },
    )
    .await?;
    // The borrow has ended; the caller can query, commit, or roll back the transaction.
    transaction.commit().await?;

    // Return the receipt for optional caller dispatch only after commit succeeds.
    Ok(receipt)
}
```

On an early return, dropping the transaction asks SQLx to roll it back; use `transaction.rollback().await` when you need to await rollback explicitly.

With the `cloudflare` feature enabled, optional explicit dispatch belongs at the call site after `create_account` resolves, using your caller-owned `pool` and dispatcher `namespace`:

```rust
use bellows::cloudflare::dispatch_task;

let receipt = create_account(pool, "alice@example.com").await?;
dispatch_task(namespace, Welcome::NAME, &receipt.task_id.to_string()).await?;
```

Retain `receipt` if dispatch fails; the transaction has already committed. On Workers, use the same commit boundary with a `tokio_postgres` transaction and your `worker::ObjectNamespace`. Do not dispatch while publication is still transaction-local.

A shared external pool is useful for connection reuse but does **not** join business mutations into one transaction. For example, with `pool: &sqlx::PgPool`, future publication borrows a shared handle without closing the pool:

```rust
let mut executor = pool;
let receipt = PostgresPublishingBackend::publish_future_with_executor::<Welcome>(
    &mut executor,
    "bob@example.com".into(),
    Instant::now() + Duration::from_secs(60),
    PostgresBackendOptions {
        schema: Some("bellows".into()),
    },
)
.await?;
```

For an ORM, implement `PostgresPublishingExecutor::query_task_id` by forwarding the supplied `PostgresPublishQuery.sql` once to the borrowed transaction. Bind `$1` to `task_name`, `$2` to the already-encoded `payload_json` text, `$3` to `callback_id`, and `$4` to `available_from_unix_ms`; normalize the returned `task_id` to the exact signed `i64` without a floating-point conversion. Return the original driver error as `BoxBackendError`. Do not reconstruct the insert, interpolate values, or fall back to the ORM's root client/pool. The [TypeScript adapter example](./bellows-ts/README.md#orm-adapters) illustrates the same query-only contract without an ORM dependency. If the ORM owns a transaction callback, await the **outer transaction operation**, including commit, before dispatching.

All paths validate explicit schema names and qualify the task table; with no schema option, they use the executor's existing search path. External publication does not initialize schemas/tables, issue `SET search_path`, or fall back to `public`.

#### Receipts, notifications, and dispatch

A receipt returned inside a transaction is **provisional**: the caller can roll back, and a later commit can fail. Receipt-validation errors can likewise refer to a row still pending in that transaction; they do not roll it back. Lower-level Rust receipts convert PostgreSQL's nonnegative signed `i64` IDs to exact `u64` values without a JavaScript safe-integer ceiling. TypeScript returns numeric receipts only for exact safe integers; otherwise `PostgresPublishedTaskIdError.taskId` retains the exact string ID, whether the insert committed in standalone publication or is still transaction-local. Retain exact IDs for recovery rather than blindly republishing; a database/transport exception after sending SQL is not proof of rollback.

PostgreSQL delivers the existing trigger notifications only after commit, and not after rollback; listener-free is not notification-free. Explicit Durable Object dispatch remains a separate caller action after successful commit. A post-commit dispatch failure does not undo publication: retain the receipt and recover dispatch using the original task name and ID instead of republishing. The Cloudflare processor accepts only canonical positive IDs up to `9007199254740991`; an exact ID outside that range requires a different recovery path.

There is no automatic retry, savepoint, transaction finalization, outbox, or durable dispatch guarantee. Future publication records availability only, not a scheduler or a future Worker request. Plain publication supports callback-bearing definitions without registering callbacks; singleton publication remains unavailable, and awaitable publication still requires the full backend. The high-level Cloudflare publisher below retains its owned publish/close/dispatch lifecycle and does not accept application transactions.

## Cloudflare Workers (Rust and TypeScript)

Both languages can implement the **producer Worker -> retained Durable Object dispatcher -> service-bound processor Worker** topology. One processor routes multiple published definitions by name, executing one task ID per request. PostgreSQL stores the tasks; the Durable Object only retains outstanding dispatch requests **in memory**, deduplicated by ID even across different names. The processor claims a task, executes a Bellows worker using the claimed payload, renews ownership while processing, and awaits failure/completion recording.

### Producer

Use `createPostgresPublisher` from `@xjonathanlei/bellows/cloudflare/postgres` in TypeScript or `bellows::cloudflare::sdk::PostgresPublisher` in Rust. Bind one published task and its dispatcher; your application owns authentication, routing, business validation, and HTTP responses. The delegate is a typed operation, not an HTTP endpoint.

For Rust, target `wasm32-unknown-unknown` with `default-features = false, features = ["cloudflare"]`. This helper accepts input already validated by your application handler, as in the [compiled producer](./bellows/tests/integration/cloudflare/producer/lib.rs):

```rust
use bellows::{
    PublishTrigger, TaskDefinition,
    backends::postgres_publishing::PostgresBackendOptions,
    cloudflare::sdk::{
        PostgresPublisher, PostgresPublisherConfig, PostgresPublisherError,
        PostgresPublisherReceipt,
    },
};
use serde::{Deserialize, Serialize};
use worker::Env;

pub struct GreetingTask;

#[derive(Serialize, Deserialize)]
pub struct GreetingPayload {
    pub name: String,
}

impl TaskDefinition for GreetingTask {
    const NAME: &str = "cloudflare_greeting";
    type Callback = ();
    type Trigger = PublishTrigger<GreetingPayload>;
}

pub async fn queue_greeting(
    env: &Env,
    payload: GreetingPayload,
) -> Result<PostgresPublisherReceipt, PostgresPublisherError> {
    let publisher = PostgresPublisher::<GreetingTask, _>::new(|env: &Env| {
        Ok(PostgresPublisherConfig::new(
            env.hyperdrive("HYPERDRIVE")?.connection_string(),
            PostgresBackendOptions {
                schema: Some(env.var("BELLOWS_SCHEMA")?.to_string()),
            },
            env.durable_object("DISPATCHER")?,
        ))
    });
    publisher.publish(env, payload).await
}
```

See the [typed TypeScript handler](./bellows-ts/README.md#publisher). Construction performs no I/O. Synchronous configuration runs once per publication, so a delegate can be reused without retaining connections. Each call acquires a fresh listener-free publishing backend, publishes once, retains the ID, validates it, awaits backend shutdown, then awaits the complete `dispatchTask` / `dispatch_task` response from object `global`, supplying the published definition's exact name. Callers must await the operation within the request; it does not extend request lifetime.

Success returns `PostgresPublisherReceipt` (`taskId: string` / `task_id: String`) and confirms **dispatch acceptance, not task completion or business success**. IDs must be canonical positive decimal strings no greater than `9007199254740991`. Callback-bearing definitions support plain publication only, without callback registration; singleton tasks are rejected. Only immediate publication is exposed.

`PostgresPublisherError` retains the first stage and cause, an optional exact receipt, and a separate later backend-close failure. Stages are `configuration`, `acquisition`, `publication`, `task-id`, `backend-close`, and `dispatch`. Top-level messages contain only the stage; underlying causes are available for deliberate inspection, not safe public responses or automatic logging.

- A close or dispatch failure with a receipt means the task was published but acceptance is unconfirmed, not necessarily rejected. Receipts remain ID-only: recover through a trusted path using `dispatch_task(namespace, Task::NAME, &receipt.task_id)` or `dispatchTask(namespace, task.name, receipt.taskId)` with the original definition, rather than publishing again.
- A `task-id` error also retains the exact committed ID, including TypeScript's otherwise unsafe PostgreSQL IDs. That ID is **unsupported by this processor** and needs a different recovery action, not blind redispatch.
- No receipt on a publication error is an unknown outcome, not proof of rollback. Never automatically republish.

Shutdown is awaited on ordinary error paths after acquisition; dropping/cancelling a Rust future, abrupt Worker termination, or a wasm trap has no async-finally guarantee.

### Processor and deployment

Use `createPostgresProcessor` from the same TypeScript subpath or `bellows::cloudflare::sdk::PostgresProcessor` in Rust. Supply a synchronous environment-to-config callback with the Hyperdrive connection string, optional schema, and typed task registrations. See the [Rust processor](./bellows/tests/integration/cloudflare/processor/lib.rs) for factory definitions and request-local cleanup:

```rust
use bellows::cloudflare::sdk::{PostgresProcessorConfig, PostgresProcessorTask};

let config = PostgresProcessorConfig::new(
    connection_string,
    options,
    vec![
        PostgresProcessorTask::new(greeting_factory),
        PostgresProcessorTask::new(full_name_factory),
    ],
).with_cleanup(cleanup);
```

See the [TypeScript registration example](./bellows-ts/README.md#processor).

Both dispatch hops require `{ taskId, taskName }`, not a payload. Registrations must be non-empty with unique, non-empty definition names, matched exactly. Claims check both ID and persisted name, preventing decoding with the wrong definition. See the [shared protocol](./bellows-ts/test/integration/cloudflare/README.md#protocol-and-limits) for validation and HTTP responses.

The delegate owns worker IDs and a fresh listener-free execution backend for each selected attempt. It awaits the runtime, registered application cleanup, and Bellows backend shutdown before responding. Cleanup runs once whenever configuration returned, including invalid registries and unknown names; acquired backends always close afterwards, even if cleanup fails. Applications still own their side-effect resources; use separate business connections and register cleanup for work that can outlive the runtime. TypeScript cleanup must drain outstanding business promises after lease loss; Rust cleanup must retain resource ownership outside the aborted worker.

HTTP **200** with `{ taskId, attemptFinished: true }` means an attempt ended, **not that the task succeeded**. It includes no-claim and handled-failure attempts. The adapter adds no automatic retries, durable dispatcher recovery, or protection against abrupt request termination.

Keep the processor private, initialize schemas separately through an administrative connection, and use Hyperdrive with query caching disabled and verified origin TLS. TypeScript requires `nodejs_compat` for `pg`; Rust must omit it. Generic TypeScript root/Cloudflare imports do not load PostgreSQL or Node modules; the PostgreSQL subpath still requires `pg` compatibility.

For caller-managed integrations, direct `PostgresPublishingBackend` plus `dispatchTask` / `dispatch_task`, or `PostgresExecutionBackend` plus `runTaskOnce` / `run_task_once`, remain available. Await your own backend shutdown; do not construct the listening `PostgresBackend` in a Worker. The retained dispatcher's 30-second heartbeat is not lease renewal, retry, or eviction recovery. This topology does not provide daemon-equivalent discovery or exactly-once side effects.

- [Rust example and harness](./bellows/tests/integration/cloudflare/README.md)
- [TypeScript example and harness](./bellows-ts/test/integration/cloudflare/README.md)
- [Mixed-language harness and shared fixtures](./interop-tests/cloudflare/README.md)

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
