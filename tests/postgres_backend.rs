#![cfg(feature = "postgres")]

//! Integration tests for the Postgres backend.
//!
//! These tests require a Postgres server listening on `localhost:5432`.
//!
//! A quick local setup is:
//!
//! ```console
//! docker run --rm --name bellows-postgres-test \
//!     -e POSTGRES_USER=postgres \
//!     -e POSTGRES_PASSWORD=postgres \
//!     -e POSTGRES_DB=postgres \
//!     -p 5432:5432 \
//!     postgres:17
//! ```

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bellows::{
    Backend, PublishTrigger, SingletonTrigger, TaskDefinition, Worker, WorkerFactory,
    backends::postgres::PostgresBackend, dispatcher::WorkerDispatcher,
};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Executor, PgConnection};
use tokio::sync::{
    Semaphore,
    mpsc::{UnboundedReceiver as MpscReceiver, UnboundedSender as MpscSender},
};

struct EchoTaskSpec;

#[derive(Debug, Serialize, Deserialize)]
struct EchoTaskPayload {
    pub name: String,
}

impl TaskDefinition for EchoTaskSpec {
    const NAME: &str = "echo";

    type Callback = String;
    type Trigger = PublishTrigger<EchoTaskPayload>;
}

struct AckTaskSpec;

impl TaskDefinition for AckTaskSpec {
    const NAME: &str = "ack";

    type Callback = ();
    type Trigger = PublishTrigger<()>;
}

struct SingletonTaskSpec;

impl TaskDefinition for SingletonTaskSpec {
    const NAME: &str = "singleton_echo";

    type Callback = ();
    type Trigger = SingletonTrigger;
}

#[derive(Debug, PartialEq, Eq)]
struct ProcessedTask {
    task_id: u64,
    name: String,
}

struct EchoWorkerFactory {
    processed_tx: MpscSender<ProcessedTask>,
}

impl WorkerFactory for EchoWorkerFactory {
    type Worker = EchoWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        EchoWorker {
            processed_tx: self.processed_tx.clone(),
        }
    }
}

struct EchoWorker {
    processed_tx: MpscSender<ProcessedTask>,
}

impl Worker for EchoWorker {
    type Task = EchoTaskSpec;

    async fn process(self, task_id: u64, task_payload: EchoTaskPayload) -> String {
        self.processed_tx
            .send(ProcessedTask {
                task_id,
                name: task_payload.name.clone(),
            })
            .expect("processed task collector should remain available during the test");

        task_payload.name
    }
}

struct AckWorkerFactory {
    processed_tx: MpscSender<u64>,
}

impl WorkerFactory for AckWorkerFactory {
    type Worker = AckWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        AckWorker {
            processed_tx: self.processed_tx.clone(),
        }
    }
}

struct AckWorker {
    processed_tx: MpscSender<u64>,
}

impl Worker for AckWorker {
    type Task = AckTaskSpec;

    async fn process(self, task_id: u64, _task_payload: ()) {
        self.processed_tx
            .send(task_id)
            .expect("ack task collector should remain available during the test");
    }
}

struct SingletonWorkerFactory {
    processed_tx: MpscSender<u64>,
    release_signal: Arc<Semaphore>,
}

impl WorkerFactory for SingletonWorkerFactory {
    type Worker = SingletonWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        SingletonWorker {
            processed_tx: self.processed_tx.clone(),
            release_signal: self.release_signal.clone(),
        }
    }
}

struct SingletonWorker {
    processed_tx: MpscSender<u64>,
    release_signal: Arc<Semaphore>,
}

impl Worker for SingletonWorker {
    type Task = SingletonTaskSpec;

    async fn process(self, task_id: u64, _task_payload: ()) {
        self.processed_tx
            .send(task_id)
            .expect("processed task collector should remain available during the test");
        self.release_signal
            .acquire()
            .await
            .expect("singleton worker gate semaphore should remain available")
            .forget();
    }
}

#[tokio::test]
async fn test_postgres_backend() {
    let database = TestDatabase::new("backend").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();

    let factory = EchoWorkerFactory { processed_tx };
    let dispatcher = WorkerDispatcher::new(backend.clone(), factory);

    let dispatcher_handle = dispatcher.launch().await.unwrap();

    backend
        .publish::<EchoTaskSpec>(EchoTaskPayload {
            name: "Alice".to_string(),
        })
        .await
        .unwrap();
    backend
        .publish::<EchoTaskSpec>(EchoTaskPayload {
            name: "Bob".to_string(),
        })
        .await
        .unwrap();
    backend
        .publish::<EchoTaskSpec>(EchoTaskPayload {
            name: "Charlie".to_string(),
        })
        .await
        .unwrap();

    assert_names_echoed(&mut processed_rx, &["Alice", "Bob", "Charlie"]).await;

    dispatcher_handle.drain().await;

    assert!(processed_rx.recv().await.is_none());

    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_publish_awaitable_returns_typed_callback() {
    let database = TestDatabase::new("awaitable_string").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let dispatcher = WorkerDispatcher::new(backend.clone(), EchoWorkerFactory { processed_tx });
    let dispatcher_handle = dispatcher.launch().await.unwrap();

    let awaitable = backend
        .publish_awaitable::<EchoTaskSpec>(EchoTaskPayload {
            name: "Alice".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(awaitable.wait().await.unwrap(), "Alice");
    assert_eq!(processed_rx.recv().await.unwrap().name, "Alice");

    dispatcher_handle.drain().await;
    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_publish_awaitable_supports_unit_callback() {
    let database = TestDatabase::new("awaitable_unit").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let dispatcher = WorkerDispatcher::new(backend.clone(), AckWorkerFactory { processed_tx });
    let dispatcher_handle = dispatcher.launch().await.unwrap();

    let awaitable = backend.publish_awaitable::<AckTaskSpec>(()).await.unwrap();
    let task_id = awaitable.task_id();

    assert_eq!(awaitable.wait().await.unwrap(), ());
    assert_eq!(processed_rx.recv().await.unwrap(), task_id);

    dispatcher_handle.drain().await;
    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_singleton_task_dispatch() {
    let database = TestDatabase::new("singleton").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let release_signal = Arc::new(Semaphore::new(0));

    let factory = SingletonWorkerFactory {
        processed_tx,
        release_signal: release_signal.clone(),
    };
    let dispatcher = WorkerDispatcher::new(backend, factory);

    let dispatcher_handle = dispatcher.launch().await.unwrap();

    let first_task_id = processed_rx
        .recv()
        .await
        .expect("singleton task should be processed without publishing");
    assert!(first_task_id > 0);

    release_signal.add_permits(1);

    let second_task_id = processed_rx
        .recv()
        .await
        .expect("singleton task should be re-dispatched after finishing");
    assert_eq!(second_task_id, first_task_id);

    let drain_handle = tokio::spawn(dispatcher_handle.drain());
    release_signal.add_permits(1);
    drain_handle.await.unwrap();

    assert!(processed_rx.try_recv().is_err());

    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_sweeping() {
    let database = TestDatabase::new("sweeping").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();

    let factory = EchoWorkerFactory { processed_tx };
    let dispatcher = WorkerDispatcher::new(backend.clone(), factory);

    backend
        .publish::<EchoTaskSpec>(EchoTaskPayload {
            name: "Alice".to_string(),
        })
        .await
        .unwrap();

    let dispatcher_handle = dispatcher.launch().await.unwrap();

    backend
        .publish::<EchoTaskSpec>(EchoTaskPayload {
            name: "Bob".to_string(),
        })
        .await
        .unwrap();
    backend
        .publish::<EchoTaskSpec>(EchoTaskPayload {
            name: "Charlie".to_string(),
        })
        .await
        .unwrap();

    assert_names_echoed(&mut processed_rx, &["Alice", "Bob", "Charlie"]).await;

    dispatcher_handle.drain().await;

    assert!(processed_rx.recv().await.is_none());

    database.cleanup().await;
}

async fn assert_names_echoed(rx: &mut MpscReceiver<ProcessedTask>, names: &[&str]) {
    let mut processed = Vec::new();
    while processed.len() < names.len()
        && let Some(task) = rx.recv().await
    {
        processed.push(task);
    }

    assert_eq!(processed.len(), names.len());
    for name in names {
        assert!(processed.iter().any(|task| task.name == *name));
    }
}

struct TestDatabase {
    database_name: String,
    url: String,
}

impl TestDatabase {
    async fn new(test_name: &str) -> Self {
        let database_name = format!("bellows_{}_{}", test_name, unique_suffix());

        let mut admin =
            PgConnection::connect("postgres://postgres:postgres@localhost:5432/postgres")
                .await
                .expect("failed to connect to local postgres on localhost:5432");

        admin
            .execute(format!(r#"CREATE DATABASE "{}""#, database_name).as_str())
            .await
            .expect("failed to create temporary postgres test database");

        Self {
            database_name: database_name.clone(),
            url: format!("postgres://postgres:postgres@localhost:5432/{database_name}"),
        }
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn cleanup(&self) {
        let mut admin =
            PgConnection::connect("postgres://postgres:postgres@localhost:5432/postgres")
                .await
                .expect("failed to connect to local postgres on localhost:5432 for cleanup");

        admin
            .execute(
                format!(
                    r#"
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '{database_name}'
  AND pid <> pg_backend_pid()
"#,
                    database_name = self.database_name
                )
                .as_str(),
            )
            .await
            .expect("failed to terminate temporary postgres test database connections");

        admin
            .execute(format!(r#"DROP DATABASE "{}""#, self.database_name).as_str())
            .await
            .expect("failed to drop temporary postgres test database");
    }
}

fn unique_suffix() -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    format!("{}_{}", std::process::id(), unix_nanos)
}
