#![cfg(feature = "postgres")]

//! Ignored integration tests for the Postgres backend.
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
//!
//! Then run the ignored tests explicitly:
//!
//! cargo test --features postgres --test postgres_backend -- --ignored

use std::time::{SystemTime, UNIX_EPOCH};

use bellows::{
    Backend, TaskDefinition, Worker, WorkerFactory, backends::postgres::PostgresBackend,
    dispatcher::WorkerDispatcher,
};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Executor, PgConnection};
use tokio::sync::mpsc::{UnboundedReceiver as MpscReceiver, UnboundedSender as MpscSender};

struct EchoTaskSpec;

#[derive(Debug, Serialize, Deserialize)]
struct EchoTaskPayload {
    pub name: String,
}

impl TaskDefinition for EchoTaskSpec {
    const NAME: &str = "echo";

    type Payload = EchoTaskPayload;
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

    async fn process(self, task_id: u64, task_payload: EchoTaskPayload) {
        self.processed_tx
            .send(ProcessedTask {
                task_id,
                name: task_payload.name,
            })
            .expect("processed task collector should remain available during the test");
    }
}

#[tokio::test]
#[ignore = "requires Postgres instance"]
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
#[ignore = "requires Postgres instance"]
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
