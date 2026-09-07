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

use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bellows::{
    Backend, PublishDispatchToken, PublishTrigger, SingletonTrigger, TaskDefinition,
    TaskExecutionBackend, TaskFailure, TaskResult, TaskSuccess, Worker, WorkerFactory,
    backends::{
        ClaimTaskError, FailTaskError, FinishTaskError, RenewTaskError,
        postgres::{PostgresBackend, PostgresBackendOptions, initialize_postgres_schema},
        postgres_execution::PostgresExecutionBackend,
    },
    dispatcher::WorkerDispatcher,
    run_task_once,
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

    async fn process(self, task_id: u64, task_payload: EchoTaskPayload) -> TaskResult<String> {
        self.processed_tx
            .send(ProcessedTask {
                task_id,
                name: task_payload.name.clone(),
            })
            .expect("processed task collector should remain available during the test");

        Ok(TaskSuccess::done(task_payload.name))
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

    async fn process(self, task_id: u64, _task_payload: ()) -> TaskResult<()> {
        self.processed_tx
            .send(task_id)
            .expect("ack task collector should remain available during the test");
        Ok(TaskSuccess::done(()))
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

    async fn process(self, task_id: u64, _task_payload: ()) -> TaskResult<()> {
        self.processed_tx
            .send(task_id)
            .expect("processed task collector should remain available during the test");
        self.release_signal
            .acquire()
            .await
            .expect("singleton worker gate semaphore should remain available")
            .forget();
        Ok(TaskSuccess::done(()))
    }
}

struct BlockingTaskSpec;

impl TaskDefinition for BlockingTaskSpec {
    const NAME: &str = "blocking";

    type Callback = ();
    type Trigger = PublishTrigger<()>;
}

struct BlockingWorkerFactory {
    started_tx: MpscSender<u64>,
    release_signal: Arc<Semaphore>,
}

impl WorkerFactory for BlockingWorkerFactory {
    type Worker = BlockingWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        BlockingWorker {
            started_tx: self.started_tx.clone(),
            release_signal: self.release_signal.clone(),
        }
    }
}

struct BlockingWorker {
    started_tx: MpscSender<u64>,
    release_signal: Arc<Semaphore>,
}

impl Worker for BlockingWorker {
    type Task = BlockingTaskSpec;

    async fn process(self, task_id: u64, _task_payload: ()) -> TaskResult<()> {
        self.started_tx
            .send(task_id)
            .expect("blocking task collector should remain available during the test");
        self.release_signal
            .acquire()
            .await
            .expect("blocking worker gate semaphore should remain available")
            .forget();
        Ok(TaskSuccess::done(()))
    }
}

struct RetryTaskSpec;

impl TaskDefinition for RetryTaskSpec {
    const NAME: &str = "retry_once";

    type Callback = ();
    type Trigger = PublishTrigger<()>;
}

struct RetryWorkerFactory {
    // Workers should be stateless to avoid context leak but it's for testing here so it's fine.
    attempts: Arc<AtomicUsize>,
    processed_tx: MpscSender<u64>,
}

impl WorkerFactory for RetryWorkerFactory {
    type Worker = RetryWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        RetryWorker {
            attempts: self.attempts.clone(),
            processed_tx: self.processed_tx.clone(),
        }
    }
}

struct RetryWorker {
    // Workers should be stateless to avoid context leak but it's for testing here so it's fine.
    attempts: Arc<AtomicUsize>,
    processed_tx: MpscSender<u64>,
}

impl Worker for RetryWorker {
    type Task = RetryTaskSpec;

    async fn process(self, task_id: u64, _task_payload: ()) -> TaskResult<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Err(TaskFailure::retry_immediately())
        } else {
            self.processed_tx
                .send(task_id)
                .expect("retry task collector should remain available during the test");
            Ok(TaskSuccess::done(()))
        }
    }
}

struct ReschedulingPublishedTaskSpec;

impl TaskDefinition for ReschedulingPublishedTaskSpec {
    const NAME: &str = "rescheduling_published";

    type Callback = u64;
    type Trigger = PublishTrigger<()>;
}

struct ReschedulingPublishedWorkerFactory {
    attempts: Arc<AtomicUsize>,
    processed_tx: MpscSender<u64>,
    next_run_at: Instant,
}

impl WorkerFactory for ReschedulingPublishedWorkerFactory {
    type Worker = ReschedulingPublishedWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        ReschedulingPublishedWorker {
            attempts: self.attempts.clone(),
            processed_tx: self.processed_tx.clone(),
            next_run_at: self.next_run_at,
        }
    }
}

struct ReschedulingPublishedWorker {
    attempts: Arc<AtomicUsize>,
    processed_tx: MpscSender<u64>,
    next_run_at: Instant,
}

impl Worker for ReschedulingPublishedWorker {
    type Task = ReschedulingPublishedTaskSpec;

    async fn process(self, task_id: u64, _task_payload: ()) -> TaskResult<u64> {
        self.processed_tx.send(task_id).expect(
            "rescheduling published task collector should remain available during the test",
        );

        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Ok(TaskSuccess::schedule_next_run(task_id, self.next_run_at))
        } else {
            Ok(TaskSuccess::done(task_id))
        }
    }
}

struct ScheduledSingletonTaskSpec;

impl TaskDefinition for ScheduledSingletonTaskSpec {
    const NAME: &str = "scheduled_singleton";

    type Callback = ();
    type Trigger = SingletonTrigger;
}

struct ScheduledSingletonWorkerFactory {
    attempts: Arc<AtomicUsize>,
    processed_tx: MpscSender<u64>,
    next_run_at: Instant,
    release_signal: Arc<Semaphore>,
}

impl WorkerFactory for ScheduledSingletonWorkerFactory {
    type Worker = ScheduledSingletonWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        ScheduledSingletonWorker {
            attempts: self.attempts.clone(),
            processed_tx: self.processed_tx.clone(),
            next_run_at: self.next_run_at,
            release_signal: self.release_signal.clone(),
        }
    }
}

struct ScheduledSingletonWorker {
    attempts: Arc<AtomicUsize>,
    processed_tx: MpscSender<u64>,
    next_run_at: Instant,
    release_signal: Arc<Semaphore>,
}

impl Worker for ScheduledSingletonWorker {
    type Task = ScheduledSingletonTaskSpec;

    async fn process(self, task_id: u64, _task_payload: ()) -> TaskResult<()> {
        self.processed_tx
            .send(task_id)
            .expect("scheduled singleton task collector should remain available during the test");

        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Ok(TaskSuccess::schedule_next_run((), self.next_run_at))
        } else {
            self.release_signal
                .acquire()
                .await
                .expect("scheduled singleton worker gate semaphore should remain available")
                .forget();
            Ok(TaskSuccess::done(()))
        }
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
async fn test_postgres_publish_future_delays_task_availability() {
    let database = TestDatabase::new("future_publish").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let dispatcher = WorkerDispatcher::new(backend.clone(), EchoWorkerFactory { processed_tx });
    let dispatcher_handle = dispatcher.launch().await.unwrap();

    let published = backend
        .publish_future::<EchoTaskSpec>(
            EchoTaskPayload {
                name: "Alice".to_string(),
            },
            std::time::Instant::now() + std::time::Duration::from_millis(200),
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), processed_rx.recv())
            .await
            .is_err()
    );

    let processed = tokio::time::timeout(std::time::Duration::from_secs(1), processed_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(processed.task_id, published.task_id);
    assert_eq!(processed.name, "Alice");

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
async fn test_dispatcher_drains_multiple_preexisting_tasks_without_waiting() {
    let database = TestDatabase::new("drains_multiple_preexisting_tasks").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release_signal = Arc::new(Semaphore::new(0));

    let first = backend.publish::<BlockingTaskSpec>(()).await.unwrap();
    let second = backend.publish::<BlockingTaskSpec>(()).await.unwrap();

    let dispatcher = WorkerDispatcher::new(
        backend,
        BlockingWorkerFactory {
            started_tx,
            release_signal: release_signal.clone(),
        },
    );
    let dispatcher_handle = dispatcher.launch().await.unwrap();

    let started_first = tokio::time::timeout(std::time::Duration::from_secs(1), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let started_second = tokio::time::timeout(std::time::Duration::from_secs(1), started_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(started_first == first.task_id || started_first == second.task_id);
    assert!(started_second == first.task_id || started_second == second.task_id);
    assert_ne!(started_first, started_second);

    let drain_handle = tokio::spawn(dispatcher_handle.drain());
    release_signal.add_permits(2);
    drain_handle.await.unwrap();

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

#[tokio::test]
async fn test_worker_failure_is_retried() {
    let database = TestDatabase::new("worker_failure_is_retried").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let attempts = Arc::new(AtomicUsize::new(0));

    let dispatcher = WorkerDispatcher::new(
        backend.clone(),
        RetryWorkerFactory {
            attempts: attempts.clone(),
            processed_tx,
        },
    );
    let dispatcher_handle = dispatcher.launch().await.unwrap();

    let published = backend.publish::<RetryTaskSpec>(()).await.unwrap();

    assert_eq!(processed_rx.recv().await.unwrap(), published.task_id);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    dispatcher_handle.drain().await;
    database.cleanup().await;
}

#[tokio::test]
async fn test_successful_published_task_can_schedule_next_run() {
    let database = TestDatabase::new("successful_published_schedule_next_run").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let attempts = Arc::new(AtomicUsize::new(0));
    let next_run_at = Instant::now() + Duration::from_millis(200);

    let dispatcher = WorkerDispatcher::new(
        backend.clone(),
        ReschedulingPublishedWorkerFactory {
            attempts: attempts.clone(),
            processed_tx,
            next_run_at,
        },
    );
    let dispatcher_handle = dispatcher.launch().await.unwrap();

    let awaitable = backend
        .publish_awaitable::<ReschedulingPublishedTaskSpec>(())
        .await
        .unwrap();

    let first_task_id = tokio::time::timeout(Duration::from_secs(1), processed_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(awaitable.wait().await.unwrap(), first_task_id);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), processed_rx.recv())
            .await
            .is_err()
    );

    let second_task_id = tokio::time::timeout(Duration::from_secs(1), processed_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(second_task_id, first_task_id);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    dispatcher_handle.drain().await;
    database.cleanup().await;
}

#[tokio::test]
async fn test_successful_singleton_task_can_schedule_next_run() {
    let database = TestDatabase::new("successful_singleton_schedule_next_run").await;
    let backend = PostgresBackend::connect(database.url()).await.unwrap();
    backend.initialize().await.unwrap();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let attempts = Arc::new(AtomicUsize::new(0));
    let release_signal = Arc::new(Semaphore::new(0));
    let next_run_at = Instant::now() + Duration::from_millis(200);

    let dispatcher = WorkerDispatcher::new(
        backend,
        ScheduledSingletonWorkerFactory {
            attempts: attempts.clone(),
            processed_tx,
            next_run_at,
            release_signal: release_signal.clone(),
        },
    );
    let dispatcher_handle = dispatcher.launch().await.unwrap();

    let first_task_id = tokio::time::timeout(Duration::from_secs(1), processed_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(50), processed_rx.recv())
            .await
            .is_err()
    );

    let second_task_id = tokio::time::timeout(Duration::from_secs(1), processed_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(second_task_id, first_task_id);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    let drain_handle = tokio::spawn(dispatcher_handle.drain());
    release_signal.add_permits(1);
    drain_handle.await.unwrap();

    assert!(processed_rx.try_recv().is_err());

    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_rejects_invalid_schema_before_connecting() {
    for schema in [
        "", "Public", "1schema", "a.b", "a\"b", "a b", "a\n", "a\r", "a\nb", "é", "a-b",
    ] {
        let error = PostgresBackend::connect_with_options(
            "not a database URL",
            PostgresBackendOptions {
                schema: Some(schema.to_owned()),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, sqlx::Error::Configuration(_)));
        assert!(error.to_string().contains("Postgres schema names"));

        let error = PostgresExecutionBackend::connect_with_options(
            "not a database URL",
            PostgresBackendOptions {
                schema: Some(schema.to_owned()),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, sqlx::Error::Configuration(_)));
        assert!(error.to_string().contains("Postgres schema names"));

        let error = initialize_postgres_schema("not a database URL", schema)
            .await
            .unwrap_err();
        assert!(matches!(error, sqlx::Error::Configuration(_)));
        assert!(error.to_string().contains("Postgres schema names"));
    }
}

#[tokio::test]
async fn test_postgres_execution_named_schema_without_listener() {
    let database = TestDatabase::new("execution_named").await;
    let schema = format!("execution_{}", unique_suffix());
    let table = format!("\"{schema}\".bellows_tasks");
    let application_name = format!("execution_{}", unique_suffix());
    let execution_url = format!("{}?application_name={application_name}", database.url());
    let mut admin = PgConnection::connect(database.url()).await.unwrap();
    admin
        .execute(format!("CREATE SCHEMA \"{schema}\"").as_str())
        .await
        .unwrap();
    let backend = PostgresExecutionBackend::connect_with_options(
        &execution_url,
        PostgresBackendOptions {
            schema: Some(schema.clone()),
        },
    )
    .await
    .unwrap();
    let uninitialized_table: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(&table)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert!(uninitialized_table.is_none(), "connect must not initialize");
    initialize_postgres_schema(database.url(), &schema)
        .await
        .unwrap();
    initialize_postgres_schema(database.url(), &schema)
        .await
        .unwrap();

    let task_id: i64 = sqlx::query_scalar(&format!(
        "INSERT INTO {table} (task_name, payload_json) VALUES ($1, $2) RETURNING task_id"
    ))
    .bind(EchoTaskSpec::NAME)
    .bind(
        serde_json::to_string(&EchoTaskPayload {
            name: "Alice".to_owned(),
        })
        .unwrap(),
    )
    .fetch_one(&mut admin)
    .await
    .unwrap();
    let task_id = u64::try_from(task_id).unwrap();

    struct ExecutionWorkerFactory {
        processed_tx: MpscSender<ProcessedTask>,
        release_signal: Arc<Semaphore>,
    }

    impl WorkerFactory for ExecutionWorkerFactory {
        type Worker = ExecutionWorker;

        fn build(&self, worker_id: u64) -> Self::Worker {
            assert_eq!(worker_id, 17);
            ExecutionWorker {
                processed_tx: self.processed_tx.clone(),
                release_signal: self.release_signal.clone(),
            }
        }
    }

    struct ExecutionWorker {
        processed_tx: MpscSender<ProcessedTask>,
        release_signal: Arc<Semaphore>,
    }

    impl Worker for ExecutionWorker {
        type Task = EchoTaskSpec;

        async fn process(self, task_id: u64, payload: EchoTaskPayload) -> TaskResult<String> {
            self.processed_tx
                .send(ProcessedTask {
                    task_id,
                    name: payload.name.clone(),
                })
                .unwrap();
            self.release_signal.acquire().await.unwrap().forget();
            Ok(TaskSuccess::done(payload.name))
        }
    }

    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let release_signal = Arc::new(Semaphore::new(0));
    let execution = tokio::spawn(run_task_once(
        backend.clone(),
        ExecutionWorkerFactory {
            processed_tx,
            release_signal: release_signal.clone(),
        },
        17,
        PublishDispatchToken::Task(task_id),
    ));
    assert_eq!(
        processed_rx.recv().await.unwrap(),
        ProcessedTask {
            task_id,
            name: "Alice".to_owned(),
        }
    );
    // Verify ownership while processing is gated. This also lets SQLx's asynchronous connection
    // return finish before finalization, so the serial count below measures listener overhead,
    // not overlapping pool checkout/release for an immediately completing worker.
    let lease: (Option<i64>, Option<i64>) = sqlx::query_as(&format!(
        "SELECT lease_worker_id, available_from_unix_ms FROM {table} WHERE task_id = $1"
    ))
    .bind(i64::try_from(task_id).unwrap())
    .fetch_one(&mut admin)
    .await
    .unwrap();
    assert_eq!(lease.0, Some(17));
    assert!(lease.1.is_some());
    let default_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('public.bellows_tasks')::text")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert!(default_table.is_none());
    release_signal.add_permits(1);
    execution.await.unwrap();
    assert!(processed_rx.recv().await.is_none());
    let remaining: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(remaining, 0, "row removal must commit before returning");

    // This serial workload uses one pooled connection, with no additional dedicated listener.
    let connections: Vec<(i32, String, String)> = sqlx::query_as(
        "SELECT pid, state, query FROM pg_stat_activity \
         WHERE application_name = $1 AND datname = $2",
    )
    .bind(&application_name)
    .bind(&database.database_name)
    .fetch_all(&mut admin)
    .await
    .unwrap();
    assert_eq!(
        connections.len(),
        1,
        "unexpected connections: {connections:?}"
    );
    drop(backend);
    admin.close().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_execution_delivers_callbacks() {
    for named_schema in [false, true] {
        let database = TestDatabase::new("execution_callback").await;
        let mut admin = PgConnection::connect(database.url()).await.unwrap();
        let options = if named_schema {
            let schema = format!("callbacks_{}", unique_suffix());
            admin
                .execute(format!("CREATE SCHEMA \"{schema}\"").as_str())
                .await
                .unwrap();
            PostgresBackendOptions {
                schema: Some(schema),
            }
        } else {
            PostgresBackendOptions::default()
        };
        let publisher = PostgresBackend::connect_with_options(database.url(), options.clone())
            .await
            .unwrap();
        publisher.initialize().await.unwrap();
        let executor = if named_schema {
            PostgresExecutionBackend::connect_with_options(database.url(), options.clone())
                .await
                .unwrap()
        } else {
            PostgresExecutionBackend::connect(database.url())
                .await
                .unwrap()
        };
        let awaitable = publisher
            .publish_awaitable::<EchoTaskSpec>(EchoTaskPayload {
                name: "Alice".to_owned(),
            })
            .await
            .unwrap();
        let task_id = awaitable.task_id();
        let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
        run_task_once(
            executor.clone(),
            EchoWorkerFactory { processed_tx },
            17,
            PublishDispatchToken::Task(task_id),
        )
        .await;

        let table = match options.schema {
            Some(schema) => format!("\"{schema}\".bellows_tasks"),
            None => "bellows_tasks".to_owned(),
        };
        let remaining: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&mut admin)
            .await
            .unwrap();
        assert_eq!(remaining, 0);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), awaitable.wait())
                .await
                .expect("execution-only completion must notify the publisher")
                .unwrap(),
            "Alice"
        );
        assert_eq!(
            processed_rx.recv().await.unwrap(),
            ProcessedTask {
                task_id,
                name: "Alice".to_owned(),
            }
        );
        assert!(processed_rx.recv().await.is_none());
        drop(executor);
        drop(publisher);
        admin.close().await.unwrap();
        database.cleanup().await;
    }
}

#[tokio::test]
async fn test_postgres_named_schema_dispatch_and_reinitialization() {
    let database = TestDatabase::new("named_dispatch").await;
    let schema = format!("tasks_{}", unique_suffix());
    let mut admin = PgConnection::connect(database.url()).await.unwrap();
    admin
        .execute(format!("CREATE SCHEMA \"{schema}\"").as_str())
        .await
        .unwrap();
    initialize_postgres_schema(database.url(), &schema)
        .await
        .unwrap();

    let trigger_query = "SELECT oid::bigint FROM pg_trigger \
        WHERE tgrelid = $1::regclass AND tgname = 'bellows_tasks_notify_available'";
    let table = format!("\"{schema}\".bellows_tasks");
    let original_trigger: i64 = sqlx::query_scalar(trigger_query)
        .bind(&table)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    let backend = PostgresBackend::connect_with_options(
        database.url(),
        PostgresBackendOptions {
            schema: Some(schema.clone()),
        },
    )
    .await
    .unwrap();
    let published = backend
        .publish::<EchoTaskSpec>(EchoTaskPayload {
            name: "Alice".to_owned(),
        })
        .await
        .unwrap();

    initialize_postgres_schema(database.url(), &schema)
        .await
        .unwrap();
    backend.initialize().await.unwrap();
    let trigger: i64 = sqlx::query_scalar(trigger_query)
        .bind(&table)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        trigger, original_trigger,
        "initialization must retain the trigger"
    );
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let dispatcher = WorkerDispatcher::new(backend.clone(), EchoWorkerFactory { processed_tx });
    let handle = dispatcher.launch().await.unwrap();
    assert_eq!(
        processed_rx.recv().await.unwrap(),
        ProcessedTask {
            task_id: published.task_id,
            name: "Alice".to_owned(),
        }
    );

    // Publishing after launch exercises notification-driven discovery and typed callback delivery.
    let awaitable = backend
        .publish_awaitable::<EchoTaskSpec>(EchoTaskPayload {
            name: "Bob".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(awaitable.wait().await.unwrap(), "Bob");
    assert_eq!(processed_rx.recv().await.unwrap().name, "Bob");
    handle.drain().await;
    assert!(processed_rx.recv().await.is_none());
    let remaining: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
    let default_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('public.bellows_tasks')::text")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert!(default_table.is_none());
    drop(backend);
    admin.close().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_missing_schema_does_not_fall_back() {
    let database = TestDatabase::new("missing_schema").await;
    let schema = format!("missing_{}", unique_suffix());
    assert!(
        initialize_postgres_schema(database.url(), &schema)
            .await
            .is_err()
    );
    let backend = PostgresBackend::connect_with_options(
        database.url(),
        PostgresBackendOptions {
            schema: Some(schema.clone()),
        },
    )
    .await
    .unwrap();
    assert!(backend.initialize().await.is_err());
    let mut admin = PgConnection::connect(database.url()).await.unwrap();
    let schema_exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
            .bind(&schema)
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert!(!schema_exists);
    let default_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('public.bellows_tasks')::text")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert!(default_table.is_none());

    // A failed initialization must leave the pool usable for a later administrative retry.
    admin
        .execute(format!("CREATE SCHEMA \"{schema}\"").as_str())
        .await
        .unwrap();
    backend.initialize().await.unwrap();
    backend.publish::<AckTaskSpec>(()).await.unwrap();
    drop(backend);
    admin.close().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_named_schema_task_operations() {
    let database = TestDatabase::new("named_operations").await;
    let schema = format!("_tasks_{}", unique_suffix());
    let table = format!("\"{schema}\".bellows_tasks");
    let mut admin = PgConnection::connect(database.url()).await.unwrap();
    admin
        .execute(format!("CREATE SCHEMA \"{schema}\"").as_str())
        .await
        .unwrap();
    let backend = PostgresBackend::connect_with_options(
        database.url(),
        PostgresBackendOptions {
            schema: Some(schema),
        },
    )
    .await
    .unwrap();
    backend.initialize().await.unwrap();
    let expiration = Instant::now() + Duration::from_secs(60);
    let later = expiration + Duration::from_secs(60);
    assert!(matches!(
        backend
            .claim_published::<AckTaskSpec>(17, 999, expiration)
            .await,
        Err(ClaimTaskError::TaskNotFound)
    ));
    assert!(matches!(
        backend
            .claim_earliest_published::<AckTaskSpec>(17, expiration)
            .await,
        Err(ClaimTaskError::TaskUnavailable {
            available_from: None
        })
    ));
    let future = backend
        .publish_future::<AckTaskSpec>((), later)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .claim_published::<AckTaskSpec>(17, future.task_id, expiration)
            .await,
        Err(ClaimTaskError::TaskUnavailable {
            available_from: Some(_)
        })
    ));
    assert!(matches!(
        backend
            .claim_earliest_published::<AckTaskSpec>(17, expiration)
            .await,
        Err(ClaimTaskError::TaskUnavailable {
            available_from: Some(_)
        })
    ));

    let first = backend.publish_awaitable::<AckTaskSpec>(()).await.unwrap();
    let first_id = first.task_id();
    let second = backend.publish::<AckTaskSpec>(()).await.unwrap();
    let claimed = backend
        .claim_earliest_published::<AckTaskSpec>(17, expiration)
        .await
        .unwrap();
    assert_eq!(claimed.task_id, first_id);
    assert!(matches!(
        backend
            .claim_published::<AckTaskSpec>(18, first_id, expiration)
            .await,
        Err(ClaimTaskError::TaskLeased { .. })
    ));
    assert!(matches!(
        backend.renew(18, first_id, later).await,
        Err(RenewTaskError::LeaseLost)
    ));
    assert!(matches!(
        backend.fail(18, first_id, None).await,
        Err(FailTaskError::LeaseLost)
    ));
    assert!(matches!(
        backend.finish::<AckTaskSpec>(18, first_id, (), None).await,
        Err(FinishTaskError::LeaseLost)
    ));
    backend.renew(17, first_id, later).await.unwrap();
    let state = task_state(&mut admin, &table, first_id).await;
    assert_eq!(state.0, Some(17));
    assert!(state.1.unwrap() > unix_ms_now() + 60_000);
    assert!(state.2.is_some());
    backend.fail(17, first_id, Some(later)).await.unwrap();
    let state = task_state(&mut admin, &table, first_id).await;
    assert_eq!(state.0, None);
    assert!(state.1.is_some());
    assert!(state.2.is_some(), "failure must retain the callback");
    assert!(matches!(
        backend
            .claim_published::<AckTaskSpec>(17, first_id, expiration)
            .await,
        Err(ClaimTaskError::TaskUnavailable {
            available_from: Some(_)
        })
    ));
    let claimed = backend
        .claim_earliest_published::<AckTaskSpec>(17, expiration)
        .await
        .unwrap();
    assert_eq!(claimed.task_id, second.task_id);
    backend
        .finish::<AckTaskSpec>(17, second.task_id, (), None)
        .await
        .unwrap();

    make_task_available(&mut admin, &table, first_id).await;
    backend
        .claim_published::<AckTaskSpec>(17, first_id, expiration)
        .await
        .unwrap();
    backend
        .finish::<AckTaskSpec>(17, first_id, (), Some(later))
        .await
        .unwrap();
    first.wait().await.unwrap();
    let state = task_state(&mut admin, &table, first_id).await;
    assert_eq!(state.0, None);
    assert!(state.1.is_some());
    assert_eq!(
        state.2, None,
        "rescheduling must clear the delivered callback"
    );
    make_task_available(&mut admin, &table, first_id).await;
    assert_eq!(
        backend
            .claim_earliest_published::<AckTaskSpec>(17, expiration)
            .await
            .unwrap()
            .task_id,
        first_id
    );
    backend
        .finish::<AckTaskSpec>(17, first_id, (), None)
        .await
        .unwrap();
    make_task_available(&mut admin, &table, future.task_id).await;
    backend
        .claim_published::<AckTaskSpec>(17, future.task_id, expiration)
        .await
        .unwrap();
    backend
        .finish::<AckTaskSpec>(17, future.task_id, (), None)
        .await
        .unwrap();
    let remaining: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        remaining, 0,
        "published completion must remove rows before returning"
    );

    let singleton = backend
        .claim_singleton::<SingletonTaskSpec>(17, expiration)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .claim_singleton::<SingletonTaskSpec>(18, expiration)
            .await,
        Err(ClaimTaskError::TaskLeased { .. })
    ));
    backend.renew(17, singleton.task_id, later).await.unwrap();
    backend.fail(17, singleton.task_id, None).await.unwrap();
    assert_eq!(
        task_state(&mut admin, &table, singleton.task_id).await,
        (None, None, None)
    );
    assert_eq!(
        backend
            .claim_singleton::<SingletonTaskSpec>(18, expiration)
            .await
            .unwrap()
            .task_id,
        singleton.task_id
    );
    backend
        .finish::<SingletonTaskSpec>(18, singleton.task_id, (), Some(later))
        .await
        .unwrap();
    let state = task_state(&mut admin, &table, singleton.task_id).await;
    assert_eq!(state.0, None);
    assert!(state.1.is_some());
    assert!(matches!(
        backend
            .claim_singleton::<SingletonTaskSpec>(17, expiration)
            .await,
        Err(ClaimTaskError::TaskUnavailable {
            available_from: Some(_)
        })
    ));
    make_task_available(&mut admin, &table, singleton.task_id).await;
    assert_eq!(
        backend
            .claim_singleton::<SingletonTaskSpec>(17, expiration)
            .await
            .unwrap()
            .task_id,
        singleton.task_id
    );
    backend
        .finish::<SingletonTaskSpec>(17, singleton.task_id, (), None)
        .await
        .unwrap();
    assert_eq!(
        task_state(&mut admin, &table, singleton.task_id).await,
        (None, None, None)
    );
    drop(backend);
    admin.close().await.unwrap();
    database.cleanup().await;
}

async fn task_state(
    admin: &mut PgConnection,
    table: &str,
    task_id: u64,
) -> (Option<i64>, Option<i64>, Option<i64>) {
    sqlx::query_as(&format!(
        "SELECT lease_worker_id, available_from_unix_ms, callback_id FROM {table} WHERE task_id = $1"
    ))
    .bind(i64::try_from(task_id).unwrap())
    .fetch_one(admin)
    .await
    .unwrap()
}

async fn make_task_available(admin: &mut PgConnection, table: &str, task_id: u64) {
    sqlx::query(&format!(
        "UPDATE {table} SET available_from_unix_ms = NULL WHERE task_id = $1"
    ))
    .bind(i64::try_from(task_id).unwrap())
    .execute(admin)
    .await
    .unwrap();
}

fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
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
