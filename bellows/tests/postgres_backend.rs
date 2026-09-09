#![cfg(all(not(target_arch = "wasm32"), feature = "postgres"))]

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
    TaskExecutionBackend, TaskFailure, TaskPublishingBackend, TaskResult, TaskSuccess, Worker,
    WorkerFactory,
    backends::{
        ClaimTaskError, FailTaskError, FinishTaskError, PublishTaskError, RenewTaskError,
        postgres::{PostgresBackend, PostgresBackendOptions, initialize_postgres_schema},
        postgres_execution::PostgresExecutionBackend,
        postgres_publishing::PostgresPublishingBackend,
    },
    dispatcher::WorkerDispatcher,
    run_task_once,
};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Executor, PgConnection, Row};
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

    // Drain stops new claims, not a claim already awaiting PostgreSQL. Release those workers
    // too, until draining drops the factory and closes the collector.
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            biased;
            dispatcher_handle.drain(),
            async {
                release_signal.add_permits(1);
                while let Some(task_id) = processed_rx.recv().await {
                    assert_eq!(task_id, first_task_id);
                    release_signal.add_permits(1);
                }
            },
        );
    })
    .await
    .expect("singleton attempts already in flight should drain");

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

    // An already-started claim may succeed when this worker releases ownership.
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            biased;
            dispatcher_handle.drain(),
            async {
                release_signal.add_permits(1);
                while let Some(task_id) = processed_rx.recv().await {
                    assert_eq!(task_id, first_task_id);
                    release_signal.add_permits(1);
                }
            },
        );
    })
    .await
    .expect("rescheduled singleton attempts already in flight should drain");

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

        let error = PostgresPublishingBackend::connect_with_options(
            "not a database URL",
            PostgresBackendOptions {
                schema: Some(schema.to_owned()),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, sqlx::Error::Configuration(_)));
        assert!(error.to_string().contains("Postgres schema names"));
    }
}

#[tokio::test]
async fn test_postgres_publishing_rows_notifications_and_execution() {
    for named_schema in [false, true] {
        let database = TestDatabase::new("publishing_rows").await;
        let mut admin = PgConnection::connect(database.url()).await.unwrap();
        let schema = if named_schema { "publishing" } else { "public" };
        if named_schema {
            admin.execute("CREATE SCHEMA publishing").await.unwrap();
        }
        let options = PostgresBackendOptions {
            schema: named_schema.then(|| schema.to_owned()),
        };
        let publisher = if named_schema {
            PostgresPublishingBackend::connect_with_options(database.url(), options.clone())
                .await
                .unwrap()
        } else {
            PostgresPublishingBackend::connect(database.url())
                .await
                .unwrap()
        };
        let table = format!("\"{schema}\".bellows_tasks");
        let uninitialized: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(&table)
            .fetch_one(&mut admin)
            .await
            .unwrap();
        assert!(uninitialized.is_none());
        assert!(publisher.publish::<AckTaskSpec>(()).await.is_err());
        initialize_postgres_schema(database.url(), schema)
            .await
            .unwrap();
        let listener = PostgresBackend::connect_with_options(database.url(), options.clone())
            .await
            .unwrap();
        let mut signals = listener.subscribe::<EchoTaskSpec>().await.unwrap();
        let executor = PostgresExecutionBackend::connect_with_options(database.url(), options)
            .await
            .unwrap();
        let before = Instant::now();
        let deadline = before + Duration::from_secs(60);
        let deadline_ms = unix_ms_now() + 60_000;
        let name = "hello \"🦀\"\n";
        let immediate = publisher
            .publish::<EchoTaskSpec>(EchoTaskPayload { name: name.into() })
            .await
            .unwrap();
        let future = publisher
            .publish_future::<EchoTaskSpec>(EchoTaskPayload { name: name.into() }, deadline)
            .await
            .unwrap();
        let unit = publisher.publish::<AckTaskSpec>(()).await.unwrap();
        let future_unit = publisher
            .publish_future::<AckTaskSpec>((), deadline)
            .await
            .unwrap();
        publisher.close().await.unwrap();

        for (receipt, task_name, payload, delayed) in [
            (
                immediate,
                EchoTaskSpec::NAME,
                serde_json::to_string(&EchoTaskPayload { name: name.into() }).unwrap(),
                false,
            ),
            (
                future,
                EchoTaskSpec::NAME,
                serde_json::to_string(&EchoTaskPayload { name: name.into() }).unwrap(),
                true,
            ),
            (unit, AckTaskSpec::NAME, "null".to_owned(), false),
            (future_unit, AckTaskSpec::NAME, "null".to_owned(), true),
        ] {
            let row = sqlx::query(&format!("SELECT * FROM {table} WHERE task_id = $1"))
                .bind(i64::try_from(receipt.task_id).unwrap())
                .fetch_one(&mut admin)
                .await
                .unwrap();
            assert_eq!(row.get::<String, _>("task_name"), task_name);
            assert_eq!(row.get::<String, _>("payload_json"), payload);
            assert_eq!(row.get::<Option<String>, _>("task_unique_key"), None);
            assert_eq!(row.get::<Option<i64>, _>("callback_id"), None);
            assert_eq!(row.get::<Option<i64>, _>("lease_worker_id"), None);
            let available = row.get::<Option<i64>, _>("available_from_unix_ms");
            if delayed {
                assert!((available.unwrap() - deadline_ms).abs() <= 20);
            } else {
                assert_eq!(available, None);
            }
        }
        for (receipt, delayed) in [(immediate, false), (future, true)] {
            let signal = tokio::time::timeout(Duration::from_secs(5), signals.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(signal.task_id, Some(receipt.task_id));
            if delayed {
                assert!(signal.available_from >= deadline - Duration::from_millis(20));
                assert!(signal.available_from <= deadline + Duration::from_millis(20));
            } else {
                assert!(signal.available_from >= before && signal.available_from <= Instant::now());
            }
        }
        assert!(matches!(
            executor
                .claim_published::<AckTaskSpec>(17, immediate.task_id, deadline)
                .await,
            Err(ClaimTaskError::TaskNotFound)
        ));
        assert!(matches!(
            executor
                .claim_published::<EchoTaskSpec>(17, future.task_id, deadline)
                .await,
            Err(ClaimTaskError::TaskUnavailable {
                available_from: Some(_)
            })
        ));
        make_task_available(&mut admin, &table, future.task_id).await;
        for receipt in [immediate, future] {
            let claimed = executor
                .claim_published::<EchoTaskSpec>(17, receipt.task_id, deadline)
                .await
                .unwrap();
            assert_eq!(claimed.task_payload.name, name);
            executor
                .finish::<EchoTaskSpec>(17, receipt.task_id, "no callback registered".into(), None)
                .await
                .unwrap();
        }
        make_task_available(&mut admin, &table, future_unit.task_id).await;
        for receipt in [unit, future_unit] {
            executor
                .claim_published::<AckTaskSpec>(17, receipt.task_id, deadline)
                .await
                .unwrap();
            executor
                .finish::<AckTaskSpec>(17, receipt.task_id, (), None)
                .await
                .unwrap();
        }
        let remaining: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&mut admin)
            .await
            .unwrap();
        assert_eq!(remaining, 0);
        if named_schema {
            let public: Option<String> =
                sqlx::query_scalar("SELECT to_regclass('public.bellows_tasks')::text")
                    .fetch_one(&mut admin)
                    .await
                    .unwrap();
            assert!(public.is_none());
        }
        executor.close().await.unwrap();
        drop(signals);
        drop(listener);
        admin.close().await.unwrap();
        database.cleanup().await;
    }
}

#[tokio::test]
async fn test_postgres_publishing_missing_schema_and_table_never_fall_back() {
    let database = TestDatabase::new("publishing_missing").await;
    initialize_postgres_schema(database.url(), "public")
        .await
        .unwrap();
    let mut admin = PgConnection::connect(database.url()).await.unwrap();
    let publisher = PostgresPublishingBackend::connect_with_options(
        database.url(),
        PostgresBackendOptions {
            schema: Some("missing".into()),
        },
    )
    .await
    .unwrap();
    for schema_exists in [false, true] {
        if schema_exists {
            admin.execute("CREATE SCHEMA missing").await.unwrap();
        }
        let PublishTaskError::Backend(error) =
            publisher.publish::<AckTaskSpec>(()).await.unwrap_err();
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<sqlx::Error>()
            .unwrap();
        assert_eq!(
            source.as_database_error().unwrap().code().as_deref(),
            Some("42P01")
        );
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'missing')",
        )
        .fetch_one(&mut admin)
        .await
        .unwrap();
        assert_eq!(
            exists, schema_exists,
            "connecting/publishing must not create a schema"
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM public.bellows_tasks")
            .fetch_one(&mut admin)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    publisher.close().await.unwrap();
    admin.close().await.unwrap();
    database.cleanup().await;
}

#[derive(Deserialize)]
struct RejectedPayload;

impl Serialize for RejectedPayload {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "intentional payload serialization failure",
        ))
    }
}

struct RejectedPayloadTask;
impl TaskDefinition for RejectedPayloadTask {
    const NAME: &str = "rejected_payload";
    type Callback = String;
    type Trigger = PublishTrigger<RejectedPayload>;
}

#[tokio::test]
async fn test_postgres_publishing_error_sources_no_retries_and_id_range() {
    let database = TestDatabase::new("publishing_errors").await;
    initialize_postgres_schema(database.url(), "public")
        .await
        .unwrap();
    let mut admin = PgConnection::connect(database.url()).await.unwrap();
    let publisher = PostgresPublishingBackend::connect(database.url())
        .await
        .unwrap();
    for future in [false, true] {
        let result = if future {
            publisher
                .publish_future::<RejectedPayloadTask>(RejectedPayload, Instant::now())
                .await
        } else {
            publisher
                .publish::<RejectedPayloadTask>(RejectedPayload)
                .await
        };
        let PublishTaskError::Backend(error) = result.unwrap_err();
        assert!(error.source().unwrap().is::<serde_json::Error>());
        assert!(
            error
                .to_string()
                .contains("intentional payload serialization failure")
        );
    }
    let called: bool = sqlx::query_scalar("SELECT is_called FROM bellows_tasks_task_id_seq")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert!(!called, "serialization errors must not send inserts");
    admin
        .execute("ALTER TABLE bellows_tasks ADD CONSTRAINT reject_insert CHECK (false)")
        .await
        .unwrap();
    let PublishTaskError::Backend(error) = publisher.publish::<AckTaskSpec>(()).await.unwrap_err();
    let source = error
        .source()
        .unwrap()
        .downcast_ref::<sqlx::Error>()
        .unwrap();
    assert_eq!(
        source.as_database_error().unwrap().code().as_deref(),
        Some("23514")
    );
    let attempts: i64 = sqlx::query_scalar("SELECT last_value FROM bellows_tasks_task_id_seq")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(attempts, 1, "SQL failures must not retry the insert");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM bellows_tasks")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(count, 0);
    admin
        .execute("ALTER TABLE bellows_tasks DROP CONSTRAINT reject_insert")
        .await
        .unwrap();
    assert_eq!(
        publisher.publish::<AckTaskSpec>(()).await.unwrap().task_id,
        2
    );
    admin
        .execute("ALTER SEQUENCE bellows_tasks_task_id_seq MINVALUE -1 RESTART WITH -1")
        .await
        .unwrap();
    let PublishTaskError::Backend(error) = publisher.publish::<AckTaskSpec>(()).await.unwrap_err();
    assert!(error.source().unwrap().is::<std::num::TryFromIntError>());
    let committed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bellows_tasks WHERE task_id = -1")
            .fetch_one(&mut admin)
            .await
            .unwrap();
    assert_eq!(
        committed, 1,
        "receipt decoding errors do not undo committed inserts"
    );
    // Native PostgreSQL receipts retain the full nonnegative i64 range, not a JS safe-integer cap.
    for id in [9_007_199_254_740_992_i64, i64::MAX] {
        admin
            .execute(format!("ALTER SEQUENCE bellows_tasks_task_id_seq RESTART WITH {id}").as_str())
            .await
            .unwrap();
        assert_eq!(
            publisher.publish::<AckTaskSpec>(()).await.unwrap().task_id,
            id as u64
        );
    }
    publisher.close().await.unwrap();
    admin.close().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_publishing_serial_pool_and_shared_close() {
    let database = TestDatabase::new("publishing_pool").await;
    initialize_postgres_schema(database.url(), "public")
        .await
        .unwrap();
    let mut admin = PgConnection::connect(database.url()).await.unwrap();
    let app = format!("publishing_{}", unique_suffix());
    let publisher =
        PostgresPublishingBackend::connect(&format!("{}?application_name={app}", database.url()))
            .await
            .unwrap();
    for _ in 0..8 {
        publisher.publish::<AckTaskSpec>(()).await.unwrap();
        // SQLx returns a pooled connection asynchronously; let this serial workload settle.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    wait_for_publisher_connections(&mut admin, &app, 1).await;
    let queries: Vec<String> =
        sqlx::query_scalar("SELECT query FROM pg_stat_activity WHERE application_name = $1")
            .bind(&app)
            .fetch_all(&mut admin)
            .await
            .unwrap();
    assert!(
        queries
            .iter()
            .all(|query| !query.to_uppercase().contains("LISTEN"))
    );
    let clone = publisher.clone();
    clone.close().await.unwrap();
    publisher.close().await.unwrap();
    wait_for_publisher_connections(&mut admin, &app, 0).await;
    for future in [false, true] {
        let result = if future {
            publisher
                .publish_future::<AckTaskSpec>((), Instant::now())
                .await
        } else {
            publisher.publish::<AckTaskSpec>(()).await
        };
        let PublishTaskError::Backend(error) = result.unwrap_err();
        assert!(matches!(
            error.source().unwrap().downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::PoolClosed)
        ));
    }
    admin.close().await.unwrap();
    database.cleanup().await;
}

#[tokio::test]
async fn test_postgres_publishing_close_waits_for_gated_insert() {
    let database = TestDatabase::new("publishing_close").await;
    initialize_postgres_schema(database.url(), "public")
        .await
        .unwrap();
    let mut admin = PgConnection::connect(database.url()).await.unwrap();
    let app = format!("publishing_{}", unique_suffix());
    let publisher =
        PostgresPublishingBackend::connect(&format!("{}?application_name={app}", database.url()))
            .await
            .unwrap();
    admin
        .execute("BEGIN; LOCK TABLE bellows_tasks IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let clone = publisher.clone();
    let insert = tokio::spawn(async move { clone.publish::<AckTaskSpec>(()).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            admin.execute("SELECT pg_stat_clear_snapshot()").await.unwrap();
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock')")
                .bind(&app).fetch_one(&mut admin).await.unwrap();
            if blocked { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("insert never reached the SQL gate");
    let mut close = Box::pin(publisher.close());
    std::future::poll_fn(|cx| {
        assert!(close.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(!insert.is_finished());
    admin.execute("COMMIT").await.unwrap();
    let receipt = tokio::time::timeout(Duration::from_secs(5), insert)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), close)
        .await
        .unwrap()
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM bellows_tasks WHERE task_id = $1")
        .bind(receipt.task_id as i64)
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(count, 1);
    wait_for_publisher_connections(&mut admin, &app, 0).await;
    admin.close().await.unwrap();
    database.cleanup().await;
}

async fn wait_for_publisher_connections(admin: &mut PgConnection, app: &str, expected: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
            )
            .bind(app)
            .fetch_one(&mut *admin)
            .await
            .unwrap();
            if count == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("publisher connections did not settle");
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
    backend.close().await.unwrap();
    backend.clone().close().await.unwrap();
    assert!(matches!(backend.fail(17, task_id, None).await,
        Err(FailTaskError::Backend(error))
        if matches!(error.source().and_then(|source| source.downcast_ref::<sqlx::Error>()),
            Some(sqlx::Error::PoolClosed))));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
            )
            .bind(&application_name)
            .fetch_one(&mut admin)
            .await
            .unwrap();
            if count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("execution pool must close its connections across all clones");
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
