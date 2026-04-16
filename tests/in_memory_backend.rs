#![cfg(feature = "in_memory")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use bellows::{
    Backend, PublishTrigger, SingletonTrigger, TaskDefinition, TaskFailure, Worker, WorkerFactory,
    backends::in_memory::InMemoryBackend, dispatcher::WorkerDispatcher,
};
use serde::{Deserialize, Serialize};
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

    async fn process(
        self,
        task_id: u64,
        task_payload: EchoTaskPayload,
    ) -> Result<String, TaskFailure> {
        self.processed_tx
            .send(ProcessedTask {
                task_id,
                name: task_payload.name.clone(),
            })
            .expect("processed task collector should remain available during the test");

        Ok(task_payload.name)
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

    async fn process(self, task_id: u64, _task_payload: ()) -> Result<(), TaskFailure> {
        self.processed_tx
            .send(task_id)
            .expect("ack task collector should remain available during the test");
        Ok(())
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

    async fn process(self, task_id: u64, _task_payload: ()) -> Result<(), TaskFailure> {
        self.processed_tx
            .send(task_id)
            .expect("processed task collector should remain available during the test");
        self.release_signal
            .acquire()
            .await
            .expect("singleton worker gate semaphore should remain available")
            .forget();
        Ok(())
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

    async fn process(self, task_id: u64, _task_payload: ()) -> Result<(), TaskFailure> {
        self.started_tx
            .send(task_id)
            .expect("blocking task collector should remain available during the test");
        self.release_signal
            .acquire()
            .await
            .expect("blocking worker gate semaphore should remain available")
            .forget();
        Ok(())
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

    async fn process(self, task_id: u64, _task_payload: ()) -> Result<(), TaskFailure> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Err(TaskFailure::retry_immediately())
        } else {
            self.processed_tx
                .send(task_id)
                .expect("retry task collector should remain available during the test");
            Ok(())
        }
    }
}

#[tokio::test]
async fn test_in_memory_backend() {
    let backend = InMemoryBackend::new();
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

    // Wait for all 3 tasks to be processed. This must be called before `drain()` to avoid a race
    // condition where the dispatcher hasn't managed to claim all the tasks before being drained
    assert_names_echoed(&mut processed_rx, &["Alice", "Bob", "Charlie"]).await;

    dispatcher_handle.drain().await;

    assert!(processed_rx.recv().await.is_none());
}

#[tokio::test]
async fn test_publish_awaitable_returns_typed_callback() {
    let backend = InMemoryBackend::new();
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
}

#[tokio::test]
async fn test_publish_awaitable_supports_unit_callback() {
    let backend = InMemoryBackend::new();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();
    let dispatcher = WorkerDispatcher::new(backend.clone(), AckWorkerFactory { processed_tx });
    let dispatcher_handle = dispatcher.launch().await.unwrap();

    let awaitable = backend.publish_awaitable::<AckTaskSpec>(()).await.unwrap();
    let task_id = awaitable.task_id();

    assert_eq!(awaitable.wait().await.unwrap(), ());
    assert_eq!(processed_rx.recv().await.unwrap(), task_id);

    dispatcher_handle.drain().await;
}

#[tokio::test]
async fn test_publish_future_delays_task_availability() {
    let backend = InMemoryBackend::new();
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
}

#[tokio::test]
async fn test_in_memory_singleton_task_dispatch() {
    let backend = InMemoryBackend::new();
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
    assert_eq!(first_task_id, 0);

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
}

#[tokio::test]
async fn test_dispatcher_drains_multiple_preexisting_tasks_without_waiting() {
    let backend = InMemoryBackend::new();
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
}

#[tokio::test]
async fn test_sweeping() {
    let backend = InMemoryBackend::new();
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::unbounded_channel();

    let factory = EchoWorkerFactory { processed_tx };
    let dispatcher = WorkerDispatcher::new(backend.clone(), factory);

    backend
        .publish::<EchoTaskSpec>(EchoTaskPayload {
            name: "Alice".to_string(),
        })
        .await
        .unwrap();

    // Launch dispatcher _after_ a task has already been published
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

    // All 3 tasks must have been processed despite the late dispatcher launch
    assert_names_echoed(&mut processed_rx, &["Alice", "Bob", "Charlie"]).await;

    dispatcher_handle.drain().await;

    assert!(processed_rx.recv().await.is_none());
}

#[tokio::test]
async fn test_worker_failure_is_retried() {
    let backend = InMemoryBackend::new();
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
