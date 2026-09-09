//! Executable contracts for Bellows' real wasm platform, with controlled backend I/O.
//! No native executor, production lease overrides, or multi-request runtime control state.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bellows::{
    PublishActivationStrategy, PublishDispatchToken, PublishTrigger, TaskDefinition,
    TaskExecutionBackend, TaskFailure, TaskResult, TaskSuccess, Worker, WorkerFactory,
    backends::{
        ClaimTaskError, ClaimedTask, FailTaskError, FailedTask, FinishTaskError, FinishedTask,
        RenewTaskError, RenewedTaskLease,
    },
    run_task_once,
    time::Instant,
};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, oneshot};

struct Task;
impl TaskDefinition for Task {
    const NAME: &'static str = "wasm_contract";
    type Callback = ();
    type Trigger = PublishTrigger<String>;
}

struct Signals {
    started: Semaphore,
    progress: Semaphore,
    progressed: Semaphore,
    complete: Semaphore,
    dropped: Semaphore,
    renewing: Semaphore,
    renew: Semaphore,
    renewed: Semaphore,
    recording: Semaphore,
    record: Semaphore,
    builds: AtomicUsize,
    renewals: AtomicUsize,
    finishes: AtomicUsize,
    failures: AtomicUsize,
    mode: String,
}

impl Signals {
    fn new(mode: &str) -> Self {
        Self {
            started: Semaphore::new(0),
            progress: Semaphore::new(0),
            progressed: Semaphore::new(0),
            complete: Semaphore::new(0),
            dropped: Semaphore::new(0),
            renewing: Semaphore::new(0),
            renew: Semaphore::new(0),
            renewed: Semaphore::new(0),
            recording: Semaphore::new(0),
            record: Semaphore::new(0),
            builds: AtomicUsize::new(0),
            renewals: AtomicUsize::new(0),
            finishes: AtomicUsize::new(0),
            failures: AtomicUsize::new(0),
            mode: mode.into(),
        }
    }
}

async fn take(signal: &Semaphore) {
    signal.acquire().await.unwrap().forget();
}

#[derive(Clone)]
struct Backend(Arc<Signals>);
struct Factory(Arc<Signals>);
struct Processing(Arc<Signals>);

impl WorkerFactory for Factory {
    type Worker = Processing;
    fn build(&self, worker_id: u64) -> Self::Worker {
        assert_eq!(worker_id, 42);
        self.0.builds.fetch_add(1, Ordering::SeqCst);
        Processing(self.0.clone())
    }
}

impl Worker for Processing {
    type Task = Task;
    async fn process(self, task_id: u64, payload: String) -> TaskResult<()> {
        assert_eq!(task_id, 7);
        assert_eq!(payload, "database payload");
        self.0.started.add_permits(1);
        take(&self.0.progress).await;
        self.0.progressed.add_permits(1);
        take(&self.0.complete).await;
        if self.0.mode == "fail" {
            Err(TaskFailure::retry_immediately())
        } else {
            Ok(TaskSuccess::done(()))
        }
    }
}

impl Drop for Processing {
    fn drop(&mut self) {
        self.0.dropped.add_permits(1);
    }
}

impl TaskExecutionBackend for Backend {
    async fn claim_published<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        expiration: Instant,
    ) -> Result<ClaimedTask<<T::Trigger as PublishActivationStrategy>::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        assert_eq!((worker_id, task_id, T::NAME), (42, 7, Task::NAME));
        assert!(expiration > Instant::now() + Duration::from_secs(19));
        if self.0.mode == "no-claim" {
            return Err(ClaimTaskError::TaskNotFound);
        }
        Ok(ClaimedTask {
            task_id,
            task_payload: serde_json::from_str("\"database payload\"").unwrap(),
            // Exercise the real platform deadline timer without changing Bellows' constants.
            lease_expiration: Instant::now() + Duration::from_millis(10_005),
        })
    }

    async fn claim_earliest_published<T>(
        &self,
        _: u64,
        _: Instant,
    ) -> Result<ClaimedTask<<T::Trigger as PublishActivationStrategy>::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        panic!("contract must use an explicit dispatch token");
    }

    async fn claim_singleton<T: TaskDefinition>(
        &self,
        _: u64,
        _: Instant,
    ) -> Result<ClaimedTask<()>, ClaimTaskError> {
        panic!("contract must use a published task");
    }

    async fn renew(
        &self,
        worker_id: u64,
        task_id: u64,
        expiration: Instant,
    ) -> Result<RenewedTaskLease, RenewTaskError> {
        assert_eq!((worker_id, task_id), (42, 7));
        assert!(expiration > Instant::now() + Duration::from_secs(19));
        self.0.renewing.add_permits(1);
        take(&self.0.renew).await;
        match self.0.mode.as_str() {
            "lost" | "lost-completed" => Err(RenewTaskError::LeaseLost),
            "error" | "error-completed" => {
                Err(RenewTaskError::Backend("controlled renewal error".into()))
            }
            _ => {
                self.0.renewals.fetch_add(1, Ordering::SeqCst);
                self.0.renewed.add_permits(1);
                Ok(RenewedTaskLease {
                    new_expiration: expiration,
                })
            }
        }
    }

    async fn fail(
        &self,
        worker_id: u64,
        task_id: u64,
        available_from: Option<Instant>,
    ) -> Result<FailedTask, FailTaskError> {
        assert_eq!((worker_id, task_id, available_from), (42, 7, None));
        self.0.recording.add_permits(1);
        take(&self.0.record).await;
        self.0.failures.fetch_add(1, Ordering::SeqCst);
        Ok(FailedTask { task_id })
    }

    async fn finish<T: TaskDefinition>(
        &self,
        worker_id: u64,
        task_id: u64,
        _: T::Callback,
        available_from: Option<Instant>,
    ) -> Result<FinishedTask, FinishTaskError> {
        assert_eq!((worker_id, task_id, available_from), (42, 7, None));
        self.0.recording.add_permits(1);
        take(&self.0.record).await;
        self.0.finishes.fetch_add(1, Ordering::SeqCst);
        Ok(FinishedTask { task_id })
    }
}

pub async fn run(mode: &str) -> Value {
    let signals = Arc::new(Signals::new(mode));
    let (send, mut done) = oneshot::channel();
    let backend = Backend(signals.clone());
    let factory = Factory(signals.clone());
    let before = Instant::now();
    worker::wasm_bindgen_futures::spawn_local(async move {
        run_task_once(backend, factory, 42, PublishDispatchToken::Task(7)).await;
        send.send(()).unwrap();
    });
    if mode == "no-claim" {
        done.await.unwrap();
        assert_eq!(signals.builds.load(Ordering::SeqCst), 0);
    } else {
        take(&signals.started).await;
        take(&signals.renewing).await;
        assert!(before.elapsed() >= Duration::from_millis(5));
        // Renewal is pending. An inline/unspawned worker would deadlock this contract.
        signals.progress.add_permits(1);
        take(&signals.progressed).await;
        let successful_renewal = matches!(mode, "finish" | "fail" | "finish-running");
        if matches!(mode, "finish" | "fail") || mode.ends_with("-completed") {
            signals.complete.add_permits(1);
            take(&signals.dropped).await;
        }
        assert_eq!(signals.recording.available_permits(), 0);
        assert!(matches!(
            done.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        signals.renew.add_permits(1);
        if mode == "finish-running" {
            take(&signals.renewed).await;
            assert_eq!(signals.dropped.available_permits(), 0);
            signals.complete.add_permits(1);
            take(&signals.dropped).await;
        }
        if successful_renewal {
            take(&signals.recording).await;
            assert!(matches!(
                done.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            signals.record.add_permits(1);
        }
        done.await.unwrap();
        if !successful_renewal && !mode.ends_with("-completed") {
            // Await actual destruction of the spawned future, not just run_task_once's return.
            take(&signals.dropped).await;
        }
        assert_eq!(signals.builds.load(Ordering::SeqCst), 1);
        assert_eq!(
            signals.renewals.load(Ordering::SeqCst),
            usize::from(successful_renewal)
        );
        assert_eq!(
            signals.finishes.load(Ordering::SeqCst),
            usize::from(matches!(mode, "finish" | "finish-running"))
        );
        assert_eq!(
            signals.failures.load(Ordering::SeqCst),
            usize::from(mode == "fail")
        );
        assert_eq!(signals.recording.available_permits(), 0);
    }
    json!({ "ok": true, "mode": mode, "builds": signals.builds.load(Ordering::SeqCst),
        "renewals": signals.renewals.load(Ordering::SeqCst),
        "finishes": signals.finishes.load(Ordering::SeqCst),
        "failures": signals.failures.load(Ordering::SeqCst) })
}
