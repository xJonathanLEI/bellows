#![cfg(feature = "in_memory")]

use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bellows::{
    ActivationStrategy, Backend, PublishActivationStrategy, PublishDispatchToken, PublishTrigger,
    SingletonTrigger, TaskDefinition, TaskExecutionBackend, TaskFailure, TaskResult, TaskSuccess,
    Worker, WorkerFactory,
    backends::{
        ClaimTaskError, ClaimedTask, FailTaskError, FailedTask, FinishTaskError, FinishedTask,
        RenewTaskError, RenewedTaskLease, in_memory::InMemoryBackend,
    },
    run_task_once,
};
use tokio::sync::{Semaphore, mpsc};

#[derive(Clone)]
struct Published;

impl TaskDefinition for Published {
    const NAME: &str = "runtime_published";
    type Callback = ();
    type Trigger = PublishTrigger<()>;
}

#[derive(Clone)]
struct Singleton;

impl TaskDefinition for Singleton {
    const NAME: &str = "runtime_singleton";
    type Callback = ();
    type Trigger = SingletonTrigger;
}

#[derive(Clone)]
struct Factory<T> {
    builds: Arc<AtomicUsize>,
    started: mpsc::UnboundedSender<(u64, u64)>,
    gate: Arc<Semaphore>,
    dropped: Arc<Semaphore>,
    result: TaskResult<()>,
    task: PhantomData<fn() -> T>,
}

impl<T> Factory<T> {
    fn new() -> (Self, mpsc::UnboundedReceiver<(u64, u64)>) {
        let (started, rx) = mpsc::unbounded_channel();
        (
            Self {
                builds: Arc::new(AtomicUsize::new(0)),
                started,
                gate: Arc::new(Semaphore::new(0)),
                dropped: Arc::new(Semaphore::new(0)),
                result: Ok(TaskSuccess::done(())),
                task: PhantomData,
            },
            rx,
        )
    }
}

struct TestWorker<T> {
    worker_id: u64,
    factory: Factory<T>,
}

impl<T> WorkerFactory for Factory<T>
where
    T: TaskDefinition<Callback = ()> + Clone + 'static,
{
    type Worker = TestWorker<T>;

    fn build(&self, worker_id: u64) -> Self::Worker {
        self.builds.fetch_add(1, Ordering::SeqCst);
        TestWorker {
            worker_id,
            factory: self.clone(),
        }
    }
}

impl<T> Worker for TestWorker<T>
where
    T: TaskDefinition<Callback = ()> + 'static,
{
    type Task = T;

    async fn process(
        self,
        task_id: u64,
        _payload: <T::Trigger as ActivationStrategy>::EffectivePayload,
    ) -> TaskResult<()> {
        self.factory
            .started
            .send((self.worker_id, task_id))
            .unwrap();
        self.factory.gate.acquire().await.unwrap().forget();
        self.factory.result
    }
}

impl<T> Drop for TestWorker<T> {
    fn drop(&mut self) {
        self.factory.dropped.add_permits(1);
    }
}

#[derive(Clone, Copy, Default)]
enum Renewal {
    #[default]
    Normal,
    Due,
    Lost,
    Error,
}

// Deliberately implements only TaskExecutionBackend, not Backend.
#[derive(Clone)]
struct ExecutionOnly {
    inner: InMemoryBackend,
    claim_error: bool,
    finish_error: bool,
    fail_error: bool,
    recording: Arc<Semaphore>,
    recording_gate: Option<Arc<Semaphore>>,
    renewal: Renewal,
    renewing: Arc<Semaphore>,
    renewal_gate: Arc<Semaphore>,
}

impl ExecutionOnly {
    fn new(inner: InMemoryBackend) -> Self {
        Self {
            inner,
            claim_error: false,
            finish_error: false,
            fail_error: false,
            recording: Arc::new(Semaphore::new(0)),
            recording_gate: None,
            renewal: Renewal::Normal,
            renewing: Arc::new(Semaphore::new(0)),
            renewal_gate: Arc::new(Semaphore::new(0)),
        }
    }

    async fn record(&self) {
        self.recording.add_permits(1);
        if let Some(gate) = &self.recording_gate {
            gate.acquire().await.unwrap().forget();
        }
    }
}

impl TaskExecutionBackend for ExecutionOnly {
    async fn claim_published<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<<T::Trigger as PublishActivationStrategy>::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        if self.claim_error {
            return Err(ClaimTaskError::Backend("injected claim error".into()));
        }
        let mut claimed = self
            .inner
            .claim_published::<T>(worker_id, task_id, lease_expiration)
            .await?;
        if !matches!(self.renewal, Renewal::Normal) {
            claimed.lease_expiration = Instant::now();
        }
        Ok(claimed)
    }

    async fn claim_earliest_published<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<<T::Trigger as PublishActivationStrategy>::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.inner
            .claim_earliest_published::<T>(worker_id, lease_expiration)
            .await
    }

    async fn claim_singleton<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<()>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        self.inner
            .claim_singleton::<T>(worker_id, lease_expiration)
            .await
    }

    async fn renew(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<RenewedTaskLease, RenewTaskError> {
        self.renewing.add_permits(1);
        self.renewal_gate.acquire().await.unwrap().forget();
        match self.renewal {
            Renewal::Lost => Err(RenewTaskError::LeaseLost),
            Renewal::Error => Err(RenewTaskError::Backend("injected renewal error".into())),
            Renewal::Normal | Renewal::Due => {
                self.inner.renew(worker_id, task_id, lease_expiration).await
            }
        }
    }

    async fn fail(
        &self,
        worker_id: u64,
        task_id: u64,
        available_from: Option<Instant>,
    ) -> Result<FailedTask, FailTaskError> {
        self.record().await;
        if self.fail_error {
            return Err(FailTaskError::Backend("injected failure error".into()));
        }
        self.inner.fail(worker_id, task_id, available_from).await
    }

    async fn finish<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        callback_payload: T::Callback,
        available_from: Option<Instant>,
    ) -> Result<FinishedTask, FinishTaskError>
    where
        T: TaskDefinition,
    {
        self.record().await;
        if self.finish_error {
            return Err(FinishTaskError::Backend("injected finish error".into()));
        }
        self.inner
            .finish::<T>(worker_id, task_id, callback_payload, available_from)
            .await
    }
}

fn lease_expiration() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

#[tokio::test]
async fn waits_for_worker_and_removes_task() {
    let backend = InMemoryBackend::new();
    let task = backend.publish::<Published>(()).await.unwrap();
    let (factory, mut started) = Factory::<Published>::new();
    let execution = tokio::spawn(run_task_once(
        backend.clone(),
        factory.clone(),
        17,
        PublishDispatchToken::Task(task.task_id),
    ));
    assert_eq!(started.recv().await, Some((17, task.task_id)));
    assert!(!execution.is_finished());
    factory.gate.add_permits(1);
    let (): () = execution.await.unwrap();
    assert!(matches!(
        backend
            .claim_published::<Published>(18, task.task_id, lease_expiration())
            .await,
        Err(ClaimTaskError::TaskNotFound)
    ));
}

#[tokio::test]
async fn execution_only_backend_waits_for_completion_and_failure_recording() {
    for failed in [false, true] {
        let inner = InMemoryBackend::new();
        let task = inner.publish::<Published>(()).await.unwrap();
        let mut backend = ExecutionOnly::new(inner.clone());
        let gate = Arc::new(Semaphore::new(0));
        backend.recording_gate = Some(gate.clone());
        let (mut factory, _started) = Factory::<Published>::new();
        factory.gate.add_permits(1);
        if failed {
            factory.result = Err(TaskFailure::retry_immediately());
        }
        let execution = tokio::spawn(run_task_once(
            backend.clone(),
            factory,
            17,
            PublishDispatchToken::Task(task.task_id),
        ));
        backend.recording.acquire().await.unwrap().forget();
        assert!(!execution.is_finished());
        assert!(matches!(
            inner
                .claim_published::<Published>(18, task.task_id, lease_expiration())
                .await,
            Err(ClaimTaskError::TaskLeased { .. })
        ));
        gate.add_permits(1);
        execution.await.unwrap();
        let claim = inner
            .claim_published::<Published>(18, task.task_id, lease_expiration())
            .await;
        if failed {
            assert!(claim.is_ok());
        } else {
            assert!(matches!(claim, Err(ClaimTaskError::TaskNotFound)));
        }
    }
}

#[tokio::test]
async fn earliest_available_executes_only_one_task() {
    let backend = InMemoryBackend::new();
    let first = backend.publish::<Published>(()).await.unwrap();
    let second = backend.publish::<Published>(()).await.unwrap();
    let (factory, mut started) = Factory::<Published>::new();
    factory.gate.add_permits(2);
    run_task_once(
        backend.clone(),
        factory.clone(),
        17,
        PublishDispatchToken::EarliestAvailable,
    )
    .await;
    assert_eq!(started.recv().await, Some((17, first.task_id)));
    assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend
            .claim_earliest_published::<Published>(18, lease_expiration())
            .await
            .unwrap()
            .task_id,
        second.task_id
    );
}

#[tokio::test]
async fn singleton_uses_unit_dispatch_token() {
    let backend = InMemoryBackend::new();
    let (factory, mut started) = Factory::<Singleton>::new();
    factory.gate.add_permits(1);
    run_task_once(ExecutionOnly::new(backend.clone()), factory.clone(), 17, ()).await;
    let (worker_id, task_id) = started.recv().await.unwrap();
    assert_eq!(worker_id, 17);
    assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend
            .claim_singleton::<Singleton>(18, lease_expiration())
            .await
            .unwrap()
            .task_id,
        task_id
    );
}

#[tokio::test]
async fn unsuccessful_claims_never_build_a_worker() {
    let backend = InMemoryBackend::new();
    let leased = backend.publish::<Published>(()).await.unwrap();
    backend
        .claim_published::<Published>(18, leased.task_id, lease_expiration())
        .await
        .unwrap();
    let future = backend
        .publish_future::<Published>((), lease_expiration())
        .await
        .unwrap();
    let (factory, _started) = Factory::<Published>::new();
    for token in [
        PublishDispatchToken::Task(u64::MAX),
        PublishDispatchToken::Task(leased.task_id),
        PublishDispatchToken::Task(future.task_id),
        PublishDispatchToken::EarliestAvailable,
    ] {
        run_task_once(backend.clone(), factory.clone(), 17, token).await;
    }
    assert_eq!(factory.builds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn retries_and_rescheduling_require_an_external_attempt() {
    for result in [
        Err(TaskFailure::retry_immediately()),
        Err(TaskFailure::retry_at(lease_expiration())),
        Ok(TaskSuccess::schedule_next_run((), lease_expiration())),
    ] {
        let backend = InMemoryBackend::new();
        let task = backend.publish::<Published>(()).await.unwrap();
        let token = PublishDispatchToken::Task(task.task_id);
        let (mut factory, _started) = Factory::<Published>::new();
        factory.result = result;
        factory.gate.add_permits(2);
        run_task_once(backend.clone(), factory.clone(), 17, token).await;
        assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
        factory.result = Ok(TaskSuccess::done(()));
        run_task_once(backend.clone(), factory.clone(), 18, token).await;
        let scheduled = match result {
            Ok(success) => success.available_from,
            Err(failure) => failure.available_from,
        };
        let claim = backend
            .claim_published::<Published>(19, task.task_id, lease_expiration())
            .await;
        if let Some(expected) = scheduled {
            assert_eq!(factory.builds.load(Ordering::SeqCst), 1);
            assert!(matches!(
                claim,
                Err(ClaimTaskError::TaskUnavailable { available_from: Some(actual) })
                    if actual == expected
            ));
        } else {
            assert_eq!(factory.builds.load(Ordering::SeqCst), 2);
            assert!(matches!(claim, Err(ClaimTaskError::TaskNotFound)));
        }
    }
}

#[tokio::test]
async fn backend_errors_return_unit() {
    for operation in ["claim", "finish", "fail"] {
        let inner = InMemoryBackend::new();
        let task = inner.publish::<Published>(()).await.unwrap();
        let mut backend = ExecutionOnly::new(inner.clone());
        backend.claim_error = operation == "claim";
        backend.finish_error = operation == "finish";
        backend.fail_error = operation == "fail";
        let (mut factory, _started) = Factory::<Published>::new();
        factory.gate.add_permits(1);
        if backend.fail_error {
            factory.result = Err(TaskFailure::retry_immediately());
        }
        let (): () = run_task_once(
            backend,
            factory.clone(),
            17,
            PublishDispatchToken::Task(task.task_id),
        )
        .await;
        assert_eq!(
            factory.builds.load(Ordering::SeqCst),
            usize::from(operation != "claim")
        );
        let claim = inner
            .claim_published::<Published>(18, task.task_id, lease_expiration())
            .await;
        if operation == "claim" {
            assert!(claim.is_ok());
        } else {
            assert!(matches!(claim, Err(ClaimTaskError::TaskLeased { .. })));
        }
    }
}

#[tokio::test]
async fn renews_while_processing_and_aborts_worker_on_renewal_failure() {
    for renewal in [Renewal::Due, Renewal::Lost, Renewal::Error] {
        let inner = InMemoryBackend::new();
        let task = inner.publish::<Published>(()).await.unwrap();
        let mut backend = ExecutionOnly::new(inner.clone());
        backend.renewal = renewal;
        let (factory, mut started) = Factory::<Published>::new();
        let execution = tokio::spawn(run_task_once(
            backend.clone(),
            factory.clone(),
            17,
            PublishDispatchToken::Task(task.task_id),
        ));
        assert_eq!(started.recv().await, Some((17, task.task_id)));
        backend.renewing.acquire().await.unwrap().forget();
        assert!(!execution.is_finished());
        backend.renewal_gate.add_permits(1);
        if matches!(renewal, Renewal::Due) {
            factory.gate.add_permits(1);
        }
        execution.await.unwrap();
        // Abortion is asynchronous; wait for the worker's drop rather than assuming it already ran.
        factory.dropped.acquire().await.unwrap().forget();
        let claim = inner
            .claim_published::<Published>(18, task.task_id, lease_expiration())
            .await;
        if matches!(renewal, Renewal::Due) {
            assert_eq!(backend.recording.available_permits(), 1);
            assert!(matches!(claim, Err(ClaimTaskError::TaskNotFound)));
        } else {
            assert_eq!(factory.gate.available_permits(), 0);
            assert_eq!(backend.recording.available_permits(), 0);
            assert!(matches!(claim, Err(ClaimTaskError::TaskLeased { .. })));
        }
    }
}
