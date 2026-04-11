use std::collections::VecDeque;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedReceiver as MpscReceiver, UnboundedSender as MpscSender};

use crate::runtime::WorkerRuntime;
use crate::{
    ActivationStrategy, Backend, PublishTrigger, SingletonTrigger, TaskDefinition, Worker,
    WorkerFactory,
    backends::{
        BackendSignalSubscription, NewTaskAvailableSignalPayload, SubscribeError, SweepTasksError,
    },
};

pub struct WorkerDispatcher<B, F> {
    backend: B,
    factory: Arc<F>,
}

impl<B, F> WorkerDispatcher<B, F> {
    pub fn new(backend: B, factory: F) -> Self {
        Self {
            backend,
            factory: Arc::new(factory),
        }
    }
}

impl<B, F> WorkerDispatcher<B, F>
where
    B: Backend + 'static,
    F: WorkerFactory + 'static,
    F::Worker: 'static,
    <F::Worker as Worker>::Task: 'static,
{
    pub async fn launch(self) -> Result<WorkerDispatcherHandle, WorkerDispatcherLaunchError>
    where
        <<F::Worker as Worker>::Task as TaskDefinition>::Trigger: SignalDispatch,
    {
        let drain_signal = Arc::new(Notify::const_new());
        let drained_signal = Arc::new(Notify::const_new());
        let (finished_tx, finished_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let subscription = self
            .backend
            .subscribe::<<F::Worker as Worker>::Task>()
            .await
            .map_err(WorkerDispatcherLaunchError::SubscribeFailed)?;
        let swept_tasks =
            <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as ActivationStrategy>::sweep_tasks::<B, <F::Worker as Worker>::Task>(&self.backend)
                .await
                .map_err(WorkerDispatcherLaunchError::SweepFailed)?;

        let daemon = Daemon {
            context: DaemonContext {
                backend: self.backend,
                factory: self.factory,
                subscription,
                drain_signal: drain_signal.clone(),
                drained_signal: drained_signal.clone(),
                finished_tx,
                finished_rx,
            },
            state: DaemonState {
                draining: false,
                pending_workers: 0,
                next_worker_id: 0,
                startup_tasks: swept_tasks.into_iter().collect::<VecDeque<_>>(),
            },
        };

        tokio::spawn(daemon.run());

        Ok(WorkerDispatcherHandle {
            drain_signal,
            drained_signal,
        })
    }
}

#[derive(Debug)]
pub enum WorkerDispatcherLaunchError {
    SubscribeFailed(SubscribeError),
    SweepFailed(SweepTasksError),
}

impl Display for WorkerDispatcherLaunchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SubscribeFailed(error) => write!(f, "subscription failed: {error}"),
            Self::SweepFailed(error) => write!(f, "sweep failed: {error}"),
        }
    }
}

impl StdError for WorkerDispatcherLaunchError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::SubscribeFailed(error) => Some(error),
            Self::SweepFailed(error) => Some(error),
        }
    }
}

/// A handle for controlling the background worker dispatcher task.
///
/// This handle can be used to request the dispatcher to gracefully stop by draining all in-flight
/// workers.
#[derive(Debug)]
pub struct WorkerDispatcherHandle {
    drain_signal: Arc<Notify>,
    drained_signal: Arc<Notify>,
}

impl WorkerDispatcherHandle {
    /// Stops the dispatcher from claiming new tasks immediately and waits for all in-flight tasks
    /// to finish processing.
    pub async fn drain(self) {
        self.drain_signal.notify_one();
        self.drained_signal.notified().await;
    }
}

impl Drop for WorkerDispatcherHandle {
    fn drop(&mut self) {
        self.drain_signal.notify_one();
    }
}

struct Daemon<B, F>
where
    F: WorkerFactory,
{
    context: DaemonContext<B, F>,
    state: DaemonState<<F::Worker as Worker>::Task>,
}

impl<B, F> Daemon<B, F>
where
    B: Backend + 'static,
    F: WorkerFactory + 'static,
    F::Worker: 'static,
    <F::Worker as Worker>::Task: 'static,
    <<F::Worker as Worker>::Task as TaskDefinition>::Trigger: SignalDispatch,
{
    async fn run(mut self) {
        while let EventLoopResult::Continue =
            Self::event_loop(&mut self.context, &mut self.state).await
        {}

        self.context.drained_signal.notify_one();
    }

    async fn event_loop(
        ctx: &mut DaemonContext<B, F>,
        state: &mut DaemonState<<F::Worker as Worker>::Task>,
    ) -> EventLoopResult {
        if !state.draining
            && let Some(dispatch_token) = state.startup_tasks.pop_front()
        {
            return Self::dispatch_task(dispatch_token, ctx, state);
        }

        tokio::select! {
            biased;

            _ = ctx.drain_signal.notified() => Self::handle_drain(ctx, state),
            _ = ctx.finished_rx.recv() => Self::handle_finished(ctx, state),
            sub = ctx.subscription.recv() => Self::handle_sub(sub, ctx, state),
        }
    }

    fn handle_sub(
        sub: Result<NewTaskAvailableSignalPayload, tokio::sync::broadcast::error::RecvError>,
        ctx: &DaemonContext<B, F>,
        state: &mut DaemonState<<F::Worker as Worker>::Task>,
    ) -> EventLoopResult {
        match sub {
            Ok(signal) => {
                let dispatch_token =
                    <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as SignalDispatch>::from_signal(signal);
                Self::dispatch_task(dispatch_token, ctx, state)
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => EventLoopResult::Continue,
            Err(_) => EventLoopResult::Exit,
        }
    }

    fn dispatch_task(
        dispatch_token: <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as ActivationStrategy>::DispatchToken,
        ctx: &DaemonContext<B, F>,
        state: &mut DaemonState<<F::Worker as Worker>::Task>,
    ) -> EventLoopResult {
        // Only start new work if not draining
        if !state.draining {
            let runtime = WorkerRuntime {
                backend: ctx.backend.clone(),
                factory: ctx.factory.clone(),
                worker_id: state.next_worker_id,
                finished_signal: ctx.finished_tx.clone(),
            };
            runtime.run(dispatch_token);

            state.pending_workers += 1;
            state.next_worker_id += 1;
        }

        EventLoopResult::Continue
    }

    fn handle_finished(
        _ctx: &DaemonContext<B, F>,
        state: &mut DaemonState<<F::Worker as Worker>::Task>,
    ) -> EventLoopResult {
        state.pending_workers -= 1;
        if state.draining && state.pending_workers == 0 {
            EventLoopResult::Exit
        } else {
            EventLoopResult::Continue
        }
    }

    fn handle_drain(
        _ctx: &DaemonContext<B, F>,
        state: &mut DaemonState<<F::Worker as Worker>::Task>,
    ) -> EventLoopResult {
        if state.pending_workers == 0 {
            EventLoopResult::Exit
        } else {
            state.draining = true;
            EventLoopResult::Continue
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventLoopResult {
    Continue,
    Exit,
}

struct DaemonContext<B, F>
where
    F: WorkerFactory,
{
    backend: B,
    factory: Arc<F>,
    subscription: BackendSignalSubscription<<F::Worker as Worker>::Task>,
    drain_signal: Arc<Notify>,
    drained_signal: Arc<Notify>,
    finished_tx: MpscSender<()>,
    finished_rx: MpscReceiver<()>,
}

struct DaemonState<T>
where
    T: TaskDefinition,
{
    draining: bool,
    pending_workers: usize,
    next_worker_id: u64,
    startup_tasks: VecDeque<<T::Trigger as ActivationStrategy>::DispatchToken>,
}

#[doc(hidden)]
pub trait SignalDispatch: ActivationStrategy {
    fn from_signal(signal: NewTaskAvailableSignalPayload) -> Self::DispatchToken;
}

impl<Payload> SignalDispatch for PublishTrigger<Payload>
where
    Payload: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    fn from_signal(signal: NewTaskAvailableSignalPayload) -> Self::DispatchToken {
        signal.task_id
    }
}

impl SignalDispatch for SingletonTrigger {
    fn from_signal(_signal: NewTaskAvailableSignalPayload) -> Self::DispatchToken {}
}
