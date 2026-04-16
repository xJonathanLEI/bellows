use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedReceiver as MpscReceiver, UnboundedSender as MpscSender};

use crate::runtime::{RuntimeUpdate, WorkerRuntime};
use crate::{
    ActivationStrategy, Backend, PublishDispatchToken, PublishTrigger, SingletonTrigger,
    TaskDefinition, Worker, WorkerFactory,
    backends::{BackendSignalSubscription, NewTaskAvailableSignalPayload, SubscribeError},
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
        let (report_tx, report_rx) = tokio::sync::mpsc::unbounded_channel::<RuntimeReport>();

        let subscription = self
            .backend
            .subscribe::<<F::Worker as Worker>::Task>()
            .await
            .map_err(WorkerDispatcherLaunchError::SubscribeFailed)?;

        let daemon = Daemon {
            context: DaemonContext {
                backend: self.backend,
                factory: self.factory,
                subscription,
                drain_signal: drain_signal.clone(),
                drained_signal: drained_signal.clone(),
                report_tx,
                report_rx,
            },
            state: DaemonState {
                draining: false,
                pending_workers: 0,
                next_worker_id: 0,
                earliest_available_from: Some(Instant::now()),
                earliest_claim_in_flight: false,
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
}

impl Display for WorkerDispatcherLaunchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SubscribeFailed(error) => write!(f, "subscription failed: {error}"),
        }
    }
}

impl StdError for WorkerDispatcherLaunchError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::SubscribeFailed(error) => Some(error),
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
    state: DaemonState,
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

    async fn event_loop(ctx: &mut DaemonContext<B, F>, state: &mut DaemonState) -> EventLoopResult {
        if !state.draining
            && state.should_dispatch_earliest_now()
            && !state.earliest_claim_in_flight
        {
            return Self::dispatch_earliest_claim(ctx, state);
        }

        if let Some(available_from) = state.earliest_available_from {
            let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(available_from));
            tokio::pin!(sleep);

            tokio::select! {
                biased;

                _ = ctx.drain_signal.notified() => Self::handle_drain(state),
                report = ctx.report_rx.recv() => Self::handle_report(report, state),
                sub = ctx.subscription.recv() => Self::handle_sub(sub, ctx, state),
                _ = &mut sleep => {
                    if !state.draining && !state.earliest_claim_in_flight {
                        Self::dispatch_earliest_claim(ctx, state)
                    } else {
                        EventLoopResult::Continue
                    }
                },
            }
        } else {
            tokio::select! {
                biased;

                _ = ctx.drain_signal.notified() => Self::handle_drain(state),
                report = ctx.report_rx.recv() => Self::handle_report(report, state),
                sub = ctx.subscription.recv() => Self::handle_sub(sub, ctx, state),
            }
        }
    }

    fn handle_sub(
        sub: Result<NewTaskAvailableSignalPayload, tokio::sync::broadcast::error::RecvError>,
        ctx: &DaemonContext<B, F>,
        state: &mut DaemonState,
    ) -> EventLoopResult {
        match sub {
            Ok(signal) => {
                let now = Instant::now();

                if let Some(dispatch_token) =
                    <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as SignalDispatch>::try_dispatch_from_signal(signal, now)
                {
                    Self::dispatch_task(dispatch_token, ctx, state)
                } else {
                    state.note_earliest_available_from(Some(signal.available_from));
                    EventLoopResult::Continue
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                state.note_earliest_available_from(Some(Instant::now()));
                EventLoopResult::Continue
            }
            Err(_) => EventLoopResult::Exit,
        }
    }

    fn dispatch_earliest_claim(
        ctx: &DaemonContext<B, F>,
        state: &mut DaemonState,
    ) -> EventLoopResult {
        state.earliest_claim_in_flight = true;
        state.earliest_available_from = None;

        Self::dispatch_task_with_completion(
            <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as SignalDispatch>::next_available_dispatch_token(),
            ctx,
            state,
            true,
        )
    }

    fn dispatch_task(
        dispatch_token: <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as ActivationStrategy>::DispatchToken,
        ctx: &DaemonContext<B, F>,
        state: &mut DaemonState,
    ) -> EventLoopResult {
        Self::dispatch_task_with_completion(dispatch_token, ctx, state, false)
    }

    fn dispatch_task_with_completion(
        dispatch_token: <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as ActivationStrategy>::DispatchToken,
        ctx: &DaemonContext<B, F>,
        state: &mut DaemonState,
        clears_earliest_claim: bool,
    ) -> EventLoopResult {
        if !state.draining {
            let (runtime_update_tx, mut runtime_update_rx) =
                tokio::sync::mpsc::unbounded_channel::<RuntimeUpdate>();

            let runtime = WorkerRuntime {
                backend: ctx.backend.clone(),
                factory: ctx.factory.clone(),
                worker_id: state.next_worker_id,
                update_signal: runtime_update_tx,
            };
            runtime.run(dispatch_token);

            let report_tx = ctx.report_tx.clone();
            tokio::spawn(async move {
                let mut clears_earliest_claim = clears_earliest_claim;

                while let Some(update) = runtime_update_rx.recv().await {
                    let _ = report_tx.send(RuntimeReport::Update {
                        update,
                        clears_earliest_claim,
                    });
                    clears_earliest_claim = false;
                }

                let _ = report_tx.send(RuntimeReport::Exited {
                    clears_earliest_claim,
                });
            });

            state.pending_workers += 1;
            state.next_worker_id += 1;
        }

        EventLoopResult::Continue
    }

    fn handle_report(report: Option<RuntimeReport>, state: &mut DaemonState) -> EventLoopResult {
        if let Some(report) = report {
            match report {
                RuntimeReport::Update {
                    update,
                    clears_earliest_claim,
                } => {
                    if clears_earliest_claim {
                        state.earliest_claim_in_flight = false;
                    }

                    if clears_earliest_claim && update.claimed_task {
                        state.note_earliest_available_from(Some(Instant::now()));
                    }

                    if let Some(next_available_from_update) = update.next_available_from_update {
                        match next_available_from_update {
                            Some(available_from) => {
                                state.note_earliest_available_from(Some(available_from));
                            }
                            None if state.earliest_available_from.is_none() => {
                                state.note_earliest_available_from(None);
                            }
                            None => {}
                        }
                    }
                }
                RuntimeReport::Exited {
                    clears_earliest_claim,
                } => {
                    if clears_earliest_claim {
                        state.earliest_claim_in_flight = false;
                    }

                    if state.pending_workers > 0 {
                        state.pending_workers -= 1;
                    }
                }
            }
        }

        if state.draining && state.pending_workers == 0 {
            EventLoopResult::Exit
        } else {
            EventLoopResult::Continue
        }
    }

    fn handle_drain(state: &mut DaemonState) -> EventLoopResult {
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

enum RuntimeReport {
    Update {
        update: RuntimeUpdate,
        clears_earliest_claim: bool,
    },
    Exited {
        clears_earliest_claim: bool,
    },
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
    report_tx: MpscSender<RuntimeReport>,
    report_rx: MpscReceiver<RuntimeReport>,
}

struct DaemonState {
    draining: bool,
    pending_workers: usize,
    next_worker_id: u64,
    earliest_available_from: Option<Instant>,
    earliest_claim_in_flight: bool,
}

impl DaemonState {
    fn should_dispatch_earliest_now(&self) -> bool {
        self.earliest_available_from
            .is_some_and(|available_from| available_from <= Instant::now())
    }

    fn note_earliest_available_from(&mut self, available_from: Option<Instant>) {
        let Some(available_from) = available_from else {
            self.earliest_available_from = None;
            return;
        };

        match self.earliest_available_from {
            Some(current) if current <= available_from => {}
            _ => self.earliest_available_from = Some(available_from),
        }
    }
}

#[doc(hidden)]
pub trait SignalDispatch: ActivationStrategy {
    fn try_dispatch_from_signal(
        signal: NewTaskAvailableSignalPayload,
        now: Instant,
    ) -> Option<Self::DispatchToken>;

    fn next_available_dispatch_token() -> Self::DispatchToken;
}

impl<Payload> SignalDispatch for PublishTrigger<Payload>
where
    Payload: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    fn try_dispatch_from_signal(
        signal: NewTaskAvailableSignalPayload,
        now: Instant,
    ) -> Option<Self::DispatchToken> {
        (signal.available_from <= now)
            .then_some(signal.task_id)
            .flatten()
            .map(PublishDispatchToken::Task)
    }

    fn next_available_dispatch_token() -> Self::DispatchToken {
        PublishDispatchToken::EarliestAvailable
    }
}

impl SignalDispatch for SingletonTrigger {
    fn try_dispatch_from_signal(
        _signal: NewTaskAvailableSignalPayload,
        _now: Instant,
    ) -> Option<Self::DispatchToken> {
        None
    }

    fn next_available_dispatch_token() -> Self::DispatchToken {}
}
