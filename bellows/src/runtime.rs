use std::{sync::Arc, time::Duration};

use crate::{
    ActivationStrategy, ActivationStrategyKind, TaskDefinition, TaskExecutionBackend, TaskSuccess,
    Worker, WorkerFactory,
    backends::{ClaimTaskError, FailTaskError, FinishTaskError, RenewTaskError},
    platform,
    time::Instant,
};
use tokio::sync::mpsc::UnboundedSender as MpscSender;
use tracing::{trace, warn};

const LEASE_DURATION: Duration = Duration::from_secs(20);
const LEASE_RENEWAL_THRESHOLD: Duration = Duration::from_secs(10);

/// The next action for a host after a single task attempt, not a business success status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAttemptOutcome {
    /// The requested published task is absent or its completion was committed.
    Done,
    /// Recheck at the observed availability/lease deadline or committed rescheduling deadline.
    RetryAt { available_from: Instant },
    /// Ownership or backend state is uncertain; apply the host's infrastructure retry policy.
    Retry,
}

/// Executes and awaits one task attempt without starting a dispatcher.
///
/// Published tasks accept [`crate::PublishDispatchToken::Task`] or
/// [`crate::PublishDispatchToken::EarliestAvailable`]; singleton tasks accept `()`.
/// Both full [`crate::Backend`] implementations and execution-only backends can be used.
///
/// Claims the task before building a worker, renews its lease while processing, and awaits the
/// failure or completion recording attempt before returning a [`TaskAttemptOutcome`].
/// Missing, leased, or unavailable tasks do not start a worker. Backend errors are logged and
/// return [`TaskAttemptOutcome::Retry`]; a worker's intended schedule is returned only after
/// successful finalization.
/// Renewal failure aborts the worker without recording completion or undoing side effects.
///
/// This does not discover work or retry internally. Retries and successful rescheduling require
/// another external trigger. The host must drive/await this future to execute it; dropping it
/// does not provide a cancellation guarantee.
///
/// Requires Tokio with timers natively, or `cloudflare` and a live Workers request on wasm.
/// Does not extend request lifetime. Native worker panics record failure; wasm traps may leave a lease held.
pub async fn run_task_once<B, F>(
    backend: B,
    factory: F,
    worker_id: u64,
    dispatch_token: <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as ActivationStrategy>::DispatchToken,
) -> TaskAttemptOutcome
where
    B: TaskExecutionBackend + 'static,
    F: WorkerFactory + 'static,
{
    let (update_signal, _) = tokio::sync::mpsc::unbounded_channel();
    WorkerRuntime {
        backend,
        factory: Arc::new(factory),
        worker_id,
        update_signal,
    }
    .run_and_wait(dispatch_token)
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeUpdate {
    pub next_available_from_update: Option<Option<Instant>>,
    pub claimed_task: bool,
}

/// An internal type that handles worker lifecycle.
#[derive(Debug)]
pub(crate) struct WorkerRuntime<B, F> {
    pub backend: B,
    pub factory: Arc<F>,
    pub worker_id: u64,
    pub update_signal: MpscSender<RuntimeUpdate>,
}

impl<B, F> WorkerRuntime<B, F>
where
    B: TaskExecutionBackend + 'static,
    F: WorkerFactory + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    pub fn run(
        self,
        dispatch_token: <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as ActivationStrategy>::DispatchToken,
    ) {
        platform::spawn(self.run_and_wait(dispatch_token));
    }

    pub async fn run_and_wait(
        self,
        dispatch_token: <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as ActivationStrategy>::DispatchToken,
    ) -> TaskAttemptOutcome {
        let daemon = Daemon {
            backend: self.backend,
            factory: self.factory,
            worker_id: self.worker_id,
            update_signal: self.update_signal,
            status: DaemonStatus::WaitingForTask { dispatch_token },
        };
        daemon.run().await
    }
}

struct Daemon<B, F, T>
where
    T: TaskDefinition,
{
    backend: B,
    factory: Arc<F>,
    worker_id: u64,
    update_signal: MpscSender<RuntimeUpdate>,
    status: DaemonStatus<T>,
}

impl<B, F> Daemon<B, F, <F::Worker as Worker>::Task>
where
    B: TaskExecutionBackend,
    F: WorkerFactory,
    F::Worker: 'static,
{
    async fn run(mut self) -> TaskAttemptOutcome {
        loop {
            let (new_self, action, update) = self.event_loop().await;
            self = new_self;

            if let Some(update) = update {
                let _ = self.update_signal.send(update);
            }

            if let EventLoopResult::Exit(outcome) = action {
                return outcome;
            }
        }
    }

    async fn event_loop(mut self) -> (Self, EventLoopResult, Option<RuntimeUpdate>) {
        let (status, action, update) = match self.status {
            DaemonStatus::WaitingForTask { dispatch_token } => {
                match <<<F::Worker as Worker>::Task as TaskDefinition>::Trigger as ActivationStrategy>::claim_task::<B, <F::Worker as Worker>::Task>(
                    &self.backend,
                    self.worker_id,
                    dispatch_token,
                    Instant::now() + LEASE_DURATION,
                )
                .await {
                    Ok(task) => (
                        DaemonStatus::TaskClaimed {
                            task_id: task.task_id,
                            task_payload: task.task_payload,
                            lease_expiration: task.lease_expiration,
                        },
                        EventLoopResult::Continue,
                        Some(RuntimeUpdate {
                            next_available_from_update: None,
                            claimed_task: true,
                        }),
                    ),
                    Err(ClaimTaskError::TaskLeased { expiration }) => {
                        trace!("Unable to claim task with worker #{}", self.worker_id);
                        (
                            DaemonStatus::WorkerExited,
                            EventLoopResult::Exit(TaskAttemptOutcome::RetryAt { available_from: expiration }),
                            Some(RuntimeUpdate {
                                next_available_from_update: Some(Some(expiration)),
                                claimed_task: false,
                            }),
                        )
                    }
                    Err(ClaimTaskError::TaskUnavailable { available_from }) => {
                        trace!("No task available for worker #{}", self.worker_id);
                        (
                            DaemonStatus::WorkerExited,
                            EventLoopResult::Exit(available_from.map_or(TaskAttemptOutcome::Retry, |available_from| TaskAttemptOutcome::RetryAt { available_from })),
                            Some(RuntimeUpdate {
                                next_available_from_update: Some(available_from),
                                claimed_task: false,
                            }),
                        )
                    }
                    Err(ClaimTaskError::TaskNotFound) => {
                        let outcome = match <<F::Worker as Worker>::Task as TaskDefinition>::Trigger::KIND {
                            ActivationStrategyKind::Publish => TaskAttemptOutcome::Done,
                            ActivationStrategyKind::Singleton => TaskAttemptOutcome::Retry,
                        };
                        (DaemonStatus::WorkerExited, EventLoopResult::Exit(outcome), None)
                    }
                    Err(ClaimTaskError::Backend(err)) => {
                        warn!(
                            "Unable to claim task with worker #{} due to backend error: {}",
                            self.worker_id, err
                        );
                        (DaemonStatus::WorkerExited, EventLoopResult::Exit(TaskAttemptOutcome::Retry), None)
                    }
                }
            }
            DaemonStatus::TaskClaimed {
                task_id,
                task_payload,
                lease_expiration,
            } => {
                let worker = self.factory.build(self.worker_id);
                let worker_handle =
                    platform::spawn(async move { worker.process(task_id, task_payload).await });

                (
                    DaemonStatus::WorkerStarted {
                        task_id,
                        worker_handle,
                        lease_expiration,
                    },
                    EventLoopResult::Continue,
                    None,
                )
            }
            DaemonStatus::WorkerStarted {
                task_id,
                mut worker_handle,
                lease_expiration,
            } => {
                let renewal_deadline = lease_expiration
                    .checked_sub(LEASE_RENEWAL_THRESHOLD)
                    .unwrap_or_else(Instant::now);
                let renewal = platform::sleep_until(renewal_deadline);

                tokio::select! {
                    _ = renewal => {
                        let renewal = self
                            .backend
                            .renew(
                                self.worker_id,
                                task_id,
                                Instant::now() + LEASE_DURATION,
                            )
                            .await;

                        match renewal {
                            Ok(new_lease) => {
                                trace!("Lease renewed");
                                (
                                    DaemonStatus::WorkerStarted {
                                        task_id,
                                        worker_handle,
                                        lease_expiration: new_lease.new_expiration,
                                    },
                                    EventLoopResult::Continue,
                                    None,
                                )
                            }
                            Err(RenewTaskError::LeaseLost) => {
                                trace!(
                                    "Lease lost for task #{}, aborting worker #{}",
                                    task_id,
                                    self.worker_id
                                );
                                worker_handle.abort();
                                (DaemonStatus::WorkerExited, EventLoopResult::Exit(TaskAttemptOutcome::Retry), None)
                            }
                            Err(RenewTaskError::Backend(err)) => {
                                warn!(
                                    "Unable to renew lease for task #{} with worker #{}: {}",
                                    task_id,
                                    self.worker_id,
                                    err
                                );
                                worker_handle.abort();
                                (DaemonStatus::WorkerExited, EventLoopResult::Exit(TaskAttemptOutcome::Retry), None)
                            }
                        }
                    },
                    join_result = &mut worker_handle => {
                        match join_result {
                            Ok(Ok(task_success)) => (
                                DaemonStatus::WorkerFinishedProcessing {
                                    task_id,
                                    task_success,
                                },
                                EventLoopResult::Continue,
                                None,
                            ),
                            Ok(Err(task_failure)) => (
                                DaemonStatus::WorkerFailedProcessing {
                                    task_id,
                                    available_from: task_failure.available_from,
                                },
                                EventLoopResult::Continue,
                                None,
                            ),
                            Err(err) => {
                                warn!(
                                    "Worker #{} for task #{} exited unexpectedly: {}; retrying immediately",
                                    self.worker_id,
                                    task_id,
                                    err
                                );
                                (
                                    DaemonStatus::WorkerFailedProcessing {
                                        task_id,
                                        available_from: None,
                                    },
                                    EventLoopResult::Continue,
                                    None,
                                )
                            }
                        }
                    },
                }
            }
            DaemonStatus::WorkerFailedProcessing {
                task_id,
                available_from,
            } => {
                let outcome = match self
                    .backend
                    .fail(self.worker_id, task_id, available_from)
                    .await
                {
                    Ok(_) => {
                        trace!("Failed task #{} with worker #{}", task_id, self.worker_id);
                        TaskAttemptOutcome::RetryAt { available_from: available_from.unwrap_or_else(Instant::now) }
                    }
                    Err(FailTaskError::LeaseLost) => {
                        warn!(
                            "Worker #{} failed task #{} but no longer holds its lease; assuming another worker took over",
                            self.worker_id, task_id
                        );
                        TaskAttemptOutcome::Retry
                    }
                    Err(FailTaskError::Backend(err)) => {
                        warn!(
                            "Unable to record failure for task #{} with worker #{} due to backend error: {}",
                            task_id, self.worker_id, err
                        );
                        TaskAttemptOutcome::Retry
                    }
                };

                (
                    DaemonStatus::WorkerExited,
                    EventLoopResult::Exit(outcome),
                    Some(RuntimeUpdate {
                        next_available_from_update: None,
                        claimed_task: false,
                    }),
                )
            }
            DaemonStatus::WorkerFinishedProcessing {
                task_id,
                task_success,
            } => {
                let outcome = match self
                    .backend
                    .finish::<<F::Worker as Worker>::Task>(
                        self.worker_id,
                        task_id,
                        task_success.callback_payload,
                        task_success.available_from,
                    )
                    .await
                {
                    Ok(_) => {
                        trace!("Finished task #{} with worker #{}", task_id, self.worker_id);
                        match (task_success.available_from, <<F::Worker as Worker>::Task as TaskDefinition>::Trigger::KIND) {
                            (Some(available_from), _) => TaskAttemptOutcome::RetryAt { available_from },
                            (None, ActivationStrategyKind::Singleton) => TaskAttemptOutcome::RetryAt { available_from: Instant::now() },
                            (None, ActivationStrategyKind::Publish) => TaskAttemptOutcome::Done,
                        }
                    }
                    Err(FinishTaskError::LeaseLost) => {
                        warn!(
                            "Worker #{} finished task #{} but no longer holds its lease; assuming another worker took over",
                            self.worker_id, task_id
                        );
                        TaskAttemptOutcome::Retry
                    }
                    Err(FinishTaskError::Backend(err)) => {
                        warn!(
                            "Unable to finalize task #{} with worker #{} due to backend error: {}",
                            task_id, self.worker_id, err
                        );
                        TaskAttemptOutcome::Retry
                    }
                };

                (
                    DaemonStatus::WorkerExited,
                    EventLoopResult::Exit(outcome),
                    Some(RuntimeUpdate {
                        next_available_from_update: None,
                        claimed_task: false,
                    }),
                )
            }
            DaemonStatus::WorkerExited => {
                unreachable!("Worker runtime event loop should have ended")
            }
        };

        self.status = status;
        (self, action, update)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventLoopResult {
    Continue,
    Exit(TaskAttemptOutcome),
}

/// Worker runtime daemon lifecycle stages.
enum DaemonStatus<T>
where
    T: TaskDefinition,
{
    /// The runtime has just been started and have not claimed the task yet.
    WaitingForTask {
        dispatch_token: <T::Trigger as ActivationStrategy>::DispatchToken,
    },
    /// The runtime has claimed the task.
    TaskClaimed {
        task_id: u64,
        task_payload: <T::Trigger as ActivationStrategy>::EffectivePayload,
        lease_expiration: Instant,
    },
    /// The background worker has been spawned.
    WorkerStarted {
        task_id: u64,
        worker_handle: platform::JoinHandle<crate::TaskResult<T::Callback>>,
        lease_expiration: Instant,
    },
    /// The worker finished unsuccessfully and the task should be released back to the backend.
    WorkerFailedProcessing {
        task_id: u64,
        available_from: Option<Instant>,
    },
    /// The worker future completed successfully and the task should be finalized in the backend.
    WorkerFinishedProcessing {
        task_id: u64,
        task_success: TaskSuccess<T::Callback>,
    },
    /// The worker runtime is done.
    WorkerExited,
}
