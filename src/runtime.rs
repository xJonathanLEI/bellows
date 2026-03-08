use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    Backend, TaskDefinition, Worker, WorkerFactory,
    backends::{ClaimTaskError, FinishTaskError, RenewTaskError},
};

use tokio::sync::mpsc::UnboundedSender as MpscSender;
use tracing::{trace, warn};

const LEASE_DURATION: Duration = Duration::from_secs(20);
const LEASE_RENEWAL_THRESHOLD: Duration = Duration::from_secs(10);

/// An internal type that handles worker lifecycle.
#[derive(Debug)]
pub(crate) struct WorkerRuntime<B, F> {
    pub backend: B,
    pub factory: Arc<F>,
    pub worker_id: u64,
    pub finished_signal: MpscSender<()>,
}

impl<B, F> WorkerRuntime<B, F>
where
    B: Backend + 'static,
    F: WorkerFactory + 'static,
{
    pub fn run(self, task_id: u64) {
        let daemon = Daemon {
            backend: self.backend,
            factory: self.factory,
            worker_id: self.worker_id,
            finished_signal: self.finished_signal,
            task_id,
            status: DaemonStatus::WaitingForTask,
        };
        tokio::spawn(daemon.run());
    }
}

#[derive(Debug)]
struct Daemon<B, F, T> {
    backend: B,
    factory: Arc<F>,
    worker_id: u64,
    finished_signal: MpscSender<()>,
    task_id: u64,
    status: DaemonStatus<T>,
}

impl<B, F> Daemon<B, F, <<F::Worker as Worker>::Task as TaskDefinition>::Payload>
where
    B: Backend,
    F: WorkerFactory,
    F::Worker: 'static,
    <<F::Worker as Worker>::Task as TaskDefinition>::Payload: 'static,
{
    async fn run(mut self) {
        loop {
            let (new_self, action) = self.event_loop().await;
            self = new_self;

            if matches!(action, EventLoopResult::Exit) {
                break;
            }
        }

        let _ = self.finished_signal.send(());
    }

    async fn event_loop(mut self) -> (Self, EventLoopResult) {
        let (status, action) = match self.status {
            DaemonStatus::WaitingForTask => {
                match self
                    .backend
                    .claim::<<F::Worker as Worker>::Task>(
                        self.worker_id,
                        self.task_id,
                        Instant::now() + LEASE_DURATION,
                    )
                    .await
                {
                    Ok(task) => (
                        DaemonStatus::TaskClaimed {
                            task_payload: task.task_payload,
                            lease_expiration: task.lease_expiration,
                        },
                        EventLoopResult::Continue,
                    ),
                    Err(ClaimTaskError::TaskLeased { .. } | ClaimTaskError::TaskNotFound) => {
                        // This is fine as it's likely that another worker claimed the task.
                        trace!("Unable to claim task #{}", self.task_id);
                        (DaemonStatus::WaitingForTask, EventLoopResult::Exit)
                    }
                    Err(ClaimTaskError::Backend(err)) => {
                        warn!(
                            "Unable to claim task #{} due to backend error: {}",
                            self.task_id, err
                        );
                        (DaemonStatus::WaitingForTask, EventLoopResult::Exit)
                    }
                }
            }
            DaemonStatus::TaskClaimed {
                task_payload,
                lease_expiration,
            } => {
                let worker = self.factory.build(self.worker_id);
                let task_id = self.task_id;
                let worker_handle = tokio::spawn(async move {
                    worker.process(task_id, task_payload).await;
                });

                (
                    DaemonStatus::WorkerStarted {
                        worker_handle,
                        lease_expiration,
                    },
                    EventLoopResult::Continue,
                )
            }
            DaemonStatus::WorkerStarted {
                mut worker_handle,
                lease_expiration,
            } => {
                let renewal_deadline = lease_expiration
                    .checked_sub(LEASE_RENEWAL_THRESHOLD)
                    .unwrap_or_else(Instant::now);
                let renewal =
                    tokio::time::sleep_until(tokio::time::Instant::from_std(renewal_deadline));

                tokio::select! {
                    _ = renewal => {
                        let renewal = self
                            .backend
                            .renew(
                                self.worker_id,
                                self.task_id,
                                Instant::now() + LEASE_DURATION,
                            )
                            .await;

                        match renewal {
                            Ok(new_lease) => {
                                trace!("Lease renewed");
                                (
                                    DaemonStatus::WorkerStarted {
                                        worker_handle,
                                        lease_expiration: new_lease.new_expiration,
                                    },
                                    EventLoopResult::Continue,
                                )
                            }
                            Err(RenewTaskError::LeaseLost) => {
                                trace!(
                                    "Lease lost for task #{}, aborting worker #{}",
                                    self.task_id,
                                    self.worker_id
                                );
                                worker_handle.abort();
                                (DaemonStatus::WorkerExited, EventLoopResult::Exit)
                            }
                            Err(RenewTaskError::Backend(err)) => {
                                warn!(
                                    "Unable to renew lease for task #{} with worker #{}: {}",
                                    self.task_id,
                                    self.worker_id,
                                    err
                                );
                                worker_handle.abort();
                                (DaemonStatus::WorkerExited, EventLoopResult::Exit)
                            }
                        }
                    },
                    join_result = &mut worker_handle => {
                        match join_result {
                            Ok(()) => (
                                DaemonStatus::WorkerFinishedProcessing,
                                EventLoopResult::Continue,
                            ),
                            Err(err) => {
                                // TODO: Handle post-failure state. Potential options:
                                // - give up lease to allow immediate take over
                                // - set up retry-after mechanism for retrying with backoff
                                warn!(
                                    "Worker #{} for task #{} exited unexpectedly: {}",
                                    self.worker_id,
                                    self.task_id,
                                    err
                                );
                                (DaemonStatus::WorkerExited, EventLoopResult::Exit)
                            }
                        }
                    },
                }
            }
            DaemonStatus::WorkerFinishedProcessing => {
                match self.backend.finish(self.worker_id, self.task_id).await {
                    Ok(_) => {
                        trace!(
                            "Finished task #{} with worker #{}",
                            self.task_id, self.worker_id
                        );
                    }
                    Err(FinishTaskError::LeaseLost) => {
                        warn!(
                            "Worker #{} finished task #{} but no longer holds its lease; assuming another worker took over",
                            self.worker_id, self.task_id
                        );
                    }
                    Err(FinishTaskError::Backend(err)) => {
                        warn!(
                            "Unable to finalize task #{} with worker #{} due to backend error: {}",
                            self.task_id, self.worker_id, err
                        );
                    }
                }

                (DaemonStatus::WorkerExited, EventLoopResult::Exit)
            }
            DaemonStatus::WorkerExited => {
                unreachable!("Worker runtime event loop should have ended")
            }
        };

        self.status = status;
        (self, action)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventLoopResult {
    Continue,
    Exit,
}

/// Worker runtime daemon lifecycle stages.
#[derive(Debug)]
enum DaemonStatus<T> {
    /// The runtime has just been started and have not claimed the task yet.
    WaitingForTask,
    /// The runtime has claimed the task.
    TaskClaimed {
        task_payload: T,
        lease_expiration: Instant,
    },
    /// The background worker has been spawned.
    WorkerStarted {
        worker_handle: tokio::task::JoinHandle<()>,
        lease_expiration: Instant,
    },
    /// The worker future completed successfully and the task should be finalized in the backend.
    WorkerFinishedProcessing,
    /// The worker runtime is done.
    WorkerExited,
}
