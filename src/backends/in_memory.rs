//! In-memory task backend implementation.

use std::collections::HashMap;
use std::collections::hash_map::Entry as HashMapEntry;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::time::Instant;

use tokio::sync::broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender};
use tokio::sync::mpsc::{UnboundedReceiver as MpscReceiver, UnboundedSender as MpscSender};
use tokio::sync::oneshot::Sender as OneshotSender;

use crate::backends::{
    BackendSignal, BackendSignalSubscription, ClaimTaskError, ClaimedTask, FinishTaskError,
    FinishedTask, NewTaskAvailableSignalPayload, RenewTaskError, RenewedTaskLease, SubscribeError,
    SweepTasksError, SweptTask,
};
use crate::{
    Backend, TaskDefinition,
    backends::{PublishTaskError, PublishedTask},
};

const SIGNAL_CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
pub enum InMemoryBackendError {
    DaemonUnavailable,
    ResponseDropped,
    PayloadSerialization(serde_json::Error),
    PayloadDeserialization(serde_json::Error),
}

impl Display for InMemoryBackendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DaemonUnavailable => f.write_str("in-memory backend daemon is unavailable"),
            Self::ResponseDropped => {
                f.write_str("in-memory backend daemon dropped the response channel")
            }
            Self::PayloadSerialization(error) => {
                write!(f, "task payload serialization failed: {error}")
            }
            Self::PayloadDeserialization(error) => {
                write!(f, "task payload deserialization failed: {error}")
            }
        }
    }
}

impl StdError for InMemoryBackendError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::DaemonUnavailable | Self::ResponseDropped => None,
            Self::PayloadSerialization(error) => Some(error),
            Self::PayloadDeserialization(error) => Some(error),
        }
    }
}

/// In-memory task backend implementation.
///
/// This type can be cheaply cloned. All cloned instances share a single task registry.
///
/// The in-memory backend uses JSON to hold task payloads. While the (de)serilization is technically
/// unnecessary as the data is never persisted, since the in-memory backend is typically used for
/// testing, going through the process helps uncover potential serialization bugs.
#[derive(Clone)]
pub struct InMemoryBackend {
    command_sink: MpscSender<DaemonCommand>,
}

impl InMemoryBackend {
    /// Creates a new [`InMemoryBackend`] that can be cheaply cloned.
    ///
    /// When calling `new()`, a shared in-memory service is spawned in the background. All cloned
    /// [`InMemoryBackend`] instances point to the same underlying backend.
    pub fn new() -> Self {
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<DaemonCommand>();

        let (sub_tx, _) = tokio::sync::broadcast::channel(SIGNAL_CHANNEL_SIZE);

        let daemon = Daemon {
            command_pipe: command_rx,
            next_task_id: 0,
            signal: sub_tx,
            tasks: Default::default(),
        };

        tokio::spawn(daemon.run());

        Self {
            command_sink: command_tx,
        }
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Backend for InMemoryBackend {
    async fn subscribe(&self) -> Result<BackendSignalSubscription, SubscribeError> {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::Subscribe(SubscribeArgs {
                callback: callback_tx,
            }))
            .map_err(|_| {
                SubscribeError::Backend(Box::new(InMemoryBackendError::DaemonUnavailable))
            })?;

        let result = callback_rx.await.map_err(|_| {
            SubscribeError::Backend(Box::new(InMemoryBackendError::ResponseDropped))
        })?;

        Ok(BackendSignalSubscription::new(result.sub_rx))
    }

    async fn sweep(&self) -> Result<Vec<SweptTask>, SweepTasksError> {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::SweepTasks(SweepTasksArgs {
                callback: callback_tx,
            }))
            .map_err(|_| {
                SweepTasksError::Backend(Box::new(InMemoryBackendError::DaemonUnavailable))
            })?;

        let result = callback_rx.await.map_err(|_| {
            SweepTasksError::Backend(Box::new(InMemoryBackendError::ResponseDropped))
        })?;

        Ok(result.tasks)
    }

    async fn publish<T>(&self, payload: T::Payload) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
    {
        // In-memory backend is mostly used for testing. So we use a human-readable payload
        // serialization format here.
        let payload_json = serde_json::to_string(&payload).map_err(|err| {
            PublishTaskError::Backend(Box::new(InMemoryBackendError::PayloadSerialization(err)))
        })?;

        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::PublishTask(PublishTaskArgs {
                payload_json,
                callback: callback_tx,
            }))
            .map_err(|_| {
                PublishTaskError::Backend(Box::new(InMemoryBackendError::DaemonUnavailable))
            })?;

        let result = callback_rx.await.map_err(|_| {
            PublishTaskError::Backend(Box::new(InMemoryBackendError::ResponseDropped))
        })?;

        Ok(PublishedTask {
            task_id: result.task_id,
        })
    }

    async fn claim<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<T::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::ClaimTask(ClaimTaskArgs {
                worker_id,
                task_id,
                lease_expiration,
                callback: callback_tx,
            }))
            .map_err(|_| {
                ClaimTaskError::Backend(Box::new(InMemoryBackendError::DaemonUnavailable))
            })?;

        let result = callback_rx.await.map_err(|_| {
            ClaimTaskError::Backend(Box::new(InMemoryBackendError::ResponseDropped))
        })?;

        match result {
            ClaimTaskReturn::Claimed {
                payload_json,
                expiration,
            } => {
                let payload = serde_json::from_str(&payload_json).map_err(|err| {
                    ClaimTaskError::Backend(Box::new(InMemoryBackendError::PayloadDeserialization(
                        err,
                    )))
                })?;

                Ok(ClaimedTask {
                    task_id,
                    task_payload: payload,
                    lease_expiration: expiration,
                })
            }
            ClaimTaskReturn::TaskUnavailable { expiration } => {
                Err(ClaimTaskError::TaskLeased { expiration })
            }
            ClaimTaskReturn::TaskNotFound => Err(ClaimTaskError::TaskNotFound),
        }
    }

    async fn renew(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<RenewedTaskLease, RenewTaskError> {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::RenewTask(RenewTaskArgs {
                worker_id,
                task_id,
                lease_expiration,
                callback: callback_tx,
            }))
            .map_err(|_| {
                RenewTaskError::Backend(Box::new(InMemoryBackendError::DaemonUnavailable))
            })?;

        let result = callback_rx.await.map_err(|_| {
            RenewTaskError::Backend(Box::new(InMemoryBackendError::ResponseDropped))
        })?;

        match result {
            RenewTaskReturn::Renewed { expiration } => Ok(RenewedTaskLease {
                new_expiration: expiration,
            }),
            RenewTaskReturn::LeaseLost => Err(RenewTaskError::LeaseLost),
        }
    }

    async fn finish(&self, worker_id: u64, task_id: u64) -> Result<FinishedTask, FinishTaskError> {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::FinishTask(FinishTaskArgs {
                worker_id,
                task_id,
                callback: callback_tx,
            }))
            .map_err(|_| {
                FinishTaskError::Backend(Box::new(InMemoryBackendError::DaemonUnavailable))
            })?;

        let result = callback_rx.await.map_err(|_| {
            FinishTaskError::Backend(Box::new(InMemoryBackendError::ResponseDropped))
        })?;

        match result {
            FinishTaskReturn::Finished => Ok(FinishedTask { task_id }),
            FinishTaskReturn::LeaseLost => Err(FinishTaskError::LeaseLost),
        }
    }
}

#[derive(Debug)]
struct Daemon {
    command_pipe: MpscReceiver<DaemonCommand>,
    next_task_id: u64,
    signal: BroadcastSender<BackendSignal>,
    tasks: HashMap<u64, TaskEntry>,
}

impl Daemon {
    async fn run(mut self) {
        while let Some(command) = self.command_pipe.recv().await {
            match command {
                DaemonCommand::Subscribe(args) => self.handle_subscribe(args),
                DaemonCommand::SweepTasks(args) => self.handle_sweep_tasks(args),
                DaemonCommand::PublishTask(args) => self.handle_publish_task(args),
                DaemonCommand::ClaimTask(args) => self.handle_claim_task(args),
                DaemonCommand::RenewTask(args) => self.handle_renew_task(args),
                DaemonCommand::FinishTask(args) => self.handle_finish_task(args),
            }
        }
    }

    fn handle_subscribe(&mut self, args: SubscribeArgs) {
        let rx = self.signal.subscribe();

        // It doesn't matter if the caller is gone
        let _ = args.callback.send(SubscribeReturn { sub_rx: rx });
    }

    fn handle_sweep_tasks(&mut self, args: SweepTasksArgs) {
        let now = Instant::now();
        let tasks = self
            .tasks
            .iter()
            .filter_map(|(&task_id, task)| match task.claim.as_ref() {
                Some(claim) if claim.lease_expiration > now => None,
                _ => Some(SweptTask { task_id }),
            })
            .collect();

        // It doesn't matter if the caller is gone
        let _ = args.callback.send(SweepTasksReturn { tasks });
    }

    fn handle_publish_task(&mut self, args: PublishTaskArgs) {
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        self.tasks.insert(
            task_id,
            TaskEntry {
                payload: args.payload_json,
                claim: None,
            },
        );

        let _ = self.signal.send(BackendSignal::NewTaskAvailable(
            NewTaskAvailableSignalPayload { task_id },
        ));

        // It doesn't matter if the caller is gone
        let _ = args.callback.send(PublishTaskReturn { task_id });
    }

    fn handle_claim_task(&mut self, args: ClaimTaskArgs) {
        let res = match self.tasks.entry(args.task_id) {
            HashMapEntry::Occupied(mut entry) => {
                let task = entry.get_mut();
                let claim = &mut task.claim;

                if let Some(claim) = claim {
                    if claim.lease_expiration <= Instant::now() {
                        *claim = ClaimEntry {
                            worker_id: args.worker_id,
                            lease_expiration: args.lease_expiration,
                        };
                        ClaimTaskReturn::Claimed {
                            payload_json: task.payload.clone(),
                            expiration: args.lease_expiration,
                        }
                    } else {
                        ClaimTaskReturn::TaskUnavailable {
                            expiration: claim.lease_expiration,
                        }
                    }
                } else {
                    *claim = Some(ClaimEntry {
                        worker_id: args.worker_id,
                        lease_expiration: args.lease_expiration,
                    });
                    ClaimTaskReturn::Claimed {
                        payload_json: task.payload.clone(),
                        expiration: args.lease_expiration,
                    }
                }
            }
            HashMapEntry::Vacant(_) => ClaimTaskReturn::TaskNotFound,
        };

        // It doesn't matter if the caller is gone
        let _ = args.callback.send(res);
    }

    fn handle_renew_task(&mut self, args: RenewTaskArgs) {
        let res = match self.tasks.entry(args.task_id) {
            HashMapEntry::Occupied(mut entry) => {
                let task = entry.get_mut();
                let claim = &mut task.claim;

                if let Some(claim) = claim {
                    if claim.worker_id == args.worker_id {
                        claim.lease_expiration = args.lease_expiration;
                        RenewTaskReturn::Renewed {
                            expiration: args.lease_expiration,
                        }
                    } else {
                        // Task already overtaken by another worker.
                        RenewTaskReturn::LeaseLost
                    }
                } else {
                    // Task already finished by another worker.
                    RenewTaskReturn::LeaseLost
                }
            }
            // Task already finished by another worker.
            HashMapEntry::Vacant(_) => RenewTaskReturn::LeaseLost,
        };

        // It doesn't matter if the caller is gone
        let _ = args.callback.send(res);
    }

    fn handle_finish_task(&mut self, args: FinishTaskArgs) {
        let res = match self.tasks.entry(args.task_id) {
            HashMapEntry::Occupied(entry) => {
                let claim_owned_by_worker = entry
                    .get()
                    .claim
                    .as_ref()
                    .is_some_and(|claim| claim.worker_id == args.worker_id);

                if claim_owned_by_worker {
                    entry.remove();
                    FinishTaskReturn::Finished
                } else {
                    FinishTaskReturn::LeaseLost
                }
            }
            HashMapEntry::Vacant(_) => FinishTaskReturn::LeaseLost,
        };

        // It doesn't matter if the caller is gone
        let _ = args.callback.send(res);
    }
}

#[derive(Debug)]
struct TaskEntry {
    payload: String,
    claim: Option<ClaimEntry>,
}

#[derive(Debug)]
struct ClaimEntry {
    worker_id: u64,
    lease_expiration: Instant,
}

#[derive(Debug)]
enum DaemonCommand {
    Subscribe(SubscribeArgs),
    SweepTasks(SweepTasksArgs),
    PublishTask(PublishTaskArgs),
    ClaimTask(ClaimTaskArgs),
    RenewTask(RenewTaskArgs),
    FinishTask(FinishTaskArgs),
}

#[derive(Debug)]
struct SubscribeArgs {
    callback: OneshotSender<SubscribeReturn>,
}

#[derive(Debug)]
struct SubscribeReturn {
    sub_rx: BroadcastReceiver<BackendSignal>,
}

#[derive(Debug)]
struct SweepTasksArgs {
    callback: OneshotSender<SweepTasksReturn>,
}

#[derive(Debug)]
struct SweepTasksReturn {
    tasks: Vec<SweptTask>,
}

#[derive(Debug)]
struct PublishTaskArgs {
    payload_json: String,
    callback: OneshotSender<PublishTaskReturn>,
}

#[derive(Debug)]
struct PublishTaskReturn {
    task_id: u64,
}

#[derive(Debug)]
struct ClaimTaskArgs {
    worker_id: u64,
    task_id: u64,
    lease_expiration: Instant,
    callback: OneshotSender<ClaimTaskReturn>,
}

#[derive(Debug)]
enum ClaimTaskReturn {
    Claimed {
        payload_json: String,
        expiration: Instant,
    },
    TaskUnavailable {
        expiration: Instant,
    },
    TaskNotFound,
}

#[derive(Debug)]
struct RenewTaskArgs {
    worker_id: u64,
    task_id: u64,
    lease_expiration: Instant,
    callback: OneshotSender<RenewTaskReturn>,
}

#[derive(Debug)]
enum RenewTaskReturn {
    Renewed { expiration: Instant },
    LeaseLost,
}

#[derive(Debug)]
struct FinishTaskArgs {
    worker_id: u64,
    task_id: u64,
    callback: OneshotSender<FinishTaskReturn>,
}

#[derive(Debug)]
enum FinishTaskReturn {
    Finished,
    LeaseLost,
}
