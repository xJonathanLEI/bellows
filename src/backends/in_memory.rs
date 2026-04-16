//! In-memory task backend implementation.

use std::{
    collections::{HashMap, hash_map::Entry as HashMapEntry},
    error::Error as StdError,
    fmt::{Display, Formatter},
    time::Instant,
};

use rand::RngExt;
use tokio::sync::{
    broadcast::{Receiver as BroadcastReceiver, Sender as BroadcastSender},
    mpsc::{UnboundedReceiver as MpscReceiver, UnboundedSender as MpscSender},
    oneshot::Sender as OneshotSender,
};

use crate::PublishActivationStrategy;
use crate::backends::{
    Backend, BackendSignal, BackendSignalSubscription, ClaimTaskError, ClaimedTask, FailTaskError,
    FailedTask, FinishTaskError, FinishedTask, NewTaskAvailableSignalPayload, PublishTaskError,
    PublishedTask, RenewTaskError, RenewedTaskLease, SubscribeError,
};
use crate::{AwaitableTask, TaskDefinition};

const SIGNAL_CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
pub enum InMemoryBackendError {
    DaemonUnavailable,
    ResponseDropped,
    PayloadSerialization(serde_json::Error),
    PayloadDeserialization(serde_json::Error),
    CallbackSerialization(serde_json::Error),
}

impl Display for InMemoryBackendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DaemonUnavailable => f.write_str("in-memory backend daemon is unavailable"),
            Self::ResponseDropped => f.write_str("in-memory backend response channel dropped"),
            Self::PayloadSerialization(error) => {
                write!(f, "task payload serialization failed: {error}")
            }
            Self::PayloadDeserialization(error) => {
                write!(f, "task payload deserialization failed: {error}")
            }
            Self::CallbackSerialization(error) => {
                write!(f, "task callback serialization failed: {error}")
            }
        }
    }
}

impl StdError for InMemoryBackendError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::PayloadSerialization(error)
            | Self::PayloadDeserialization(error)
            | Self::CallbackSerialization(error) => Some(error),
            Self::DaemonUnavailable | Self::ResponseDropped => None,
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

        let daemon = Daemon {
            command_pipe: command_rx,
            next_task_id: 0,
            signals: Default::default(),
            callbacks: Default::default(),
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
    async fn subscribe<T>(&self) -> Result<BackendSignalSubscription<T>, SubscribeError>
    where
        T: TaskDefinition,
    {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::Subscribe(SubscribeArgs {
                task_name: T::NAME,
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

    async fn publish<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.publish_impl::<T>(payload, None).await
    }

    async fn publish_awaitable<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<AwaitableTask<T::Callback>, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
        let published = self
            .publish_impl::<T>(
                payload,
                Some(Box::new(TypedCallbackSink { tx: callback_tx })),
            )
            .await?;

        Ok(AwaitableTask::new(published.task_id, callback_rx))
    }

    async fn claim_published<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<
        ClaimedTask<<<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload>,
        ClaimTaskError,
    >
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::ClaimPublishedTask(ClaimPublishedTaskArgs {
                task_name: T::NAME,
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
                task_id,
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
            ClaimTaskReturn::TaskLeased { expiration } => {
                Err(ClaimTaskError::TaskLeased { expiration })
            }
            ClaimTaskReturn::TaskUnavailable { available_from } => {
                Err(ClaimTaskError::TaskUnavailable { available_from })
            }
            ClaimTaskReturn::TaskNotFound => Err(ClaimTaskError::TaskNotFound),
        }
    }

    async fn claim_earliest_published<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<
        ClaimedTask<<<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload>,
        ClaimTaskError,
    >
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::ClaimEarliestPublishedTask(
                ClaimEarliestPublishedTaskArgs {
                    task_name: T::NAME,
                    worker_id,
                    lease_expiration,
                    callback: callback_tx,
                },
            ))
            .map_err(|_| {
                ClaimTaskError::Backend(Box::new(InMemoryBackendError::DaemonUnavailable))
            })?;

        let result = callback_rx.await.map_err(|_| {
            ClaimTaskError::Backend(Box::new(InMemoryBackendError::ResponseDropped))
        })?;

        match result {
            ClaimTaskReturn::Claimed {
                task_id,
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
            ClaimTaskReturn::TaskLeased { expiration } => {
                Err(ClaimTaskError::TaskLeased { expiration })
            }
            ClaimTaskReturn::TaskUnavailable { available_from } => {
                Err(ClaimTaskError::TaskUnavailable { available_from })
            }
            ClaimTaskReturn::TaskNotFound => Err(ClaimTaskError::TaskNotFound),
        }
    }

    async fn claim_singleton<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<()>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::ClaimSingletonTask(ClaimSingletonTaskArgs {
                task_name: T::NAME,
                worker_id,
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
            ClaimTaskReturn::Claimed { expiration, .. } => Ok(ClaimedTask {
                task_id: result
                    .task_id()
                    .expect("claimed singleton task should include task ID"),
                task_payload: (),
                lease_expiration: expiration,
            }),
            ClaimTaskReturn::TaskLeased { expiration } => {
                Err(ClaimTaskError::TaskLeased { expiration })
            }
            ClaimTaskReturn::TaskUnavailable { available_from } => {
                Err(ClaimTaskError::TaskUnavailable { available_from })
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

    async fn fail(
        &self,
        worker_id: u64,
        task_id: u64,
        available_from: Option<Instant>,
    ) -> Result<FailedTask, FailTaskError> {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::FailTask(FailTaskArgs {
                worker_id,
                task_id,
                available_from,
                callback: callback_tx,
            }))
            .map_err(|_| {
                FailTaskError::Backend(Box::new(InMemoryBackendError::DaemonUnavailable))
            })?;

        let result = callback_rx
            .await
            .map_err(|_| FailTaskError::Backend(Box::new(InMemoryBackendError::ResponseDropped)))?;

        match result {
            FailTaskReturn::Failed => Ok(FailedTask { task_id }),
            FailTaskReturn::LeaseLost => Err(FailTaskError::LeaseLost),
        }
    }

    async fn finish<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        callback_payload: T::Callback,
    ) -> Result<FinishedTask, FinishTaskError>
    where
        T: TaskDefinition,
    {
        let callback_payload_json = serde_json::to_string(&callback_payload).map_err(|err| {
            FinishTaskError::Backend(Box::new(InMemoryBackendError::CallbackSerialization(err)))
        })?;
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::FinishTask(FinishTaskArgs {
                worker_id,
                task_id,
                callback_payload_json,
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

impl InMemoryBackend {
    async fn publish_impl<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        callback_sink: Option<Box<dyn CallbackSink>>,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let payload_json = serde_json::to_string(&payload).map_err(|err| {
            PublishTaskError::Backend(Box::new(InMemoryBackendError::PayloadSerialization(err)))
        })?;

        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();

        self.command_sink
            .send(DaemonCommand::PublishTask(PublishTaskArgs {
                task_name: T::NAME,
                payload_json,
                callback_sink,
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
}

struct TypedCallbackSink<T> {
    tx: OneshotSender<T>,
}

trait CallbackSink: Send {
    fn send(self: Box<Self>, callback_payload_json: String);
}

impl<T> CallbackSink for TypedCallbackSink<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    fn send(self: Box<Self>, callback_payload_json: String) {
        if let Ok(callback_payload) = serde_json::from_str(&callback_payload_json) {
            let _ = self.tx.send(callback_payload);
        }
    }
}

struct Daemon {
    command_pipe: MpscReceiver<DaemonCommand>,
    next_task_id: u64,
    signals: HashMap<&'static str, BroadcastSender<BackendSignal>>,
    callbacks: HashMap<u64, Box<dyn CallbackSink>>,
    tasks: HashMap<u64, TaskEntry>,
}

impl Daemon {
    async fn run(mut self) {
        while let Some(command) = self.command_pipe.recv().await {
            match command {
                DaemonCommand::Subscribe(args) => self.handle_subscribe(args),
                DaemonCommand::PublishTask(args) => self.handle_publish_task(args),
                DaemonCommand::ClaimPublishedTask(args) => self.handle_claim_published_task(args),
                DaemonCommand::ClaimEarliestPublishedTask(args) => {
                    self.handle_claim_earliest_published_task(args)
                }
                DaemonCommand::ClaimSingletonTask(args) => self.handle_claim_singleton_task(args),
                DaemonCommand::RenewTask(args) => self.handle_renew_task(args),
                DaemonCommand::FailTask(args) => self.handle_fail_task(args),
                DaemonCommand::FinishTask(args) => self.handle_finish_task(args),
            }
        }
    }

    fn handle_subscribe(&mut self, args: SubscribeArgs) {
        let rx = self
            .signals
            .entry(args.task_name)
            .or_insert_with(|| tokio::sync::broadcast::channel(SIGNAL_CHANNEL_SIZE).0)
            .subscribe();

        let _ = args.callback.send(SubscribeReturn { sub_rx: rx });
    }

    fn handle_publish_task(&mut self, args: PublishTaskArgs) {
        let task_id = self.next_task_id;
        self.next_task_id += 1;

        let callback_id = args.callback_sink.map(|callback_sink| {
            let mut rng = rand::rng();
            loop {
                let callback_id = rng.random::<u64>();
                if let HashMapEntry::Vacant(entry) = self.callbacks.entry(callback_id) {
                    entry.insert(callback_sink);
                    break callback_id;
                }
            }
        });

        self.tasks.insert(
            task_id,
            TaskEntry {
                task_name: args.task_name,
                payload_json: args.payload_json,
                callback_id,
                worker_id: None,
                available_from: None,
                kind: TaskKind::Published,
            },
        );

        self.emit_signal(
            args.task_name,
            BackendSignal::NewTaskAvailable(NewTaskAvailableSignalPayload {
                task_id: Some(task_id),
                available_from: Instant::now(),
            }),
        );

        let _ = args.callback.send(PublishTaskReturn { task_id });
    }

    fn handle_claim_published_task(&mut self, args: ClaimPublishedTaskArgs) {
        let res = self.claim_task(
            args.task_name,
            Some(args.task_id),
            TaskKind::Published,
            args.worker_id,
            args.lease_expiration,
        );

        let _ = args.callback.send(res);
    }

    fn handle_claim_earliest_published_task(&mut self, args: ClaimEarliestPublishedTaskArgs) {
        let res = self.claim_task(
            args.task_name,
            None,
            TaskKind::Published,
            args.worker_id,
            args.lease_expiration,
        );

        let _ = args.callback.send(res);
    }

    fn handle_claim_singleton_task(&mut self, args: ClaimSingletonTaskArgs) {
        let task_id = self.tasks.iter().find_map(|(&task_id, task)| {
            (task.task_name == args.task_name && matches!(task.kind, TaskKind::Singleton))
                .then_some(task_id)
        });

        let task_id = task_id.unwrap_or_else(|| {
            let task_id = self.next_task_id;
            self.next_task_id += 1;
            self.tasks.insert(
                task_id,
                TaskEntry {
                    task_name: args.task_name,
                    payload_json: "null".to_owned(),
                    callback_id: None,
                    worker_id: None,
                    available_from: None,
                    kind: TaskKind::Singleton,
                },
            );
            task_id
        });

        let res = self.claim_task(
            args.task_name,
            Some(task_id),
            TaskKind::Singleton,
            args.worker_id,
            args.lease_expiration,
        );

        let _ = args.callback.send(res);
    }

    fn claim_task(
        &mut self,
        task_name: &'static str,
        task_id: Option<u64>,
        kind: TaskKind,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> ClaimTaskReturn {
        let now = Instant::now();
        let selected_task_id = match task_id {
            Some(task_id) => task_id,
            None => match self.find_earliest_claimable_task(task_name, kind, now) {
                Some(task_id) => task_id,
                None => {
                    return ClaimTaskReturn::TaskUnavailable {
                        available_from: self.find_earliest_future_task(task_name, kind, now),
                    };
                }
            },
        };

        let mut signal = None;

        let res = match self.tasks.entry(selected_task_id) {
            HashMapEntry::Occupied(mut entry) => {
                let task = entry.get_mut();

                if task.task_name != task_name || task.kind != kind {
                    ClaimTaskReturn::TaskNotFound
                } else if task.worker_id.is_some() && task.available_from.is_some_and(|at| at > now)
                {
                    ClaimTaskReturn::TaskLeased {
                        expiration: task
                            .available_from
                            .expect("leased task should have expiration"),
                    }
                } else if task.available_from.is_some_and(|at| at > now) {
                    ClaimTaskReturn::TaskUnavailable {
                        available_from: task.available_from,
                    }
                } else {
                    task.worker_id = Some(worker_id);
                    task.available_from = Some(lease_expiration);

                    let payload_json = task.payload_json.clone();
                    signal = Some((task.task_name, task.signal_payload(selected_task_id)));

                    ClaimTaskReturn::Claimed {
                        task_id: selected_task_id,
                        payload_json,
                        expiration: lease_expiration,
                    }
                }
            }
            HashMapEntry::Vacant(_) => ClaimTaskReturn::TaskNotFound,
        };

        if let Some((task_name, signal)) = signal {
            self.emit_signal(task_name, BackendSignal::NewTaskAvailable(signal));
        }

        res
    }

    fn handle_renew_task(&mut self, args: RenewTaskArgs) {
        let mut signal = None;

        let res = match self.tasks.entry(args.task_id) {
            HashMapEntry::Occupied(mut entry) => {
                let task = entry.get_mut();

                if task
                    .worker_id
                    .is_some_and(|claim_worker_id| claim_worker_id == args.worker_id)
                {
                    task.available_from = Some(args.lease_expiration);
                    signal = Some((task.task_name, task.signal_payload(args.task_id)));
                    RenewTaskReturn::Renewed {
                        expiration: args.lease_expiration,
                    }
                } else {
                    RenewTaskReturn::LeaseLost
                }
            }
            HashMapEntry::Vacant(_) => RenewTaskReturn::LeaseLost,
        };

        if let Some((task_name, signal)) = signal {
            self.emit_signal(task_name, BackendSignal::NewTaskAvailable(signal));
        }

        let _ = args.callback.send(res);
    }

    fn handle_fail_task(&mut self, args: FailTaskArgs) {
        let mut signal = None;

        let res = match self.tasks.entry(args.task_id) {
            HashMapEntry::Occupied(mut entry) => {
                let task = entry.get_mut();

                if task
                    .worker_id
                    .is_some_and(|claim_worker_id| claim_worker_id == args.worker_id)
                {
                    task.worker_id = None;
                    task.available_from = args.available_from;
                    signal = Some((task.task_name, task.signal_payload(args.task_id)));
                    FailTaskReturn::Failed
                } else {
                    FailTaskReturn::LeaseLost
                }
            }
            HashMapEntry::Vacant(_) => FailTaskReturn::LeaseLost,
        };

        if let Some((task_name, signal)) = signal {
            self.emit_signal(task_name, BackendSignal::NewTaskAvailable(signal));
        }

        let _ = args.callback.send(res);
    }

    fn handle_finish_task(&mut self, args: FinishTaskArgs) {
        let mut signal = None;
        let mut callback_delivery = None;

        let res = match self.tasks.entry(args.task_id) {
            HashMapEntry::Occupied(mut entry) => {
                let claim_owned_by_worker = entry
                    .get()
                    .worker_id
                    .is_some_and(|worker_id| worker_id == args.worker_id);

                if claim_owned_by_worker {
                    let task = entry.get();
                    if let Some(callback_id) = task.callback_id {
                        callback_delivery = Some((callback_id, args.callback_payload_json));
                    }

                    if matches!(entry.get().kind, TaskKind::Singleton) {
                        let task = entry.get_mut();
                        task.worker_id = None;
                        task.available_from = None;
                        signal = Some((task.task_name, task.signal_payload(args.task_id)));
                    } else {
                        entry.remove();
                    }

                    FinishTaskReturn::Finished
                } else {
                    FinishTaskReturn::LeaseLost
                }
            }
            HashMapEntry::Vacant(_) => FinishTaskReturn::LeaseLost,
        };

        if let Some((task_name, signal)) = signal {
            self.emit_signal(task_name, BackendSignal::NewTaskAvailable(signal));
        }

        if let Some((callback_id, callback_payload_json)) = callback_delivery
            && let Some(callback_sink) = self.callbacks.remove(&callback_id)
        {
            callback_sink.send(callback_payload_json);
        }

        let _ = args.callback.send(res);
    }

    fn emit_signal(&mut self, task_name: &'static str, signal: BackendSignal) {
        if let Some(signal_tx) = self.signals.get(task_name) {
            let _ = signal_tx.send(signal);
        }
    }

    fn find_earliest_claimable_task(
        &self,
        task_name: &'static str,
        kind: TaskKind,
        now: Instant,
    ) -> Option<u64> {
        self.tasks
            .iter()
            .filter(|(_, task)| {
                task.task_name == task_name
                    && task.kind == kind
                    && task
                        .available_from
                        .is_none_or(|available_from| available_from <= now)
            })
            .min_by_key(|(task_id, task)| (task.available_from.unwrap_or(now), **task_id))
            .map(|(task_id, _)| *task_id)
    }

    fn find_earliest_future_task(
        &self,
        task_name: &'static str,
        kind: TaskKind,
        now: Instant,
    ) -> Option<Instant> {
        self.tasks
            .values()
            .filter(|task| task.task_name == task_name && task.kind == kind)
            .filter_map(|task| {
                task.available_from
                    .filter(|available_from| *available_from > now)
            })
            .min()
    }
}

#[derive(Debug)]
struct TaskEntry {
    task_name: &'static str,
    payload_json: String,
    callback_id: Option<u64>,
    worker_id: Option<u64>,
    available_from: Option<Instant>,
    kind: TaskKind,
}

impl TaskEntry {
    fn signal_payload(&self, task_id: u64) -> NewTaskAvailableSignalPayload {
        NewTaskAvailableSignalPayload {
            task_id: Some(task_id),
            available_from: self.available_from.unwrap_or_else(Instant::now),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskKind {
    Published,
    Singleton,
}

enum DaemonCommand {
    Subscribe(SubscribeArgs),
    PublishTask(PublishTaskArgs),
    ClaimPublishedTask(ClaimPublishedTaskArgs),
    ClaimEarliestPublishedTask(ClaimEarliestPublishedTaskArgs),
    ClaimSingletonTask(ClaimSingletonTaskArgs),
    RenewTask(RenewTaskArgs),
    FailTask(FailTaskArgs),
    FinishTask(FinishTaskArgs),
}

struct SubscribeArgs {
    task_name: &'static str,
    callback: OneshotSender<SubscribeReturn>,
}

struct SubscribeReturn {
    sub_rx: BroadcastReceiver<BackendSignal>,
}

struct PublishTaskArgs {
    task_name: &'static str,
    payload_json: String,
    callback_sink: Option<Box<dyn CallbackSink>>,
    callback: OneshotSender<PublishTaskReturn>,
}

struct PublishTaskReturn {
    task_id: u64,
}

struct ClaimPublishedTaskArgs {
    task_name: &'static str,
    worker_id: u64,
    task_id: u64,
    lease_expiration: Instant,
    callback: OneshotSender<ClaimTaskReturn>,
}

struct ClaimEarliestPublishedTaskArgs {
    task_name: &'static str,
    worker_id: u64,
    lease_expiration: Instant,
    callback: OneshotSender<ClaimTaskReturn>,
}

struct ClaimSingletonTaskArgs {
    task_name: &'static str,
    worker_id: u64,
    lease_expiration: Instant,
    callback: OneshotSender<ClaimTaskReturn>,
}

enum ClaimTaskReturn {
    Claimed {
        task_id: u64,
        payload_json: String,
        expiration: Instant,
    },
    TaskLeased {
        expiration: Instant,
    },
    TaskUnavailable {
        available_from: Option<Instant>,
    },
    TaskNotFound,
}

impl ClaimTaskReturn {
    fn task_id(&self) -> Option<u64> {
        match self {
            Self::Claimed { task_id, .. } => Some(*task_id),
            Self::TaskLeased { .. } | Self::TaskUnavailable { .. } | Self::TaskNotFound => None,
        }
    }
}

struct RenewTaskArgs {
    worker_id: u64,
    task_id: u64,
    lease_expiration: Instant,
    callback: OneshotSender<RenewTaskReturn>,
}

enum RenewTaskReturn {
    Renewed { expiration: Instant },
    LeaseLost,
}

struct FailTaskArgs {
    worker_id: u64,
    task_id: u64,
    available_from: Option<Instant>,
    callback: OneshotSender<FailTaskReturn>,
}

enum FailTaskReturn {
    Failed,
    LeaseLost,
}

struct FinishTaskArgs {
    worker_id: u64,
    task_id: u64,
    callback_payload_json: String,
    callback: OneshotSender<FinishTaskReturn>,
}

enum FinishTaskReturn {
    Finished,
    LeaseLost,
}
