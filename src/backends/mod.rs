//! Built-in task backend implementations.

use std::{future::Future, time::Instant};

use tokio::sync::broadcast::Receiver as BroadcastReceiver;

use crate::TaskDefinition;

#[cfg(feature = "in_memory")]
pub mod in_memory;

/// A [`Backend`] is a means to connect to and interact the underlying task registry and its
/// associated signal channel.
///
/// [`Backend`] types must implement [`Clone`] and cheaply cloned.
///
/// Some [`Backend`] implementations would have "intrinsic" connection between its task registry
/// and the signal channel, such as Postgres where inserting a task row would trigger a `NOTIFY`
/// which generates a signal.
///
/// Other implementations may not have those and might use a two-step register-trigger process
/// where dispatchers handle signaling on the side.
pub trait Backend: Clone + Send {
    /// Subscribes for signals for important task updates.
    ///
    /// This is usually used by worker dispatchers to react to task availability.
    fn subscribe(&self) -> impl Future<Output = BackendSignalSubscription> + Send;

    /// Lists tasks that are currently pending and should be considered for dispatch.
    ///
    /// This is used by dispatchers during startup to sweep for pre-existing work that may not have
    /// produced an observable signal for the current subscriber. Backends should return only task
    /// IDs that are currently claimable: tasks that are not durably finished and either have no
    /// active lease or only have an expired lease.
    fn sweep(&self) -> impl Future<Output = Vec<SweptTask>> + Send;

    /// Publishes a task to be processed by workers.
    fn publish<T>(
        &self,
        payload: T::Payload,
    ) -> impl Future<Output = Result<PublishedTask, PublishTaskError>> + Send
    where
        T: TaskDefinition;

    /// Claims a task until a lease expiration time.
    fn claim<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> impl Future<Output = Result<ClaimedTask<T::Payload>, ClaimTaskError>> + Send
    where
        T: TaskDefinition;

    /// Renews a worker task lease.
    fn renew(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> impl Future<Output = Result<RenewedTaskLease, RenewTaskError>> + Send;

    /// Marks a task as successfully finished.
    ///
    /// Backends must only allow completion by the current lease holder. A worker that still owns
    /// the lease may finish the task even if the lease has technically expired, as long as no
    /// other worker has taken ownership in the meantime.
    fn finish(
        &self,
        worker_id: u64,
        task_id: u64,
    ) -> impl Future<Output = Result<FinishedTask, FinishTaskError>> + Send;
}

#[derive(Debug)]
pub struct BackendSignalSubscription {
    rx: BroadcastReceiver<BackendSignal>,
}

impl BackendSignalSubscription {
    pub async fn recv(
        &mut self,
    ) -> Result<BackendSignal, tokio::sync::broadcast::error::RecvError> {
        self.rx.recv().await
    }
}

impl BackendSignalSubscription {
    pub fn new(rx: BroadcastReceiver<BackendSignal>) -> Self {
        Self { rx }
    }
}

#[derive(Debug, Clone)]
pub enum BackendSignal {
    NewTaskAvailable(NewTaskAvailableSignalPayload),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewTaskAvailableSignalPayload {
    pub task_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweptTask {
    pub task_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedTask {
    pub task_id: u64,
}

#[derive(Debug)]
pub enum PublishTaskError {
    PayloadSerialization(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimedTask<T> {
    pub task_id: u64,
    pub task_payload: T,
    pub lease_expiration: Instant,
}

#[derive(Debug)]
pub enum ClaimTaskError {
    PayloadDeserialization(String),
    TaskLeased { expiration: Instant },
    TaskNotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenewedTaskLease {
    pub new_expiration: Instant,
}

#[derive(Debug)]
pub enum RenewTaskError {
    LeaseLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinishedTask {
    pub task_id: u64,
}

#[derive(Debug)]
pub enum FinishTaskError {
    LeaseLost,
}
