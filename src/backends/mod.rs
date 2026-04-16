//! Built-in task backend implementations.

use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
    marker::PhantomData,
    time::Instant,
};

use tokio::sync::broadcast::Receiver as BroadcastReceiver;

use crate::{AwaitableTask, PublishActivationStrategy, TaskDefinition};

#[cfg(feature = "in_memory")]
pub mod in_memory;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

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
pub trait Backend: Clone + Send + Sync {
    /// Subscribes for signals for important task updates for a specific task definition.
    ///
    /// This is usually used by worker dispatchers to react to task availability. Backends must
    /// ensure subscriptions are namespaced by [`TaskDefinition::NAME`] so subscribers only observe
    /// tasks for the requested definition.
    fn subscribe<T>(
        &self,
    ) -> impl Future<Output = Result<BackendSignalSubscription<T>, SubscribeError>> + Send
    where
        T: TaskDefinition;

    /// Publishes a task to be processed by workers.
    fn publish<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> impl Future<Output = Result<PublishedTask, PublishTaskError>> + Send
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy;

    /// Publishes a task to become available for processing at a specific time.
    fn publish_future<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Instant,
    ) -> impl Future<Output = Result<PublishedTask, PublishTaskError>> + Send
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy;

    /// Publishes a task and returns a typed handle that can await the worker callback payload.
    fn publish_awaitable<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> impl Future<Output = Result<AwaitableTask<T::Callback>, PublishTaskError>> + Send
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy;

    /// Claims a specific published task until a lease expiration time.
    fn claim_published<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> impl Future<
        Output = Result<
            ClaimedTask<<<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload>,
            ClaimTaskError,
        >,
    > + Send
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy;

    /// Claims the earliest currently-available published task until a lease expiration time.
    fn claim_earliest_published<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> impl Future<
        Output = Result<
            ClaimedTask<<<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload>,
            ClaimTaskError,
        >,
    > + Send
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy;

    /// Claims the singleton task for a definition until a lease expiration time.
    fn claim_singleton<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> impl Future<Output = Result<ClaimedTask<()>, ClaimTaskError>> + Send
    where
        T: TaskDefinition;

    /// Renews a worker task lease.
    fn renew(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> impl Future<Output = Result<RenewedTaskLease, RenewTaskError>> + Send;

    /// Marks a task as failed and updates when it should become available again.
    fn fail(
        &self,
        worker_id: u64,
        task_id: u64,
        available_from: Option<Instant>,
    ) -> impl Future<Output = Result<FailedTask, FailTaskError>> + Send;

    /// Marks a task as successfully finished.
    ///
    /// Backends must only allow completion by the current lease holder. A worker that still owns
    /// the lease may finish the task even if the lease has technically expired, as long as no
    /// other worker has taken ownership in the meantime.
    fn finish<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        callback_payload: T::Callback,
    ) -> impl Future<Output = Result<FinishedTask, FinishTaskError>> + Send
    where
        T: TaskDefinition;
}

/// Boxed backend-specific error source exposed by the public backend API.
///
/// This keeps `bellows` open to arbitrary external backend implementations without forcing an
/// associated error type into the `Backend` trait, while still preserving the original error's
/// `Display`, `Debug`, and `source` chain for downstream inspection and logging.
pub type BoxBackendError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug)]
pub enum SubscribeError {
    Backend(BoxBackendError),
}

impl Display for SubscribeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "backend subscribe failed: {error}"),
        }
    }
}

impl StdError for SubscribeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Backend(error) => Some(error.as_ref()),
        }
    }
}

pub struct BackendSignalSubscription<T> {
    rx: BroadcastReceiver<BackendSignal>,
    _task: PhantomData<fn() -> T>,
}

impl<T> BackendSignalSubscription<T>
where
    T: TaskDefinition,
{
    pub async fn recv(
        &mut self,
    ) -> Result<NewTaskAvailableSignalPayload, tokio::sync::broadcast::error::RecvError> {
        match self.rx.recv().await? {
            BackendSignal::NewTaskAvailable(signal) => Ok(signal),
        }
    }
}

impl<T> BackendSignalSubscription<T> {
    pub(crate) fn new(rx: BroadcastReceiver<BackendSignal>) -> Self {
        Self {
            rx,
            _task: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
pub enum BackendSignal {
    NewTaskAvailable(NewTaskAvailableSignalPayload),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewTaskAvailableSignalPayload {
    pub task_id: Option<u64>,
    pub available_from: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedTask {
    pub task_id: u64,
}

#[derive(Debug)]
pub enum PublishTaskError {
    Backend(BoxBackendError),
}

impl Display for PublishTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "backend publish failed: {error}"),
        }
    }
}

impl StdError for PublishTaskError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Backend(error) => Some(error.as_ref()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimedTask<T> {
    pub task_id: u64,
    pub task_payload: T,
    pub lease_expiration: Instant,
}

#[derive(Debug)]
pub enum ClaimTaskError {
    Backend(BoxBackendError),
    TaskLeased { expiration: Instant },
    TaskUnavailable { available_from: Option<Instant> },
    TaskNotFound,
}

impl Display for ClaimTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "backend claim failed: {error}"),
            Self::TaskLeased { expiration } => {
                write!(f, "task is currently leased until {expiration:?}")
            }
            Self::TaskUnavailable { available_from } => match available_from {
                Some(available_from) => {
                    write!(
                        f,
                        "no task is currently available; next availability is {available_from:?}"
                    )
                }
                None => f.write_str("no task is currently available"),
            },
            Self::TaskNotFound => f.write_str("task was not found"),
        }
    }
}

impl StdError for ClaimTaskError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Backend(error) => Some(error.as_ref()),
            Self::TaskLeased { .. } | Self::TaskUnavailable { .. } | Self::TaskNotFound => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenewedTaskLease {
    pub new_expiration: Instant,
}

#[derive(Debug)]
pub enum RenewTaskError {
    Backend(BoxBackendError),
    LeaseLost,
}

impl Display for RenewTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "backend renew failed: {error}"),
            Self::LeaseLost => f.write_str("task lease was lost before renewal"),
        }
    }
}

impl StdError for RenewTaskError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Backend(error) => Some(error.as_ref()),
            Self::LeaseLost => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailedTask {
    pub task_id: u64,
}

#[derive(Debug)]
pub enum FailTaskError {
    Backend(BoxBackendError),
    LeaseLost,
}

impl Display for FailTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "backend fail failed: {error}"),
            Self::LeaseLost => f.write_str("task lease was lost before failure was recorded"),
        }
    }
}

impl StdError for FailTaskError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Backend(error) => Some(error.as_ref()),
            Self::LeaseLost => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinishedTask {
    pub task_id: u64,
}

#[derive(Debug)]
pub enum FinishTaskError {
    Backend(BoxBackendError),
    LeaseLost,
}

impl Display for FinishTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend(error) => write!(f, "backend finish failed: {error}"),
            Self::LeaseLost => f.write_str("task lease was lost before finishing"),
        }
    }
}

impl StdError for FinishTaskError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Backend(error) => Some(error.as_ref()),
            Self::LeaseLost => None,
        }
    }
}
