//! Built-in task backend implementations.
//!
//! Native defaults include in-memory, SQLite, and PostgreSQL. On wasm, disable defaults and enable
//! `cloudflare` for listener-free PostgreSQL publishing and execution. Producers can depend on
//! [`TaskPublishingBackend`], processors on [`TaskExecutionBackend`], and native dispatchers on the
//! full [`Backend`]. Portable deadlines use [`crate::time::Instant`].

use std::{
    error::Error as StdError,
    fmt::{Display, Formatter},
    marker::PhantomData,
};

use tokio::sync::broadcast::Receiver as BroadcastReceiver;

use crate::{AwaitableTask, PublishActivationStrategy, TaskDefinition, time::Instant};

#[cfg(all(not(target_arch = "wasm32"), feature = "in_memory"))]
pub mod in_memory;
#[cfg(all(not(target_arch = "wasm32"), feature = "postgres"))]
pub mod postgres;
#[cfg(any(
    all(not(target_arch = "wasm32"), feature = "postgres"),
    all(target_arch = "wasm32", feature = "cloudflare")
))]
mod postgres_common;
#[cfg(any(
    all(not(target_arch = "wasm32"), feature = "postgres"),
    all(target_arch = "wasm32", feature = "cloudflare")
))]
pub mod postgres_execution;
#[cfg(all(not(target_arch = "wasm32"), feature = "postgres"))]
mod postgres_operations;
#[cfg(any(
    all(not(target_arch = "wasm32"), feature = "postgres"),
    all(target_arch = "wasm32", feature = "cloudflare")
))]
pub mod postgres_publishing;
#[cfg(any(
    all(test, not(target_arch = "wasm32"), feature = "postgres"),
    all(target_arch = "wasm32", feature = "cloudflare")
))]
mod postgres_worker;
#[cfg(all(not(target_arch = "wasm32"), feature = "sqlite"))]
pub mod sqlite;

#[cfg_attr(
    target_arch = "wasm32",
    doc = "[`crate::dispatcher::WorkerDispatcher`]: https://docs.rs/bellows/latest/bellows/dispatcher/struct.WorkerDispatcher.html"
)]
#[cfg_attr(
    all(target_arch = "wasm32", not(feature = "cloudflare")),
    doc = "[`crate::run_task_once`]: https://docs.rs/bellows/latest/bellows/fn.run_task_once.html"
)]
/// Full backend capabilities: publishing, execution, subscriptions, and awaitable publication.
///
/// A full backend connects to the underlying task registry and its associated signal channel.
/// The native [`crate::dispatcher::WorkerDispatcher`] requires this trait. Producers only need
/// [`TaskPublishingBackend`]; [`crate::run_task_once`] only needs [`TaskExecutionBackend`].
/// Implementations must be cheaply cloneable.
///
/// Some [`Backend`] implementations would have "intrinsic" connection between its task registry
/// and the signal channel, such as Postgres where inserting a task row would trigger a `NOTIFY`
/// which generates a signal.
///
/// Other implementations may not have those and might use a two-step register-trigger process
/// where dispatchers handle signaling on the side.
pub trait Backend: TaskPublishingBackend + TaskExecutionBackend {
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

    /// Publishes a task and returns a typed handle that can await the worker callback payload.
    ///
    /// Awaitable publication requires the full backend's callback delivery channel.
    fn publish_awaitable<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> impl Future<Output = Result<AwaitableTask<T::Callback>, PublishTaskError>> + Send
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy;
}

/// Plain task publication without requiring execution or subscriptions.
///
/// Implementations must be cheaply cloneable. Callback-bearing task definitions are supported,
/// but these methods do not register a callback or return an awaitable handle. Singleton task
/// definitions cannot be published.
///
/// Import this trait when calling `publish` or `publish_future` on concrete backend types, even
/// when they implement [`Backend`]. Generic `B: Backend` bounds include this capability.
pub trait TaskPublishingBackend: Clone + Send + Sync {
    /// Publishes a task to be processed by workers.
    fn publish<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> impl Future<Output = Result<PublishedTask, PublishTaskError>> + Send
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy;

    /// Publishes a task to become available for processing at a specific time.
    ///
    /// This records availability; it does not provide a scheduler.
    fn publish_future<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Instant,
    ) -> impl Future<Output = Result<PublishedTask, PublishTaskError>> + Send
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy;
}

#[cfg_attr(
    all(target_arch = "wasm32", not(feature = "cloudflare")),
    doc = "[`crate::run_task_once`]: https://docs.rs/bellows/latest/bellows/fn.run_task_once.html"
)]
/// The task execution operations required by [`crate::run_task_once`].
///
/// Implementations must be cheaply cloneable. This capability does not require publishing or
/// subscriptions; full [`Backend`] implementations provide those capabilities in addition to these
/// execution methods.
///
/// Custom full backends implement these six methods here, plain publication in
/// [`TaskPublishingBackend`], and subscriptions and awaitable publication in [`Backend`].
/// Import this trait when calling execution methods on concrete backend types.
pub trait TaskExecutionBackend: Clone + Send + Sync {
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
    ///
    /// When `available_from` is present, the task is rescheduled instead of being removed.
    fn finish<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        callback_payload: T::Callback,
        available_from: Option<Instant>,
    ) -> impl Future<Output = Result<FinishedTask, FinishTaskError>> + Send
    where
        T: TaskDefinition;
}

/// Boxed backend-specific error source exposed by the public backend API.
///
/// This keeps `bellows` open to arbitrary external backend implementations without forcing an
/// associated error type into the backend traits, while still preserving the original error's
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
    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "in_memory", feature = "sqlite", feature = "postgres")
    ))]
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
