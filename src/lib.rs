//! `bellows` is a Rust framework for durable task processing. It's designed to scale from being
//! embedded in-process to high-throughput distributed systems.
//!
//! `bellows` is CPU-light. It's designed to be fully event-driven and never rely on polling-based
//! primitives (though the extensibility means you're free to build one).
//!
//! ## What is a task
//!
//! In the context of `bellows`, a task is the minimal atomic unit of work with side effects where
//! the context needed for the processing work does not leak across tasks, at least not logically.
//!
//! Tasks should be minimal and atomic. A task should either succeed of fail as a whole. If a task
//! could fail with partial state changes, then it's not atomic and should be broken down. This is
//! to ensure fast system upgrades and clean failover.
//!
//! Task should have side effects. A "side effect" is something that you would rather not have in
//! your application's critical path. This could include external API calls or CPU-bound
//! calculations. If you find yourself defining tasks for pure state transitions, it should have
//! probably been written inline instead.
//!
//! Tasks should not rely on context leakage. A context refers to local temporary state or resources
//! held by a task processor for handling a specific task. Task processing should never logically
//! reuse such context from any previous task, although they could physically do so for performance
//! reasons. This is to ensure each task processing is logically independant such that the system
//! can efficiently upgrade and/or recover from task failures.
//!
//! ## Architecture
//!
//! `bellows` is backend-agnostic and extensible. Each backend contains two main parts:
//!
//! - a persistent assignment registry; and
//! - a low-bandwidth, low-latency, and high-throughput signal channel.

use std::{marker::PhantomData, time::Instant};

use serde::{Serialize, de::DeserializeOwned};

pub mod backends;
pub use backends::Backend;
use backends::{ClaimTaskError, ClaimedTask, SweepTasksError};

pub mod dispatcher;

pub(crate) mod runtime;

pub trait TaskDefinition: Send {
    /// A globally stable backend namespace for this task definition.
    ///
    /// Backends use this name to physically separate tasks belonging to different definitions so a
    /// worker can never successfully claim a task for the wrong payload type.
    const NAME: &'static str;

    /// Specifies how the task should be activated for processing. Use one of the two possible
    /// activation types:
    ///
    /// - [`PublishTrigger<Payload>`]: Use this type to specify that the task is publishable with a
    ///   payload. Task processing starts upon task publication.
    ///
    /// - [`SingletonTrigger`]: Use this type to specify that the task is unpublishable and has a
    ///   single backend-managed instance that is always immediately ready for processing by
    ///   workers.
    type Trigger: ActivationStrategy;
}

/// A trait bound used by two possible types to define the activation behaviour of tasks:
///
/// - [`PublishTrigger<Payload>`]
/// - [`SingletonTrigger`]
pub trait ActivationStrategy: private::Sealed {
    #[doc(hidden)]
    type DispatchToken: Send;

    #[doc(hidden)]
    type EffectivePayload: Send + Sync;

    #[doc(hidden)]
    fn sweep_tasks<B, T>(
        backend: &B,
    ) -> impl Future<Output = Result<Vec<Self::DispatchToken>, SweepTasksError>> + Send
    where
        B: Backend,
        T: TaskDefinition<Trigger = Self>;

    #[doc(hidden)]
    fn claim_task<B, T>(
        backend: &B,
        worker_id: u64,
        dispatch_token: Self::DispatchToken,
        lease_expiration: Instant,
    ) -> impl Future<Output = Result<ClaimedTask<Self::EffectivePayload>, ClaimTaskError>> + Send
    where
        B: Backend,
        T: TaskDefinition<Trigger = Self>;
}

/// Marker trait for activation strategies that support publishing and backend-managed payload
/// claims.
///
/// This is intentionally implemented only for [`PublishTrigger`], so bounds using this trait
/// cannot accidentally match [`SingletonTrigger`].
pub trait PublishActivationStrategy: ActivationStrategy {
    type Payload: Serialize + DeserializeOwned + Send + Sync;
}

pub trait Worker: Send {
    type Task: TaskDefinition;

    fn process(
        self,
        task_id: u64,
        task_payload: <<Self::Task as TaskDefinition>::Trigger as ActivationStrategy>::EffectivePayload,
    ) -> impl Future<Output = ()> + Send;
}

pub trait WorkerFactory: Send + Sync {
    type Worker: Worker;

    fn build(&self, worker_id: u64) -> Self::Worker;
}

/// Indicates that a task is publishable with payload. Workers start processing the task upon its
/// publication. This is the most common type of tasks.
///
/// The type is used in [`TaskDefinition::Trigger`] to specify the task's trigger type.
///
/// This type is one of the two types that implement [`ActivationStrategy`]. If you're defining a
/// task that should exist without being published first, use [`SingletonTrigger`] instead.
pub struct PublishTrigger<Payload>
where
    Payload: Serialize + DeserializeOwned + Send + Sync,
{
    payload_type: PhantomData<Payload>,
}

impl<Payload> private::Sealed for PublishTrigger<Payload> where
    Payload: Serialize + DeserializeOwned + Send + Sync
{
}

impl<Payload> ActivationStrategy for PublishTrigger<Payload>
where
    Payload: Serialize + DeserializeOwned + Send + Sync,
{
    type DispatchToken = u64;
    type EffectivePayload = Payload;

    async fn sweep_tasks<B, T>(backend: &B) -> Result<Vec<Self::DispatchToken>, SweepTasksError>
    where
        B: Backend,
        T: TaskDefinition<Trigger = Self>,
    {
        let swept_tasks = backend.sweep::<T>().await?;
        Ok(swept_tasks.into_iter().map(|task| task.task_id).collect())
    }

    fn claim_task<B, T>(
        backend: &B,
        worker_id: u64,
        dispatch_token: Self::DispatchToken,
        lease_expiration: Instant,
    ) -> impl Future<Output = Result<ClaimedTask<Self::EffectivePayload>, ClaimTaskError>> + Send
    where
        B: Backend,
        T: TaskDefinition<Trigger = Self>,
    {
        backend.claim_published::<T>(worker_id, dispatch_token, lease_expiration)
    }
}

impl<Payload> PublishActivationStrategy for PublishTrigger<Payload>
where
    Payload: Serialize + DeserializeOwned + Send + Sync,
{
    type Payload = Payload;
}

/// Indicates that a task has exactly one backend-managed instance per task definition, and workers
/// start processing it as soon as they come online. These tasks can't be published and therefore
/// don't carry any payload.
///
/// The type is used in [`TaskDefinition::Trigger`] to specify the task's trigger type.
///
/// This type is one of the two types that implement [`ActivationStrategy`]. If you're defining a
/// task that carries publishable payload, use [`PublishTrigger`] instead.
pub struct SingletonTrigger;

impl private::Sealed for SingletonTrigger {}

impl ActivationStrategy for SingletonTrigger {
    type DispatchToken = ();
    type EffectivePayload = ();

    async fn sweep_tasks<B, T>(_backend: &B) -> Result<Vec<Self::DispatchToken>, SweepTasksError>
    where
        B: Backend,
        T: TaskDefinition<Trigger = Self>,
    {
        Ok(vec![()])
    }

    fn claim_task<B, T>(
        backend: &B,
        worker_id: u64,
        _dispatch_token: Self::DispatchToken,
        lease_expiration: Instant,
    ) -> impl Future<Output = Result<ClaimedTask<Self::EffectivePayload>, ClaimTaskError>> + Send
    where
        B: Backend,
        T: TaskDefinition<Trigger = Self>,
    {
        backend.claim_singleton::<T>(worker_id, lease_expiration)
    }
}

mod private {
    /// This trait is marked as `pub` but wrapped in a private module making it inaccessible to
    /// external crates.
    ///
    /// A trait can be made such that all possible implementations are defined only in the current
    /// crate by making [`Sealed`] one of its trait bounds.
    pub trait Sealed {}
}
