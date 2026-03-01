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

use serde::{Serialize, de::DeserializeOwned};

pub mod backends;
pub use backends::Backend;

pub mod dispatcher;

pub(crate) mod runtime;

pub trait TaskDefinition {
    /// The instance-specific data provided by the task.
    type Payload: Serialize + DeserializeOwned + Send;
}

pub trait Worker: Send {
    type Task: TaskDefinition;

    fn process(
        self,
        task_id: u64,
        task_payload: <Self::Task as TaskDefinition>::Payload,
    ) -> impl Future<Output = ()> + Send;
}

pub trait WorkerFactory: Send + Sync {
    type Worker: Worker;

    fn build(&self, worker_id: u64) -> Self::Worker;
}
