use std::{error::Error, fmt};

use super::{BoxDispatchError, DurableObjectNamespaceLike, dispatch_task};
use crate::{
    PublishActivationStrategy, PublishDispatchToken, TaskDefinition, TaskPublishingBackend,
};

/// An exact published ID. Success confirms dispatch acceptance, not processing success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresPublisherReceipt {
    pub task_id: String,
}

/// The adapter operation that failed, rather than a driver-specific network classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresPublisherStage {
    Configuration,
    Acquisition,
    Publication,
    TaskId,
    BackendClose,
    Dispatch,
}

impl PostgresPublisherStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::Acquisition => "acquisition",
            Self::Publication => "publication",
            Self::TaskId => "task-id",
            Self::BackendClose => "backend-close",
            Self::Dispatch => "dispatch",
        }
    }
}

impl fmt::Display for PostgresPublisherStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The first lifecycle failure, with causes retained for deliberate inspection.
///
/// A receipt means publication is known; close/dispatch failures can be recovered using that ID
/// and the original definition's name without republishing. A `task-id` receipt is unsupported
/// by the processor, not redispatchable.
/// No receipt on a publication failure does not establish rollback. Never retry blindly.
#[derive(Debug)]
pub struct PostgresPublisherError {
    pub stage: PostgresPublisherStage,
    pub cause: BoxDispatchError,
    pub receipt: Option<PostgresPublisherReceipt>,
    /// A later close failure never replaces the primary cause.
    pub backend_close_error: Option<BoxDispatchError>,
}

impl PostgresPublisherError {
    fn new(
        stage: PostgresPublisherStage,
        cause: BoxDispatchError,
        receipt: Option<PostgresPublisherReceipt>,
    ) -> Self {
        Self {
            stage,
            cause,
            receipt,
            backend_close_error: None,
        }
    }
}

impl fmt::Display for PostgresPublisherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL publisher failed at {}", self.stage)
    }
}

impl Error for PostgresPublisherError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

pub(super) struct Scope<S, N> {
    pub settings: S,
    pub dispatcher: N,
}

// Private acquisition/closing seam: native contracts need no PostgreSQL runtime dependency.
pub(super) trait Publisher {
    type Task: TaskDefinition<
        Trigger: PublishActivationStrategy<DispatchToken = PublishDispatchToken>,
    >;
    type Settings;
    type Backend: TaskPublishingBackend;
    type Namespace: DurableObjectNamespaceLike;

    fn configure(&self) -> Result<Scope<Self::Settings, Self::Namespace>, BoxDispatchError>;
    async fn acquire(&self, settings: Self::Settings) -> Result<Self::Backend, BoxDispatchError>;
    async fn close(&self, backend: Self::Backend) -> Result<(), BoxDispatchError>;
}

pub(super) async fn publish<P: Publisher>(
    publisher: &P,
    payload: <<P::Task as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
) -> Result<PostgresPublisherReceipt, PostgresPublisherError> {
    use PostgresPublisherStage::*;

    let scope = publisher
        .configure()
        .map_err(|cause| PostgresPublisherError::new(Configuration, cause, None))?;
    let backend = publisher
        .acquire(scope.settings)
        .await
        .map_err(|cause| PostgresPublisherError::new(Acquisition, cause, None))?;
    let publication = match backend.publish::<P::Task>(payload).await {
        Ok(published) => {
            let receipt = PostgresPublisherReceipt {
                task_id: published.task_id.to_string(),
            };
            // u64 formatting is canonical; the processor additionally requires this safe range.
            if (1..=9_007_199_254_740_991).contains(&published.task_id) {
                Ok(receipt)
            } else {
                Err(PostgresPublisherError::new(
                    TaskId,
                    "task ID must be a canonical positive safe integer".into(),
                    Some(receipt),
                ))
            }
        }
        Err(cause) => Err(PostgresPublisherError::new(Publication, cause.into(), None)),
    };
    let close = publisher.close(backend).await;
    let receipt = match publication {
        Ok(receipt) => {
            if let Err(cause) = close {
                return Err(PostgresPublisherError::new(
                    BackendClose,
                    cause,
                    Some(receipt),
                ));
            }
            receipt
        }
        Err(mut error) => {
            error.backend_close_error = close.err();
            return Err(error);
        }
    };
    dispatch_task(&scope.dispatcher, P::Task::NAME, &receipt.task_id)
        .await
        .map_err(|cause| PostgresPublisherError::new(Dispatch, cause, Some(receipt.clone())))?;
    Ok(receipt)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
