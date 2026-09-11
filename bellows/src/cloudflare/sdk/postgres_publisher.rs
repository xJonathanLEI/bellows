use std::marker::PhantomData;

use worker::{Env, ObjectNamespace};

use super::sdk_error;
use crate::{
    PublishActivationStrategy, PublishDispatchToken, TaskDefinition,
    backends::postgres_publishing::{PostgresBackendOptions, PostgresPublishingBackend},
    cloudflare::{
        BoxDispatchError,
        publisher::{self, Publisher, Scope},
    },
};

pub use crate::cloudflare::publisher::{
    PostgresPublisherError, PostgresPublisherReceipt, PostgresPublisherStage,
};

/// Configuration for one immediate publication, using an existing schema.
pub struct PostgresPublisherConfig {
    /// Obtain this URL from the request's Hyperdrive binding.
    pub connection_string: String,
    pub options: PostgresBackendOptions,
    pub dispatcher: ObjectNamespace,
}

impl PostgresPublisherConfig {
    pub fn new(
        connection_string: impl Into<String>,
        options: PostgresBackendOptions,
        dispatcher: ObjectNamespace,
    ) -> Self {
        Self {
            connection_string: connection_string.into(),
            options,
            dispatcher,
        }
    }
}

/// Publishes one task, awaits backend shutdown, then dispatches its exact ID and definition name.
///
/// Construction performs no I/O. Synchronous configuration runs once per call; connections and
/// failures are never shared between calls. Callback-bearing definitions are plain publication,
/// without callback registration. Only canonical positive IDs up to 9007199254740991 can dispatch.
/// Success confirms dispatch acceptance, not completion. Inspect [`PostgresPublisherError`] before
/// converting it to a generic Worker error; known publication failures retain their exact receipt.
///
/// Await publication within your request. Normal error paths await shutdown, but dropping the
/// future, termination, or a wasm trap cannot guarantee cleanup. This does not extend request
/// lifetime, own an HTTP endpoint, retry publication, or make publication and dispatch atomic.
/// No future/awaitable publication, callback delivery, application cleanup hooks, transaction
/// participation, or durable recovery is provided.
pub struct PostgresPublisher<T, C> {
    configure: C,
    task: PhantomData<fn() -> T>,
}

impl<T, C> PostgresPublisher<T, C>
where
    T: TaskDefinition<Trigger: PublishActivationStrategy<DispatchToken = PublishDispatchToken>>,
    C: Fn(&Env) -> worker::Result<PostgresPublisherConfig>,
{
    pub fn new(configure: C) -> Self {
        Self {
            configure,
            task: PhantomData,
        }
    }

    /// Publishes immediately and returns only after shutdown and complete dispatch acceptance.
    pub async fn publish(
        &self,
        env: &Env,
        payload: <T::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<PostgresPublisherReceipt, PostgresPublisherError> {
        publisher::publish(
            &RequestPublisher::<T, C> {
                configure: &self.configure,
                env,
                task: PhantomData,
            },
            payload,
        )
        .await
    }
}

struct RequestPublisher<'a, T, C> {
    configure: &'a C,
    env: &'a Env,
    task: PhantomData<fn() -> T>,
}

impl<T, C> Publisher for RequestPublisher<'_, T, C>
where
    T: TaskDefinition<Trigger: PublishActivationStrategy<DispatchToken = PublishDispatchToken>>,
    C: Fn(&Env) -> worker::Result<PostgresPublisherConfig>,
{
    type Task = T;
    type Settings = (String, PostgresBackendOptions);
    type Backend = PostgresPublishingBackend;
    type Namespace = ObjectNamespace;

    fn configure(&self) -> Result<Scope<Self::Settings, Self::Namespace>, BoxDispatchError> {
        let config = (self.configure)(self.env).map_err(sdk_error)?;
        Ok(Scope {
            settings: (config.connection_string, config.options),
            dispatcher: config.dispatcher,
        })
    }

    async fn acquire(
        &self,
        (url, options): Self::Settings,
    ) -> Result<Self::Backend, BoxDispatchError> {
        PostgresPublishingBackend::connect_with_options(&url, options)
            .await
            .map_err(Into::into)
    }

    async fn close(&self, backend: Self::Backend) -> Result<(), BoxDispatchError> {
        backend.close().await.map_err(Into::into)
    }
}
