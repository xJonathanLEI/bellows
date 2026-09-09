use std::pin::Pin;

use worker::{Env, Request, Response};

use super::{incoming_request, outgoing_response, sdk_error};
use crate::{
    PublishActivationStrategy, PublishDispatchToken, TaskDefinition, Worker, WorkerFactory,
    backends::postgres_execution::{PostgresBackendOptions, PostgresExecutionBackend},
    cloudflare::{
        BoxDispatchError,
        processor::{self, Processor, Scope},
    },
};

/// Configuration owned by one validated processor request.
pub struct PostgresProcessorConfig<F> {
    /// Obtain this URL from the request's Hyperdrive binding.
    pub connection_string: String,
    pub options: PostgresBackendOptions,
    pub factory: F,
    cleanup: Option<Pin<Box<dyn Future<Output = worker::Result<()>>>>>,
}

impl<F> PostgresProcessorConfig<F> {
    pub fn new(
        connection_string: impl Into<String>,
        options: PostgresBackendOptions,
        factory: F,
    ) -> Self {
        Self {
            connection_string: connection_string.into(),
            options,
            factory,
            cleanup: None,
        }
    }

    /// Registers owned application cleanup, awaited once even after acquisition failure or no claim.
    ///
    /// Keep abort-safe side-effect connection ownership outside the worker in this future.
    /// Bellows always awaits its own backend shutdown afterwards, even if cleanup fails.
    pub fn with_cleanup(
        mut self,
        cleanup: impl Future<Output = worker::Result<()>> + 'static,
    ) -> Self {
        self.cleanup = Some(Box::pin(cleanup));
        self
    }
}

/// A request-scoped PostgreSQL processor delegate for one published task definition.
///
/// The synchronous callback reads bindings only after validation. Construction performs no I/O.
/// If configuration fails before returning, it owns its partially created resources.
/// Connections are never retained between requests. HTTP 200 means an attempt ended, not task
/// success. This does not extend request lifetime, retry tasks, or recover from wasm traps.
pub struct PostgresProcessor<C> {
    configure: C,
}

impl<C, F> PostgresProcessor<C>
where
    C: Fn(&Env) -> worker::Result<PostgresProcessorConfig<F>>,
    F: WorkerFactory + 'static,
    <F::Worker as Worker>::Task:
        TaskDefinition<Trigger: PublishActivationStrategy<DispatchToken = PublishDispatchToken>>,
{
    pub fn new(configure: C) -> Self {
        Self { configure }
    }

    /// Delegates POST `/process`, awaiting the runtime, registered cleanup, and backend shutdown.
    pub async fn fetch_worker(&self, request: Request, env: &Env) -> worker::Result<Response> {
        let processor = RequestProcessor {
            configure: &self.configure,
            env,
        };
        outgoing_response(processor::fetch(&processor, incoming_request(request)?).await)
    }
}

struct RequestProcessor<'a, C> {
    configure: &'a C,
    env: &'a Env,
}

impl<C, F> Processor for RequestProcessor<'_, C>
where
    C: Fn(&Env) -> worker::Result<PostgresProcessorConfig<F>>,
    F: WorkerFactory + 'static,
    <F::Worker as Worker>::Task:
        TaskDefinition<Trigger: PublishActivationStrategy<DispatchToken = PublishDispatchToken>>,
{
    type Settings = (String, PostgresBackendOptions);
    type Factory = F;
    type Backend = PostgresExecutionBackend;

    fn configure(&self) -> Result<Scope<Self::Settings, F>, BoxDispatchError> {
        let config = (self.configure)(self.env).map_err(sdk_error)?;
        Ok(Scope {
            settings: (config.connection_string, config.options),
            factory: config.factory,
            cleanup: config.cleanup.map(|cleanup| -> processor::Cleanup {
                Box::pin(async move { cleanup.await.map_err(sdk_error) })
            }),
        })
    }

    fn random_bytes(&self) -> Result<[u8; 6], BoxDispatchError> {
        web_crypto_bytes().map_err(sdk_error)
    }

    async fn acquire(
        &self,
        (url, options): Self::Settings,
    ) -> Result<Self::Backend, BoxDispatchError> {
        PostgresExecutionBackend::connect_with_options(&url, options)
            .await
            .map_err(Into::into)
    }

    async fn close(&self, backend: Self::Backend) -> Result<(), BoxDispatchError> {
        backend.close().await.map_err(Into::into)
    }
}

fn web_crypto_bytes() -> worker::Result<[u8; 6]> {
    use worker::{js_sys, wasm_bindgen::JsCast};

    let crypto = js_sys::Reflect::get(&js_sys::global(), &"crypto".into())?;
    let random: js_sys::Function =
        js_sys::Reflect::get(&crypto, &"getRandomValues".into())?.dyn_into()?;
    let bytes = js_sys::Uint8Array::new_with_length(6);
    random.call1(&crypto, &bytes)?;
    let mut result = [0; 6];
    bytes.copy_to(&mut result);
    Ok(result)
}
