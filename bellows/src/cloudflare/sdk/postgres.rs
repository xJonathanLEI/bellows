use std::pin::Pin;

use worker::{Env, Request, Response};

use super::{incoming_request, outgoing_response, sdk_error};
use crate::{
    PublishActivationStrategy, PublishDispatchToken, TaskDefinition, Worker, WorkerFactory,
    backends::postgres_execution::{PostgresBackendOptions, PostgresExecutionBackend},
    cloudflare::{
        BoxDispatchError,
        processor::{self, Processor, ProcessorTask, Scope},
    },
};

/// A typed published factory registered under its definition's exact name.
pub struct PostgresProcessorTask(ProcessorTask<PostgresExecutionBackend>);

impl PostgresProcessorTask {
    pub fn new<F>(factory: F) -> Self
    where
        F: WorkerFactory + 'static,
        <F::Worker as Worker>::Task: TaskDefinition<
            Trigger: PublishActivationStrategy<DispatchToken = PublishDispatchToken>,
        >,
    {
        Self(ProcessorTask::new(factory))
    }
}

/// Configuration owned by one validated processor request.
pub struct PostgresProcessorConfig {
    /// Obtain this URL from the request's Hyperdrive binding.
    pub connection_string: String,
    pub options: PostgresBackendOptions,
    /// Non-empty registrations with unique, non-empty definition names.
    pub tasks: Vec<PostgresProcessorTask>,
    cleanup: Option<Pin<Box<dyn Future<Output = worker::Result<()>>>>>,
}

impl PostgresProcessorConfig {
    pub fn new(
        connection_string: impl Into<String>,
        options: PostgresBackendOptions,
        tasks: Vec<PostgresProcessorTask>,
    ) -> Self {
        Self {
            connection_string: connection_string.into(),
            options,
            tasks,
            cleanup: None,
        }
    }

    /// Registers cleanup, awaited once whenever configuration returned, even for unknown names.
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

/// A request-scoped PostgreSQL processor routing exact names to typed published factories.
///
/// Unknown names return 404 without acquisition; claims still check the persisted definition name.
/// The synchronous callback reads bindings only after validation. Construction performs no I/O.
/// If configuration fails before returning, it owns its partially created resources.
/// Connections are never retained between requests. HTTP 200 means an attempt ended, not task
/// success. This does not extend request lifetime, retry tasks, or recover from wasm traps.
pub struct PostgresProcessor<C> {
    configure: C,
}

impl<C> PostgresProcessor<C>
where
    C: Fn(&Env) -> worker::Result<PostgresProcessorConfig>,
{
    pub fn new(configure: C) -> Self {
        Self { configure }
    }

    /// Delegates POST `/process` with `{ taskId, taskName }`.
    /// Awaits the runtime, registered cleanup, and backend shutdown.
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

impl<C> Processor for RequestProcessor<'_, C>
where
    C: Fn(&Env) -> worker::Result<PostgresProcessorConfig>,
{
    type Settings = (String, PostgresBackendOptions);
    type Backend = PostgresExecutionBackend;

    fn configure(&self) -> Result<Scope<Self::Settings, Self::Backend>, BoxDispatchError> {
        let config = (self.configure)(self.env).map_err(sdk_error)?;
        Ok(Scope {
            settings: (config.connection_string, config.options),
            tasks: config.tasks.into_iter().map(|task| task.0).collect(),
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
