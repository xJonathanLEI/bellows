use worker::{Env, ObjectNamespace};

use super::sdk_error;
use crate::{
    backends::postgres_discovery::{
        PostgresBackendOptions, PostgresDiscoveryBackend, PostgresSweepWindow,
    },
    cloudflare::{
        BoxDispatchError,
        sweeper::{self, Scope, Sweeper},
    },
};

pub use crate::cloudflare::sweeper::{
    PostgresSweepCandidate, PostgresSweepReport, PostgresSweeperError, PostgresSweeperSingleton,
    PostgresSweeperStage,
};

/// The entire selected schema must belong to the workload served by this dispatcher.
pub struct PostgresSweeperConfig {
    /// Obtain from this event's Hyperdrive binding, with query caching disabled.
    pub connection_string: String,
    pub options: PostgresBackendOptions,
    pub dispatcher: ObjectNamespace,
    singletons: Vec<PostgresSweeperSingleton>,
}

impl PostgresSweeperConfig {
    pub fn new(
        connection_string: impl Into<String>,
        options: PostgresBackendOptions,
        dispatcher: ObjectNamespace,
    ) -> Self {
        Self {
            connection_string: connection_string.into(),
            options,
            dispatcher,
            singletons: Vec::new(),
        }
    }

    /// Bootstrap definitions after discovery. This does not filter database recovery.
    pub fn with_singletons(
        mut self,
        singletons: impl IntoIterator<Item = PostgresSweeperSingleton>,
    ) -> Self {
        self.singletons = singletons.into_iter().collect();
        self
    }
}

/// Read-only recovery of both task kinds through `global`, plus optional singleton bootstrap.
///
/// Each call owns a fresh backend and awaits dispatches and shutdown, including on errors.
/// Fixed database time and an upper ID bound each pass, not a snapshot. Pagination does not limit
/// dispatch concurrency; concurrent changes may await another sweep. After successful discovery,
/// configured singletons not submitted in page batches receive one bulk ensure request.
///
/// Await within the event, sanitize boundary errors, and register the Cron Trigger separately.
/// With worker 0.8.5, use a rejecting JS promise export: `#[event(scheduled)]` discards returned errors.
/// Recovery latency and cleanup after platform termination are not guaranteed.
pub struct PostgresSweeper<C> {
    configure: C,
}

impl<C: Fn(&Env) -> worker::Result<PostgresSweeperConfig>> PostgresSweeper<C> {
    pub fn new(configure: C) -> Self {
        Self { configure }
    }

    pub async fn sweep(&self, env: &Env) -> Result<PostgresSweepReport, PostgresSweeperError> {
        sweeper::sweep(&EventSweeper {
            configure: &self.configure,
            env,
        })
        .await
    }
}

struct EventSweeper<'a, C> {
    configure: &'a C,
    env: &'a Env,
}

impl<C: Fn(&Env) -> worker::Result<PostgresSweeperConfig>> Sweeper for EventSweeper<'_, C> {
    type Settings = (String, PostgresBackendOptions);
    type Backend = PostgresDiscoveryBackend;
    type Window = PostgresSweepWindow;
    type Namespace = ObjectNamespace;

    fn configure(&self) -> Result<Scope<Self::Settings, Self::Namespace>, BoxDispatchError> {
        let config = (self.configure)(self.env).map_err(sdk_error)?;
        Ok(Scope {
            settings: (config.connection_string, config.options),
            dispatcher: config.dispatcher,
            singletons: config
                .singletons
                .iter()
                .map(|task| task.name().to_owned())
                .collect(),
        })
    }

    async fn acquire(
        &self,
        (url, options): Self::Settings,
    ) -> Result<Self::Backend, BoxDispatchError> {
        PostgresDiscoveryBackend::connect_with_options(&url, options)
            .await
            .map_err(Into::into)
    }

    async fn begin(&self, backend: &Self::Backend) -> Result<Self::Window, BoxDispatchError> {
        backend.begin_sweep().await.map_err(Into::into)
    }

    async fn page(
        &self,
        backend: &Self::Backend,
        window: &Self::Window,
        cursor: Option<i64>,
    ) -> Result<Vec<(i64, String, bool)>, BoxDispatchError> {
        if window.upper_id.is_none() {
            return Ok(Vec::new());
        }
        Ok(backend
            .read_page(window, cursor)
            .await?
            .into_iter()
            .map(|row| (row.task_id, row.task_name, row.is_singleton))
            .collect())
    }

    async fn close(&self, backend: Self::Backend) -> Result<(), BoxDispatchError> {
        backend.close().await.map_err(Into::into)
    }
}
