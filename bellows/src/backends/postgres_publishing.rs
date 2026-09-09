//! Listener-free PostgreSQL task publication.
//!
//! Enable `postgres` on native targets or `cloudflare` on wasm.
//! Import [`TaskPublishingBackend`] to publish with this backend. Awaitable publication requires
//! the full backend's listener-backed callback delivery, not merely a callback-bearing definition.

use crate::{
    PublishActivationStrategy, TaskDefinition,
    backends::{PublishTaskError, PublishedTask, TaskPublishingBackend},
    time::Instant,
};

pub use super::postgres_common::PostgresBackendOptions;
#[cfg(not(target_arch = "wasm32"))]
use super::postgres_operations::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
use super::postgres_worker::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
pub use super::postgres_worker::PostgresWorkerError;

#[cfg(target_arch = "wasm32")]
use PostgresWorkerError as ConnectionError;
#[cfg(not(target_arch = "wasm32"))]
use sqlx::Error as ConnectionError;

#[cfg_attr(
    target_arch = "wasm32",
    doc = "[`super::postgres::initialize_postgres_schema`]: https://docs.rs/bellows/latest/bellows/backends/postgres/fn.initialize_postgres_schema.html"
)]
/// PostgreSQL backend for producers that only need [`TaskPublishingBackend`].
///
/// Clones share a native SQLx pool or one request-scoped Workers connection. No listener, callback
/// registry, or execution API is provided. Callback-bearing definitions can still be published;
/// insert triggers continue notifying native consumers.
///
/// Initialize tables separately with [`super::postgres::initialize_postgres_schema`].
/// Future publication stores availability, not a scheduler or a future Worker request.
/// Publication does not atomically dispatch to a Durable Object, retry, or join an application
/// transaction. An error or cancellation after sending an insert does not prove it failed to commit.
///
/// Workers must use a request-scoped Hyperdrive connection and await [`Self::close`] on success and
/// error paths before returning a response. Never retain connections across requests.
/// For immediate publication followed by dispatch, `cloudflare::sdk::PostgresPublisher` owns that
/// lifecycle. This lower-level backend retains exact `u64` receipts, including IDs above the
/// Cloudflare processor's safe-positive range; the adapter returns string receipts and checks that range.
#[derive(Debug, Clone)]
pub struct PostgresPublishingBackend {
    operations: PostgresTaskOperations,
}

impl PostgresPublishingBackend {
    /// Connects using the database connection's default search path, without a listener.
    ///
    /// Workers should prefer [`Self::connect_with_options`] with an explicit schema.
    pub async fn connect(database_url: &str) -> Result<Self, ConnectionError> {
        Self::connect_with_options(database_url, PostgresBackendOptions::default()).await
    }

    #[cfg_attr(
        target_arch = "wasm32",
        doc = "[`sqlx::Error::Configuration`]: https://docs.rs/sqlx/latest/sqlx/enum.Error.html#variant.Configuration"
    )]
    /// Connects with an optional, existing PostgreSQL schema, without a listener.
    ///
    /// Validates schema names before connecting; does not initialize tables or set `search_path`.
    /// Invalid schemas return [`sqlx::Error::Configuration`] natively or `PostgresWorkerError` on Workers.
    ///
    /// Workers require one TCP host and nonzero port (default 5432). Host lists, `hostaddr`, session
    /// options, and direct TLS are unsupported. `sslmode` controls SDK STARTTLS with host-verified
    /// certificates; native socket timeout/keepalive settings do not apply.
    pub async fn connect_with_options(
        database_url: &str,
        options: PostgresBackendOptions,
    ) -> Result<Self, ConnectionError> {
        Ok(Self {
            operations: PostgresTaskOperations::connect(database_url, options).await?,
        })
    }

    /// Closes the shared publishing connection(s), affecting every clone.
    ///
    /// Waits for the native pool or Workers driver to close; subsequent operations fail.
    /// Workers closes are cancellation-safe and return the same result on repeated calls.
    /// Await before returning a Worker response; dropping clones is insufficient.
    pub async fn close(&self) -> Result<(), ConnectionError> {
        self.operations.close().await
    }
}

impl TaskPublishingBackend for PostgresPublishingBackend {
    async fn publish<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.operations.publish::<T>(payload, None, None).await
    }

    async fn publish_future<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Instant,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.operations
            .publish::<T>(payload, None, Some(available_from))
            .await
    }
}
