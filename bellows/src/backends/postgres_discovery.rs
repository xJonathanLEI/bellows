//! Read-only, listener-free PostgreSQL discovery. No payloads are decoded or tasks claimed.
//!
//! Enable `postgres` natively or `cloudflare` on wasm. Initialize schemas separately.

pub use super::postgres_common::{
    PostgresBackendOptions, PostgresDiscoveryCandidate, PostgresSweepWindow,
};
#[cfg(not(target_arch = "wasm32"))]
use super::postgres_operations::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
use super::postgres_worker::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
pub use super::postgres_worker::PostgresWorkerError;
#[cfg(target_arch = "wasm32")]
use PostgresWorkerError as DiscoveryError;
#[cfg(not(target_arch = "wasm32"))]
use sqlx::Error as DiscoveryError;

/// Owned discovery connections without listeners, schema initialization, or task definitions.
///
/// Clones share a native pool or request-scoped Workers connection. On Workers, use Hyperdrive
/// with query caching disabled and await [`Self::close`] on success and error paths; never retain
/// connections across events.
/// The selected schema must belong entirely to the workload consuming the discovered identities.
#[derive(Debug, Clone)]
pub struct PostgresDiscoveryBackend {
    operations: PostgresTaskOperations,
}

impl PostgresDiscoveryBackend {
    /// Uses the connection's default search path without changing it.
    pub async fn connect(database_url: &str) -> Result<Self, DiscoveryError> {
        Self::connect_with_options(database_url, PostgresBackendOptions::default()).await
    }

    /// Validates and qualifies an optional existing schema before connecting.
    pub async fn connect_with_options(
        database_url: &str,
        options: PostgresBackendOptions,
    ) -> Result<Self, DiscoveryError> {
        Ok(Self {
            operations: PostgresTaskOperations::connect(database_url, options).await?,
        })
    }

    /// Captures database statement time and the maximum task ID in one read.
    pub async fn begin_sweep(&self) -> Result<PostgresSweepWindow, DiscoveryError> {
        self.operations.begin_sweep().await
    }

    /// Reads at most 100 eligible identities in numeric ID order within the fixed window.
    ///
    /// Start with `None`, then pass the last returned ID, even if consuming that candidate failed.
    /// IDs retain the full signed-BIGINT range; consumer-specific ID validation belongs after discovery.
    /// An empty page ends the pass. This is not a snapshot: concurrent changes behind the cursor
    /// are left for the next pass. Eligibility does not reserve execution; claims remain authoritative.
    /// Rows are eligible when availability is null or at/before the cutoff, including
    /// expired leases with populated owners, for both published and singleton tasks.
    pub async fn read_page(
        &self,
        window: &PostgresSweepWindow,
        last_seen_id: Option<i64>,
    ) -> Result<Vec<PostgresDiscoveryCandidate>, DiscoveryError> {
        self.operations.read_page(window, last_seen_id).await
    }

    /// Awaits pool/driver shutdown, affecting all clones. Dropping a clone is insufficient.
    pub async fn close(&self) -> Result<(), DiscoveryError> {
        self.operations.close().await
    }
}
