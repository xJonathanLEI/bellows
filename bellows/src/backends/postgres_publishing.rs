//! Listener-free PostgreSQL task publication.
//!
//! Enable `postgres` on native targets or `cloudflare` on wasm.
//! Import [`TaskPublishingBackend`] for owned publication, or borrow a caller-owned executor with
//! [`PostgresPublishingBackend::publish_with_executor`]. Awaitable publication requires the full
//! backend's listener-backed callback delivery, not merely a callback-bearing definition.
//! Compose encoded SQL rows without an executor with [`prepare_postgres_publication_from_encoded_rows`].

use crate::{
    PublishActivationStrategy, TaskDefinition,
    backends::{BoxBackendError, PublishTaskError, PublishedTask, TaskPublishingBackend},
    time::Instant,
};

pub use super::postgres_common::PostgresBackendOptions;
use super::postgres_common::{
    PreparedPublication, publication_sql, published_task, validate_schema_name,
};
#[cfg(not(target_arch = "wasm32"))]
use super::postgres_operations::PostgresBackendError as PublicationError;
#[cfg(not(target_arch = "wasm32"))]
use super::postgres_operations::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
use super::postgres_worker::PostgresTaskOperations;
#[cfg(target_arch = "wasm32")]
pub use super::postgres_worker::PostgresWorkerError;

#[cfg(target_arch = "wasm32")]
use PostgresWorkerError as ConnectionError;
#[cfg(target_arch = "wasm32")]
use PostgresWorkerError as PublicationError;
#[cfg(not(target_arch = "wasm32"))]
use sqlx::Error as ConnectionError;

/// A named relation (normally a preceding CTE), not a SQL expression or qualified path.
#[derive(Debug, Clone)]
pub struct PostgresEncodedRowsSource {
    /// Lowercase ASCII identifier matching `[a-z_][a-z0-9_]*`, at most 63 bytes.
    pub relation: String,
    /// Column with one non-null PostgreSQL `text` value per row, already encoded for the task.
    /// Uses the same identifier grammar and length limit as [`Self::relation`].
    pub payload_column: String,
}

/// Inputs to [`prepare_postgres_publication_from_encoded_rows`].
#[derive(Debug, Clone)]
pub struct PostgresEncodedRowsPublicationOptions {
    /// Existing backend schema semantics: qualify when supplied, otherwise use the caller's
    /// search path without setting it or falling back to `public`.
    pub backend: PostgresBackendOptions,
    pub source: PostgresEncodedRowsSource,
    /// Number of preceding binds. Must be in `0..=65534` so the single task-name bind fits
    /// PostgreSQL's 65535-parameter limit. `usize` excludes negative and fractional offsets.
    pub parameter_offset: usize,
}

/// An invalid exact returned ID, retained without floating-point normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresPublicationReceiptError {
    pub task_id: String,
}

impl std::fmt::Display for PostgresPublicationReceiptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PostgreSQL publication task_id must be canonical positive decimal text within i64::MAX")
    }
}

impl std::error::Error for PostgresPublicationReceiptError {}

/// Pure, owned SQL and bind values for immediate publication from encoded rows.
///
/// There is no driver dependency, executor, transaction, or callback registry in this value.
#[derive(Debug, Clone)]
pub struct PostgresPublicationFragment {
    sql: String,
    parameters: [String; 1],
}

impl PostgresPublicationFragment {
    /// One `INSERT ... SELECT ... RETURNING` body suitable for a data-modifying CTE.
    /// Returns `task_id` as exact decimal PostgreSQL `text`.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The sole bind is the task name, placed after the caller's `parameter_offset` binds.
    pub fn parameters(&self) -> &[String; 1] {
        &self.parameters
    }

    /// Decode every returned ID before releasing any receipts. Empty input yields no receipts.
    ///
    /// Pass exact `task_id` text extracted from driver rows. IDs must be canonical positive
    /// decimal text through `i64::MAX`; unlike TypeScript, Rust has no JS-safe-integer ceiling.
    /// Order has no contract and implies no positional correspondence with source rows.
    ///
    /// Receipts remain provisional until caller commit. Call inside the enclosing transaction
    /// and let validation failure abort it: this method neither rolls back nor commits.
    pub fn read_receipts<I, S>(
        &self,
        task_ids: I,
    ) -> Result<Vec<PublishedTask>, PostgresPublicationReceiptError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        task_ids
            .into_iter()
            .map(|text| {
                let text = text.as_ref();
                if text
                    .as_bytes()
                    .first()
                    .is_some_and(|b| matches!(b, b'1'..=b'9'))
                    && text.bytes().all(|b| b.is_ascii_digit())
                    && let Ok(id) = text.parse::<i64>()
                    && let Ok(receipt) = published_task(id)
                {
                    return Ok(receipt);
                }
                Err(PostgresPublicationReceiptError {
                    task_id: text.to_owned(),
                })
            })
            .collect()
    }
}

/// Prepare one immediate, publishable task definition from zero or more already-encoded SQL rows.
///
/// This is a trusted encoding boundary: each source row must contain non-null PostgreSQL `text`
/// compatible with the task's payload deserializer. No Rust serializer is invoked. The source
/// relation and column are validated, quoted identifiers, not interpolated SQL. Schema validation
/// retains the existing backend semantics; only source identifiers enforce the 63-byte limit.
/// Bellows owns the insertion layout/defaults and binds the task name rather than interpolating it.
///
/// Callback-bearing definitions are publishable without callback registration. Singletons are not:
///
/// ```compile_fail
/// use bellows::{SingletonTrigger, TaskDefinition};
/// use bellows::backends::postgres_publishing::prepare_postgres_publication_from_encoded_rows;
/// struct Singleton;
/// impl TaskDefinition for Singleton {
///     const NAME: &str = "singleton";
///     type Trigger = SingletonTrigger;
///     type Callback = ();
/// }
/// let _ = prepare_postgres_publication_from_encoded_rows::<Singleton>;
/// ```
///
/// Preparation and decoding never execute SQL, acquire/close resources, initialize schemas, change
/// `search_path`, retry, dispatch, or commit. Execute once in the caller's transaction, decode before
/// commit, and dispatch only after confirmed commit. Never replay uncertain statement/commit outcomes.
///
/// SQLx binding (the caller owns transaction finalization):
///
/// ```no_run
/// # use bellows::backends::postgres_publishing::PostgresPublicationFragment;
/// # async fn example(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
/// # fragment: PostgresPublicationFragment) -> Result<(), Box<dyn std::error::Error>> {
/// // fragment source: inputs.payload, parameter_offset: 1
/// let sql = format!(
///     "WITH inputs AS (SELECT $1::text AS payload), published AS ({}) SELECT task_id FROM published",
///     fragment.sql()
/// );
/// let ids: Vec<String> = sqlx::query_scalar(&sql)
///     .bind(r#"{"name":"post"}"#)
///     .bind(&fragment.parameters()[0])
///     .fetch_all(&mut **tx).await?;
/// let receipts = fragment.read_receipts(&ids)?;
/// # Ok(()) }
/// ```
///
/// `tokio_postgres` binding (also usable on Workers; the connection driver remains caller-owned):
///
/// ```no_run
/// # use bellows::backends::postgres_publishing::PostgresPublicationFragment;
/// # async fn example(tx: &tokio_postgres::Transaction<'_>,
/// # fragment: PostgresPublicationFragment) -> Result<(), Box<dyn std::error::Error>> {
/// // fragment source: inputs.payload, parameter_offset: 1
/// let sql = format!(
///     "WITH inputs AS (SELECT $1::text AS payload), published AS ({}) SELECT task_id FROM published",
///     fragment.sql()
/// );
/// let rows = tx.query(&sql, &[&r#"{"name":"post"}"#, &fragment.parameters()[0]]).await?;
/// let ids = rows.iter().map(|row| row.try_get::<_, &str>("task_id"))
///     .collect::<Result<Vec<_>, _>>()?;
/// let receipts = fragment.read_receipts(ids)?;
/// # Ok(()) }
/// ```
pub fn prepare_postgres_publication_from_encoded_rows<T>(
    options: PostgresEncodedRowsPublicationOptions,
) -> Result<PostgresPublicationFragment, std::io::Error>
where
    T: TaskDefinition,
    T::Trigger: PublishActivationStrategy,
{
    let table = options.backend.table_name()?;
    let quote_identifier = |identifier: &str| -> Result<String, std::io::Error> {
        if identifier.len() > 63 || validate_schema_name(identifier).is_err() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "PostgreSQL publication source identifiers must match [a-z_][a-z0-9_]* and be at most 63 bytes",
            ));
        }
        Ok(format!("\"{identifier}\""))
    };
    let relation = quote_identifier(&options.source.relation)?;
    let column = quote_identifier(&options.source.payload_column)?;
    let parameter = options
        .parameter_offset
        .checked_add(1)
        .filter(|position| *position <= 65_535)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "PostgreSQL publication parameter_offset must be in 0..=65534",
            )
        })?;
    Ok(PostgresPublicationFragment {
        sql: publication_sql(
            &table,
            [
                &format!("${parameter}"),
                &format!("{relation}.{column}"),
                "NULL",
                "NULL",
            ],
            Some(&relation),
        ),
        parameters: [T::NAME.to_owned()],
    })
}

/// A Bellows-generated parameterized insert and its already-encoded inputs.
///
/// Execute [`Self::sql`] once, binding `$1` to [`Self::task_name`], `$2` to
/// [`Self::payload_json`] (text), `$3` to [`Self::callback_id`] (nullable bigint), and `$4` to
/// [`Self::available_from_unix_ms`] (nullable bigint). Do not reconstruct the insert or interpolate
/// application values into SQL.
#[derive(Debug, Clone, Copy)]
pub struct PostgresPublishQuery<'a> {
    pub sql: &'a str,
    pub task_name: &'a str,
    pub payload_json: &'a str,
    pub callback_id: Option<i64>,
    pub available_from_unix_ms: Option<i64>,
}

/// Query-only access to a caller-owned PostgreSQL executor, including an ORM transaction adapter.
///
/// Return the exact raw signed PostgreSQL `task_id`, without floating-point conversion; Bellows
/// validates the receipt. Return driver errors with their original sources. Implementations
/// need not be cloneable, shared, or `'static`. Bellows borrows the executor only for the operation
/// and never begins, finalizes, or closes its transaction, connection, pool, or connection driver.
///
/// Native adapters support SQLx `PgPool`, `&PgPool`, `PgConnection`, and `Transaction<'_, Postgres>`.
/// Workers adapters support `tokio_postgres::Client` and `tokio_postgres::Transaction<'_>`; their
/// connection driver remains caller-owned.
///
/// A shared pool reuses connections but provides no atomicity with business mutations. Use the same
/// transaction-bound executor for both; an ORM adapter must not fall back to a root client/pool.
/// If an ORM owns a transaction callback, await the outer operation, including commit, before
/// dispatching. No retries, savepoints, schema initialization, session changes, or explicit
/// dispatch are performed.
pub trait PostgresPublishingExecutor: Send {
    fn query_task_id(
        &mut self,
        query: PostgresPublishQuery<'_>,
    ) -> impl Future<Output = Result<i64, BoxBackendError>> + Send;
}

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
/// Owned publication uses its own connections. To join an application transaction, use
/// [`Self::publish_with_executor`] or [`Self::publish_future_with_executor`] instead.
/// Publication does not atomically dispatch to a Durable Object or retry. An error or cancellation
/// after sending an insert does not prove it failed to commit.
///
/// Owned Workers connections must be request-scoped and use Hyperdrive; await [`Self::close`] on
/// success and error paths before returning a response. Caller-owned connections and drivers
/// require caller cleanup instead. Never retain connections across requests.
/// For immediate or future publication followed by dispatch, `cloudflare::sdk::PostgresPublisher` owns that
/// lifecycle. This lower-level backend retains exact `u64` receipts, including IDs above the
/// Cloudflare processor's safe-positive range; the adapter returns string receipts and checks that range.
#[derive(Debug, Clone)]
pub struct PostgresPublishingBackend {
    operations: PostgresTaskOperations,
}

impl PostgresPublishingBackend {
    /// Publishes through a borrowed executor without creating or taking ownership of any resources.
    ///
    /// Options are validated before querying; an explicit schema qualifies the task table, while
    /// `None` uses the executor's existing search path without falling back to `public`.
    /// Callback-bearing definitions are supported but no callback is registered; singleton
    /// definitions are not publishable.
    ///
    /// A receipt (or receipt-validation error) inside a transaction is provisional: the row may
    /// still be pending, and a later caller commit can fail. Only the caller commits or rolls back.
    /// PostgreSQL delivers trigger notifications at commit; explicit dispatch is a separate caller
    /// action after successful commit. A post-commit dispatch failure does not undo publication:
    /// retain the receipt and recover dispatch rather than blindly republishing.
    /// Dropping this future releases the borrow, not the executor, and does not prove rollback.
    pub async fn publish_with_executor<T>(
        executor: &mut impl PostgresPublishingExecutor,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        options: PostgresBackendOptions,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        Self::publish_on_executor::<T>(executor, payload, None, options).await
    }

    /// Publishes future availability through a borrowed executor, with the same ownership and
    /// provisional-receipt rules as [`Self::publish_with_executor`]. This does not schedule work.
    pub async fn publish_future_with_executor<T>(
        executor: &mut impl PostgresPublishingExecutor,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Instant,
        options: PostgresBackendOptions,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        Self::publish_on_executor::<T>(executor, payload, Some(available_from), options).await
    }

    async fn publish_on_executor<T>(
        executor: &mut impl PostgresPublishingExecutor,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Option<Instant>,
        options: PostgresBackendOptions,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let table_name = options
            .table_name()
            .map_err(|error| PublishTaskError::Backend(Box::new(error)))?;
        let publication = PreparedPublication::new::<T>(&table_name, payload, None, available_from);
        #[cfg(not(target_arch = "wasm32"))]
        let publication = publication.map_err(PublicationError::PayloadSerialization);
        #[cfg(target_arch = "wasm32")]
        let publication =
            publication.map_err(|error| PublicationError::PayloadSerialization(error.into()));
        let publication =
            publication.map_err(|error| PublishTaskError::Backend(Box::new(error)))?;
        let task_id = executor
            .query_task_id(publication.query())
            .await
            .map_err(PublishTaskError::Backend)?;
        published_task(task_id).map_err(|error| {
            PublishTaskError::Backend(Box::new(PublicationError::InvalidTaskId(error)))
        })
    }

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

    /// Closes the shared, Bellows-owned publishing connection(s), affecting every clone.
    ///
    /// Waits for the native pool or Workers driver to close; subsequent operations fail.
    /// Workers closes are cancellation-safe and return the same result on repeated calls.
    /// Await before returning a Worker response; dropping clones is insufficient.
    /// The executor-associated functions create no backend or resources for this method to close.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PublishTrigger;
    use serde::{Deserialize, Serialize, Serializer};

    #[derive(Deserialize)]
    struct SqlOnlyPayload;

    impl Serialize for SqlOnlyPayload {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            panic!("encoded-row preparation must not serialize payloads")
        }
    }

    struct CallbackTask;
    impl TaskDefinition for CallbackTask {
        const NAME: &str = "bound ' task 🦀";
        type Trigger = PublishTrigger<SqlOnlyPayload>;
        type Callback = String;
    }

    fn options() -> PostgresEncodedRowsPublicationOptions {
        PostgresEncodedRowsPublicationOptions {
            backend: PostgresBackendOptions::default(),
            source: PostgresEncodedRowsSource {
                relation: "inputs".into(),
                payload_column: "encoded_payload".into(),
            },
            parameter_offset: 3,
        }
    }

    #[test]
    fn preparation_is_pure_and_checks_identifiers_and_parameter_bounds() {
        let fragment =
            prepare_postgres_publication_from_encoded_rows::<CallbackTask>(options()).unwrap();
        assert_eq!(fragment.parameters(), &[CallbackTask::NAME]);
        assert!(!fragment.sql().contains(CallbackTask::NAME));
        assert!(fragment.sql().contains("$4"));
        assert!(fragment.sql().contains("\"inputs\".\"encoded_payload\""));
        for invalid in [
            "",
            "Upper",
            "1a",
            "a.b",
            "a\"b",
            "a b",
            "a\n",
            "é",
            &"a".repeat(64),
        ] {
            for column in [false, true] {
                let mut options = options();
                if column {
                    options.source.payload_column = invalid.into();
                } else {
                    options.source.relation = invalid.into();
                }
                assert!(
                    prepare_postgres_publication_from_encoded_rows::<CallbackTask>(options)
                        .is_err()
                );
            }
        }
        for offset in [65_535, usize::MAX] {
            let mut options = options();
            options.parameter_offset = offset;
            assert!(
                prepare_postgres_publication_from_encoded_rows::<CallbackTask>(options).is_err()
            );
        }
        for offset in [0, 65_534] {
            let mut options = options();
            options.parameter_offset = offset;
            options.source.relation = "a".repeat(63);
            options.source.payload_column = "_".repeat(63);
            // Do not silently strengthen the legacy schema option's validation.
            options.backend.schema = Some("a".repeat(128));
            let fragment =
                prepare_postgres_publication_from_encoded_rows::<CallbackTask>(options).unwrap();
            assert!(fragment.sql().contains(&format!("${}", offset + 1)));
        }
        let mut options = options();
        options.backend.schema = Some("invalid.schema".into());
        assert!(prepare_postgres_publication_from_encoded_rows::<CallbackTask>(options).is_err());
    }

    #[test]
    fn receipts_are_all_or_error_and_retain_exact_positive_bigint_ids() {
        let fragment =
            prepare_postgres_publication_from_encoded_rows::<CallbackTask>(options()).unwrap();
        assert_eq!(fragment.read_receipts(Vec::<String>::new()).unwrap(), []);
        assert_eq!(
            fragment
                .read_receipts([
                    "1",
                    "9007199254740991",
                    "9007199254740992",
                    "9223372036854775807"
                ])
                .unwrap()
                .iter()
                .map(|receipt| receipt.task_id)
                .collect::<Vec<_>>(),
            [
                1,
                9_007_199_254_740_991,
                9_007_199_254_740_992,
                i64::MAX as u64
            ]
        );
        for invalid in [
            "",
            "0",
            "-1",
            "+1",
            "01",
            " 1",
            "1 ",
            "1\n",
            "1.0",
            "1e3",
            "１",
            "9223372036854775808",
            "18446744073709551616",
        ] {
            let error = fragment.read_receipts(["1", invalid, "2"]).unwrap_err();
            assert_eq!(error.task_id, invalid);
        }
    }
}
