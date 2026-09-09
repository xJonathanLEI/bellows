//! Required real-PostgreSQL contracts for the same operation/driver lifecycle used by Workers.
//! Like the native backend suite, these require PostgreSQL on localhost:5432; absence is a failure.

use std::{
    future::poll_fn,
    pin::Pin,
    sync::atomic::{AtomicU32, Ordering},
    task::Poll,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::oneshot, time::timeout};
use tokio_postgres::{NoTls, config::SslMode};

use super::*;
use crate::{PublishTrigger, SingletonTrigger, backends::postgres::initialize_postgres_schema};

const DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432/postgres";
const LIMIT: Duration = Duration::from_secs(5);
static NEXT_FIXTURE: AtomicU32 = AtomicU32::new(0);

struct Echo;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Payload {
    name: String,
}
impl TaskDefinition for Echo {
    const NAME: &str = "worker_echo";
    type Callback = String;
    type Trigger = PublishTrigger<Payload>;
}

struct Other;
impl TaskDefinition for Other {
    const NAME: &str = "worker_other";
    type Callback = ();
    type Trigger = PublishTrigger<()>;
}

#[derive(Deserialize)]
struct RejectedCallback;
impl Serialize for RejectedCallback {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "intentional callback serialization failure",
        ))
    }
}

struct RejectedCallbackTask;
impl TaskDefinition for RejectedCallbackTask {
    const NAME: &str = Echo::NAME;
    type Callback = RejectedCallback;
    type Trigger = PublishTrigger<Payload>;
}

struct Singleton;
impl TaskDefinition for Singleton {
    const NAME: &str = "worker_singleton";
    type Callback = String;
    type Trigger = SingletonTrigger;
}

struct PublishedSingletonName;
impl TaskDefinition for PublishedSingletonName {
    const NAME: &str = Singleton::NAME;
    type Callback = ();
    type Trigger = PublishTrigger<()>;
}

fn expiration() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

async fn pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "expected controlled pending I/O"
        );
        Poll::Ready(())
    })
    .await;
}

struct Fixture {
    admin: Client,
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    schema: String,
    table: String,
    callback_id: i64,
}

#[derive(Debug, PartialEq, Eq)]
struct State {
    owner: Option<i64>,
    available: Option<i64>,
    callback: Option<i64>,
    payload: String,
}

impl Fixture {
    async fn new() -> Self {
        let (admin, connection) = timeout(LIMIT, tokio_postgres::connect(DATABASE_URL, NoTls))
            .await
            .expect("PostgreSQL connection timed out")
            .expect("required PostgreSQL 17 server must be reachable on localhost:5432");
        let driver = tokio::spawn(connection);
        let nonce = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(crate::time::clock::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let callback_id = (i64::from(std::process::id()) << 32) | i64::from(nonce);
        let schema = format!("worker_{}_{timestamp}_{nonce}", std::process::id());
        admin
            .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
            .await
            .unwrap();
        initialize_postgres_schema(DATABASE_URL, &schema)
            .await
            .unwrap();
        let table = format!("\"{schema}\".\"bellows_tasks\"");
        Self {
            admin,
            driver,
            schema,
            table,
            callback_id,
        }
    }

    fn url(&self) -> String {
        format!(
            "{DATABASE_URL}?application_name={}&sslmode=disable",
            self.schema
        )
    }

    async fn connect(&self) -> PostgresTaskOperations {
        connect(
            &self.url(),
            PostgresBackendOptions {
                schema: Some(self.schema.clone()),
            },
        )
        .await
        .unwrap()
    }

    async fn insert(&self, name: &str, payload: &str, available: Option<i64>) -> u64 {
        let row = self.admin.query_typed_one(
            &format!("INSERT INTO {} (task_name, payload_json, available_from_unix_ms) VALUES ($1, $2, $3) RETURNING task_id", self.table),
            &[(&name, Type::TEXT), (&payload, Type::TEXT), (&available, Type::INT8)],
        ).await.unwrap();
        row.get::<_, i64>(0).try_into().unwrap()
    }

    async fn echo(&self, name: &str) -> u64 {
        self.insert(
            Echo::NAME,
            &serde_json::to_string(&Payload { name: name.into() }).unwrap(),
            None,
        )
        .await
    }

    async fn state(&self, id: u64) -> Option<State> {
        self.admin.query_typed_opt(
            &format!("SELECT lease_worker_id, available_from_unix_ms, callback_id, payload_json FROM {} WHERE task_id = $1", self.table),
            &[(&(id as i64), Type::INT8)],
        ).await.unwrap().map(|row| State {
            owner: row.get(0), available: row.get(1), callback: row.get(2), payload: row.get(3),
        })
    }

    async fn make_available(&self, id: u64) {
        self.admin
            .execute_typed(
                &format!(
                    "UPDATE {} SET available_from_unix_ms = 0 WHERE task_id = $1",
                    self.table
                ),
                &[(&(id as i64), Type::INT8)],
            )
            .await
            .unwrap();
    }

    async fn callback(&self, id: u64) {
        self.admin
            .execute_typed(
                &format!(
                    "UPDATE {} SET callback_id = $2 WHERE task_id = $1",
                    self.table
                ),
                &[(&(id as i64), Type::INT8), (&self.callback_id, Type::INT8)],
            )
            .await
            .unwrap();
    }

    async fn replace_id(&self, id: u64, replacement: i64) {
        self.admin.execute_typed(
            &format!(
                "INSERT INTO {table} (task_id, task_name, payload_json) OVERRIDING SYSTEM VALUE \
                 SELECT $1, task_name, payload_json FROM {table} WHERE task_id = $2",
                table = self.table,
            ),
            &[(&replacement, Type::INT8), (&(id as i64), Type::INT8)],
        ).await.unwrap();
        self.admin
            .execute_typed(
                &format!("DELETE FROM {} WHERE task_id = $1", self.table),
                &[(&(id as i64), Type::INT8)],
            )
            .await
            .unwrap();
    }

    async fn wait_for_blocked_query(&self) {
        timeout(LIMIT, async {
            loop {
                let blocked: bool = self.admin.query_typed_one(
                    "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock')",
                    &[(&self.schema, Type::TEXT)],
                ).await.unwrap().get(0);
                if blocked { return; }
                tokio::task::yield_now().await;
            }
        }).await.expect("operation never reached its database gate");
    }

    async fn cleanup(self) {
        // Driver exit is awaited by tests, then allow PostgreSQL to observe the TCP close.
        timeout(LIMIT, async {
            loop {
                let count: i64 = self
                    .admin
                    .query_typed_one(
                        "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
                        &[(&self.schema, Type::TEXT)],
                    )
                    .await
                    .unwrap()
                    .get(0);
                if count == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("request-scoped PostgreSQL connections leaked");
        self.admin
            .batch_execute(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .await
            .unwrap();
        drop(self.admin);
        timeout(LIMIT, self.driver).await.unwrap().unwrap().unwrap();
    }
}

async fn connect(
    url: &str,
    options: PostgresBackendOptions,
) -> Result<PostgresTaskOperations, PostgresWorkerError> {
    let table = options
        .table_name()
        .map_err(|error| PostgresWorkerError::InvalidSchema(Arc::new(error)))?;
    let (config, host, port) = connection_config(url)?;
    let socket = TcpStream::connect((host.as_str(), port)).await.unwrap();
    let (client, connection) = config.connect_raw(socket, NoTls).await?;
    Ok(PostgresTaskOperations::from_connection(
        client, connection, table,
    ))
}

#[tokio::test]
async fn configuration_validation_and_safe_error_sources() {
    for mode in ["disable", "prefer", "require"] {
        let (config, host, port) = connection_config(&format!(
            "postgres://user:secret@example.com:6543/db?sslmode={mode}"
        ))
        .unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 6543);
        assert_eq!(
            config.get_ssl_mode(),
            match mode {
                "disable" => SslMode::Disable,
                "require" => SslMode::Require,
                _ => SslMode::Prefer,
            }
        );
    }
    assert_eq!(
        connection_config("host=localhost user=postgres").unwrap().2,
        5432
    );
    assert_eq!(
        connection_config("postgres://user@[::1]/db").unwrap().1,
        "::1"
    );
    for url in [
        "secret",
        "user=postgres password=secret",
        "host=/tmp password=secret",
        "host='' password=secret",
        "host='a b' password=secret",
        "host=a,b password=secret",
        "host=a port=0 password=secret",
        "host=a port=5432,5433 password=secret",
        "host=a hostaddr=127.0.0.1 password=secret",
        "host=a options='-c search_path=secret'",
        "host=a target_session_attrs=read-write",
        "host=a sslnegotiation=direct password=secret",
        "host=a sslmode=secret",
    ] {
        let error = connection_config(url).unwrap_err();
        assert!(matches!(error, PostgresWorkerError::Configuration(_)));
        assert!(!format!("{error:?} {error}").contains("secret"));
        assert!(error.source().is_none());
    }
    for schema in ["", "Public", "a.b", "a\"b", "a\n", "é"] {
        let error = connect(
            "not a URL",
            PostgresBackendOptions {
                schema: Some(schema.into()),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, PostgresWorkerError::InvalidSchema(_)));
        assert!(error.source().unwrap().is::<io::Error>());
    }
    // JS objects never enter the error/source chain; the wasm adapter uses this safe typed shape.
    let error = PostgresWorkerError::Socket(Arc::new(io::Error::other("socket creation failed")));
    let boxed: crate::backends::BoxBackendError = Box::new(error);
    assert!(boxed.source().unwrap().is::<io::Error>());
    fn thread_safe<T: Send + Sync>() {}
    thread_safe::<PostgresTaskOperations>();
    thread_safe::<PostgresWorkerError>();
}

#[tokio::test]
async fn published_claims_use_database_payload_and_classify_unclaimable_rows() {
    let f = Fixture::new().await;
    let ops = f.connect().await;
    assert!(matches!(
        ops.claim_published::<Echo>(17, 999, expiration()).await,
        Err(ClaimTaskError::TaskNotFound)
    ));
    assert!(matches!(
        ops.renew(17, 999, expiration()).await,
        Err(RenewTaskError::LeaseLost)
    ));
    assert!(matches!(
        ops.fail(17, 999, None).await,
        Err(FailTaskError::LeaseLost)
    ));
    assert!(matches!(
        ops.finish::<Echo>(17, 999, String::new(), None).await,
        Err(FinishTaskError::LeaseLost)
    ));
    assert!(matches!(
        ops.claim_earliest_published::<Echo>(17, expiration()).await,
        Err(ClaimTaskError::TaskUnavailable {
            available_from: None
        })
    ));
    let future = unix_timestamp_ms(SystemTime::now()) + 120_000;
    let future_id = f
        .insert(Echo::NAME, r#"{"name":"future"}"#, Some(future))
        .await;
    assert!(
        matches!(ops.claim_published::<Echo>(17, future_id, expiration()).await, Err(ClaimTaskError::TaskUnavailable { available_from: Some(time) }) if time > expiration())
    );
    assert!(
        matches!(ops.claim_earliest_published::<Echo>(17, expiration()).await, Err(ClaimTaskError::TaskUnavailable { available_from: Some(time) }) if time > expiration())
    );
    let id = f.echo("Robert'); DROP TABLE bellows_tasks; -- 🦀").await;
    assert!(matches!(
        ops.claim_published::<Other>(17, id, expiration()).await,
        Err(ClaimTaskError::TaskNotFound)
    ));
    assert!(matches!(
        ops.claim_earliest_published::<Other>(17, expiration())
            .await,
        Err(ClaimTaskError::TaskUnavailable {
            available_from: None
        })
    ));
    let deadline = expiration();
    let claimed = ops.claim_published::<Echo>(17, id, deadline).await.unwrap();
    assert_eq!(claimed.task_id, id);
    assert_eq!(claimed.lease_expiration, deadline);
    assert_eq!(
        claimed.task_payload.name,
        "Robert'); DROP TABLE bellows_tasks; -- 🦀"
    );
    assert_eq!(f.state(id).await.unwrap().owner, Some(17));
    assert!(
        matches!(ops.claim_published::<Echo>(18, id, deadline).await, Err(ClaimTaskError::TaskLeased { expiration: time }) if time > Instant::now())
    );
    assert!(matches!(
        ops.renew(18, id, deadline).await,
        Err(RenewTaskError::LeaseLost)
    ));
    assert!(matches!(
        ops.fail(18, id, None).await,
        Err(FailTaskError::LeaseLost)
    ));
    assert!(matches!(
        ops.finish::<Echo>(18, id, String::new(), None).await,
        Err(FinishTaskError::LeaseLost)
    ));
    let later = deadline + Duration::from_secs(60);
    assert_eq!(
        ops.renew(17, id, later).await.unwrap().new_expiration,
        later
    );
    assert!(
        f.state(id).await.unwrap().available.unwrap()
            > unix_timestamp_ms(SystemTime::now()) + 60_000
    );
    f.callback(id).await;
    ops.fail(17, id, Some(later)).await.unwrap();
    let state = f.state(id).await.unwrap();
    assert_eq!(state.owner, None);
    assert_eq!(
        state.callback,
        Some(f.callback_id),
        "failure must retain the callback"
    );
    assert!(state.available.unwrap() > future - 1000);
    f.make_available(id).await;
    ops.claim_published::<Echo>(17, id, deadline).await.unwrap();
    ops.finish::<Echo>(17, id, String::new(), None)
        .await
        .unwrap();
    assert!(
        f.state(id).await.is_none(),
        "completion must commit before returning"
    );
    assert!(matches!(
        ops.claim_published::<Echo>(17, id, deadline).await,
        Err(ClaimTaskError::TaskNotFound)
    ));
    ops.close().await.unwrap();
    ops.clone().close().await.unwrap();
    assert!(matches!(
        ops.client().await,
        Err(PostgresWorkerError::Closed)
    ));
    f.cleanup().await;
}

#[tokio::test]
async fn earliest_ordering_skips_locked_rows_and_competing_owners_cannot_steal() {
    let f = Fixture::new().await;
    let ops = f.connect().await;
    let other = f.connect().await;
    let old = f.insert(Echo::NAME, r#"{"name":"old"}"#, Some(1)).await;
    let first = f.echo("first").await;
    let second = f.echo("second").await;
    f.admin.batch_execute("BEGIN").await.unwrap();
    f.admin
        .query_typed_one(
            &format!(
                "SELECT task_id FROM {} WHERE task_id = $1 FOR UPDATE",
                f.table
            ),
            &[(&(first as i64), Type::INT8)],
        )
        .await
        .unwrap();
    let claimed = timeout(
        LIMIT,
        ops.claim_earliest_published::<Echo>(17, expiration()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        claimed.task_id, second,
        "NULL availability sorts first, locked row is skipped"
    );
    f.admin.batch_execute("ROLLBACK").await.unwrap();
    let (a, b) = tokio::join!(
        ops.claim_published::<Echo>(17, first, expiration()),
        other.claim_published::<Echo>(18, first, expiration()),
    );
    assert!(matches!(
        (&a, &b),
        (Ok(_), Err(ClaimTaskError::TaskLeased { .. }))
            | (Err(ClaimTaskError::TaskLeased { .. }), Ok(_))
    ));
    let original_owner = if a.is_ok() { 17 } else { 18 };
    assert_eq!(
        ops.claim_earliest_published::<Echo>(19, expiration())
            .await
            .unwrap()
            .task_id,
        old
    );
    f.make_available(first).await;
    other
        .claim_published::<Echo>(20, first, expiration())
        .await
        .unwrap();
    assert!(matches!(
        ops.renew(original_owner, first, expiration()).await,
        Err(RenewTaskError::LeaseLost)
    ));
    assert!(matches!(
        ops.fail(original_owner, first, None).await,
        Err(FailTaskError::LeaseLost)
    ));
    assert!(matches!(
        ops.finish::<Echo>(original_owner, first, String::new(), None)
            .await,
        Err(FinishTaskError::LeaseLost)
    ));
    assert_eq!(f.state(first).await.unwrap().owner, Some(20));
    ops.close().await.unwrap();
    other.close().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn singleton_claims_are_atomic_guard_names_and_retain_rescheduled_rows() {
    let f = Fixture::new().await;
    let ops = f.connect().await;
    let other = f.connect().await;
    let (a, b) = tokio::join!(
        ops.claim_singleton::<Singleton>(17, expiration()),
        other.claim_singleton::<Singleton>(18, expiration()),
    );
    assert!(matches!(
        (&a, &b),
        (Ok(_), Err(ClaimTaskError::TaskLeased { .. }))
            | (Err(ClaimTaskError::TaskLeased { .. }), Ok(_))
    ));
    let (owner, id) = match (a, b) {
        (Ok(a), _) => (17, a.task_id),
        (_, Ok(b)) => (18, b.task_id),
        _ => unreachable!(),
    };
    assert!(matches!(
        ops.claim_published::<PublishedSingletonName>(19, id, expiration())
            .await,
        Err(ClaimTaskError::TaskNotFound)
    ));
    assert!(matches!(
        ops.claim_earliest_published::<PublishedSingletonName>(19, expiration())
            .await,
        Err(ClaimTaskError::TaskUnavailable {
            available_from: None
        })
    ));
    assert!(matches!(
        ops.finish::<PublishedSingletonName>(owner, id, (), None)
            .await,
        Err(FinishTaskError::LeaseLost)
    ));
    assert!(matches!(
        ops.finish::<PublishedSingletonName>(owner, id, (), Some(expiration()))
            .await,
        Err(FinishTaskError::LeaseLost)
    ));
    ops.renew(owner, id, expiration()).await.unwrap();
    ops.fail(owner, id, None).await.unwrap();
    assert_eq!(f.state(id).await.unwrap().owner, None);
    assert_eq!(
        ops.claim_singleton::<Singleton>(19, expiration())
            .await
            .unwrap()
            .task_id,
        id
    );
    ops.finish::<Singleton>(19, id, String::new(), Some(expiration()))
        .await
        .unwrap();
    assert!(matches!(
        ops.claim_singleton::<Singleton>(19, expiration()).await,
        Err(ClaimTaskError::TaskUnavailable {
            available_from: Some(_)
        })
    ));
    f.make_available(id).await;
    assert_eq!(
        ops.claim_singleton::<Singleton>(20, expiration())
            .await
            .unwrap()
            .task_id,
        id
    );
    ops.finish::<Singleton>(20, id, String::new(), None)
        .await
        .unwrap();
    let state = f.state(id).await.unwrap();
    assert_eq!(
        (
            state.owner,
            state.available,
            state.callback,
            state.payload.as_str()
        ),
        (None, None, None, "null")
    );
    // A conflicting unique key belonging to a different task name must not be overwritten.
    f.admin
        .execute_typed(
            &format!("UPDATE {} SET task_name = $1 WHERE task_id = $2", f.table),
            &[(&Other::NAME, Type::TEXT), (&(id as i64), Type::INT8)],
        )
        .await
        .unwrap();
    assert!(matches!(
        ops.claim_singleton::<Singleton>(21, expiration()).await,
        Err(ClaimTaskError::TaskNotFound)
    ));
    assert_eq!(f.state(id).await.unwrap().owner, None);
    let published = f.echo("published").await;
    ops.claim_published::<Echo>(17, published, expiration())
        .await
        .unwrap();
    assert!(matches!(
        ops.finish::<Singleton>(17, published, String::new(), None)
            .await,
        Err(FinishTaskError::LeaseLost)
    ));
    ops.close().await.unwrap();
    other.close().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn callbacks_are_delivered_transactionally_for_deletion_and_rescheduling() {
    let f = Fixture::new().await;
    let ops = f.connect().await;
    let mut listener = sqlx::postgres::PgListener::connect(DATABASE_URL)
        .await
        .unwrap();
    listener.listen(NOTIFY_CHANNEL).await.unwrap();
    for kind in 0..3 {
        let id = if kind == 2 {
            ops.claim_singleton::<Singleton>(17, expiration())
                .await
                .unwrap()
                .task_id
        } else {
            let id = f.echo("callback").await;
            ops.claim_published::<Echo>(17, id, expiration())
                .await
                .unwrap();
            id
        };
        f.callback(id).await;
        let before = f.state(id).await;
        // PostgreSQL rejects an oversized NOTIFY. Deletion or rescheduling must roll back with it.
        let result = if kind == 2 {
            ops.finish::<Singleton>(17, id, "x".repeat(9000), Some(expiration()))
                .await
        } else {
            ops.finish::<Echo>(17, id, "x".repeat(9000), (kind == 1).then(expiration))
                .await
        };
        let Err(FinishTaskError::Backend(error)) = result else {
            panic!("expected notification failure")
        };
        assert!(error.source().unwrap().is::<tokio_postgres::Error>());
        assert_eq!(
            f.state(id).await,
            before,
            "failed notification must not commit partial completion"
        );
        let payload = "hello \"🦀\"";
        if kind == 2 {
            ops.finish::<Singleton>(17, id, payload.into(), Some(expiration()))
                .await
                .unwrap();
        } else {
            ops.finish::<Echo>(17, id, payload.into(), (kind == 1).then(expiration))
                .await
                .unwrap();
        }
        timeout(LIMIT, async {
            loop {
                let notification = listener.recv().await.unwrap();
                if let NotificationPayload::TaskCallback {
                    task_name,
                    callback_id,
                    callback_payload_json,
                } = serde_json::from_str(notification.payload()).unwrap()
                    && callback_id == f.callback_id
                {
                    assert_eq!(
                        task_name,
                        if kind == 2 {
                            Singleton::NAME
                        } else {
                            Echo::NAME
                        }
                    );
                    assert_eq!(
                        serde_json::from_str::<String>(&callback_payload_json).unwrap(),
                        payload
                    );
                    break;
                }
            }
        })
        .await
        .expect("typed callback notification was not delivered");
        if kind == 0 {
            assert!(f.state(id).await.is_none());
        } else {
            let state = f.state(id).await.unwrap();
            assert_eq!((state.owner, state.callback), (None, None));
            assert!(state.available.unwrap() > unix_timestamp_ms(SystemTime::now()));
            if kind == 1 {
                f.make_available(id).await;
                assert_eq!(
                    ops.claim_earliest_published::<Echo>(18, expiration())
                        .await
                        .unwrap()
                        .task_id,
                    id
                );
                ops.finish::<Echo>(18, id, String::new(), None)
                    .await
                    .unwrap();
                assert!(f.state(id).await.is_none());
            }
        }
    }
    drop(listener);
    ops.close().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn id_bounds_and_codec_errors_preserve_sources_without_panics() {
    let f = Fixture::new().await;
    let ops = f.connect().await;
    let id = f.echo("bounds").await;
    for bad in [i64::MAX as u64 + 1, u64::MAX] {
        for (owner, task) in [(bad, id), (17, bad)] {
            assert!(
                matches!(ops.claim_published::<Echo>(owner, task, expiration()).await, Err(ClaimTaskError::Backend(error)) if error.source().unwrap().is::<TryFromIntError>())
            );
            assert!(
                matches!(ops.renew(owner, task, expiration()).await, Err(RenewTaskError::Backend(error)) if error.source().unwrap().is::<TryFromIntError>())
            );
            assert!(
                matches!(ops.fail(owner, task, None).await, Err(FailTaskError::Backend(error)) if error.source().unwrap().is::<TryFromIntError>())
            );
            assert!(
                matches!(ops.finish::<Echo>(owner, task, String::new(), None).await, Err(FinishTaskError::Backend(error)) if error.source().unwrap().is::<TryFromIntError>())
            );
        }
        assert!(matches!(
            ops.claim_earliest_published::<Echo>(bad, expiration())
                .await,
            Err(ClaimTaskError::Backend(_))
        ));
        assert!(matches!(
            ops.claim_singleton::<Singleton>(bad, expiration()).await,
            Err(ClaimTaskError::Backend(_))
        ));
    }
    assert_eq!(f.state(id).await.unwrap().owner, None);
    // Zero is representable (as on native SQLx), but no such published task exists.
    assert!(matches!(
        ops.claim_published::<Echo>(17, 0, expiration()).await,
        Err(ClaimTaskError::TaskNotFound)
    ));
    f.replace_id(id, i64::MAX).await;
    let max = i64::MAX as u64;
    ops.claim_published::<Echo>(max, max, expiration())
        .await
        .unwrap();
    ops.renew(max, max, expiration()).await.unwrap();
    ops.finish::<Echo>(max, max, String::new(), None)
        .await
        .unwrap();
    let negative = f.echo("negative").await;
    f.replace_id(negative, -1).await;
    assert!(
        matches!(ops.claim_earliest_published::<Echo>(17, expiration()).await, Err(ClaimTaskError::Backend(error)) if error.source().unwrap().is::<TryFromIntError>())
    );
    for (json, earliest) in [("not json", false), (r#"{"wrong":true}"#, true)] {
        let id = f.insert(Echo::NAME, json, None).await;
        let result = if earliest {
            ops.claim_earliest_published::<Echo>(17, expiration()).await
        } else {
            ops.claim_published::<Echo>(17, id, expiration()).await
        };
        assert!(
            matches!(result, Err(ClaimTaskError::Backend(error)) if error.source().unwrap().is::<serde_json::Error>())
        );
        assert_eq!(
            f.state(id).await.unwrap().owner,
            Some(17),
            "decoding follows the atomic claim"
        );
    }
    f.admin
        .execute_typed(
            &format!(
                "INSERT INTO {} (task_id, task_name, task_unique_key, payload_json) \
             OVERRIDING SYSTEM VALUE VALUES (-2, $1, $1, 'null')",
                f.table,
            ),
            &[(&Singleton::NAME, Type::TEXT)],
        )
        .await
        .unwrap();
    assert!(
        matches!(ops.claim_singleton::<Singleton>(17, expiration()).await,
        Err(ClaimTaskError::Backend(error)) if error.source().unwrap().is::<TryFromIntError>())
    );
    ops.close().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn isolated_schemas_and_listener_free_connections_do_not_initialize_or_set_search_path() {
    let a = Fixture::new().await;
    let b = Fixture::new().await;
    let ops_a = a.connect().await;
    let ops_b = b.connect().await;
    let id_a = a.echo("a").await;
    let id_b = b.echo("b").await;
    assert_eq!(id_a, id_b);
    assert_eq!(
        ops_a
            .claim_published::<Echo>(17, id_a, expiration())
            .await
            .unwrap()
            .task_payload
            .name,
        "a"
    );
    assert_eq!(b.state(id_b).await.unwrap().owner, None);
    assert_eq!(
        ops_b
            .claim_published::<Echo>(18, id_b, expiration())
            .await
            .unwrap()
            .task_payload
            .name,
        "b"
    );
    for ops in [&ops_a, &ops_b] {
        let client = ops.client().await.unwrap();
        assert_eq!(
            client
                .query_typed_one("SELECT current_setting('search_path')", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "\"$user\", public"
        );
        assert_eq!(
            client
                .query_typed_one("SELECT count(*) FROM pg_listening_channels()", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
        assert_eq!(
            client
                .query_typed_one("SELECT count(*) FROM pg_prepared_statements", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0,
            "typed queries must not retain named statements"
        );
    }
    let missing_schema = format!("{}_missing", a.schema);
    let missing = connect(
        &a.url(),
        PostgresBackendOptions {
            schema: Some(missing_schema.clone()),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        missing
            .claim_earliest_published::<Echo>(17, expiration())
            .await,
        Err(ClaimTaskError::Backend(_))
    ));
    assert!(
        matches!(missing.claim_published::<Echo>(17, 1, expiration()).await,
        Err(ClaimTaskError::Backend(error)) if error.source().unwrap().is::<tokio_postgres::Error>())
    );
    assert!(
        matches!(missing.claim_singleton::<Singleton>(17, expiration()).await,
        Err(ClaimTaskError::Backend(error)) if error.source().unwrap().is::<tokio_postgres::Error>())
    );
    assert!(matches!(missing.renew(17, 1, expiration()).await,
        Err(RenewTaskError::Backend(error)) if error.source().unwrap().is::<tokio_postgres::Error>()));
    assert!(matches!(missing.fail(17, 1, None).await,
        Err(FailTaskError::Backend(error)) if error.source().unwrap().is::<tokio_postgres::Error>()));
    assert!(
        matches!(missing.finish::<Echo>(17, 1, String::new(), None).await,
        Err(FinishTaskError::Backend(error)) if error.source().unwrap().is::<tokio_postgres::Error>())
    );
    assert!(
        !a.admin
            .query_typed_one(
                "SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = $1)",
                &[(&missing_schema, Type::TEXT)]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    missing.close().await.unwrap();
    ops_a.close().await.unwrap();
    ops_b.close().await.unwrap();
    a.cleanup().await;
    b.cleanup().await;
}

#[tokio::test]
async fn close_after_no_claim_or_query_failure_is_awaited_and_closes_all_clones() {
    let f = Fixture::new().await;
    for failure in [false, true] {
        let ops = f.connect().await;
        let clone = ops.clone();
        if failure {
            // A database constraint failure must not poison the connection's cleanup path.
            let id = f.echo("constraint").await;
            f.admin
                .batch_execute(&format!(
                    "ALTER TABLE {} ADD CONSTRAINT reject_owner CHECK (lease_worker_id IS NULL)",
                    f.table
                ))
                .await
                .unwrap();
            assert!(matches!(
                ops.claim_published::<Echo>(17, id, expiration()).await,
                Err(ClaimTaskError::Backend(_))
            ));
            assert_eq!(f.state(id).await.unwrap().owner, None);
        } else {
            assert!(matches!(
                ops.claim_published::<Echo>(17, 999, expiration()).await,
                Err(ClaimTaskError::TaskNotFound)
            ));
        }
        let (a, b) = timeout(LIMIT, async { tokio::join!(ops.close(), clone.close()) })
            .await
            .unwrap();
        a.unwrap();
        b.unwrap();
        assert!(
            matches!(clone.claim_published::<Echo>(17, 999, expiration()).await, Err(ClaimTaskError::Backend(error)) if matches!(error.downcast_ref(), Some(PostgresWorkerError::Closed)))
        );
        assert!(matches!(
            &*ops.shared.driver.lock().await,
            Driver::Closed(Ok(()))
        ));
    }
    f.cleanup().await;
}

#[tokio::test]
async fn close_waits_for_an_active_operation_and_finalization_commits_before_returning() {
    let f = Fixture::new().await;
    let ops = f.connect().await;
    let id = f.echo("blocked finish").await;
    ops.claim_published::<Echo>(17, id, expiration())
        .await
        .unwrap();
    f.admin
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
            f.table
        ))
        .await
        .unwrap();
    let copy = ops.clone();
    let attempt =
        tokio::spawn(async move { copy.finish::<Echo>(17, id, String::new(), None).await });
    f.wait_for_blocked_query().await;
    let mut close = Box::pin(ops.close());
    pending(close.as_mut()).await;
    assert!(!attempt.is_finished());
    f.admin.batch_execute("COMMIT").await.unwrap();
    timeout(LIMIT, attempt).await.unwrap().unwrap().unwrap();
    assert!(f.state(id).await.is_none());
    timeout(LIMIT, close).await.unwrap().unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn cancelled_finalization_and_serialization_failure_leave_the_row_owned() {
    let f = Fixture::new().await;
    let ops = f.connect().await;
    let id = f.echo("cancelled finish").await;
    ops.claim_published::<Echo>(17, id, expiration())
        .await
        .unwrap();
    f.callback(id).await;
    let before = f.state(id).await;
    assert!(matches!(
        ops.finish::<RejectedCallbackTask>(17, id, RejectedCallback, None).await,
        Err(FinishTaskError::Backend(error)) if error.source().unwrap().is::<serde_json::Error>()
    ));
    assert_eq!(f.state(id).await, before);

    f.admin
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
            f.table
        ))
        .await
        .unwrap();
    let copy = ops.clone();
    let attempt =
        tokio::spawn(async move { copy.finish::<Echo>(17, id, String::new(), None).await });
    f.wait_for_blocked_query().await;
    attempt.abort();
    assert!(attempt.await.unwrap_err().is_cancelled());
    f.admin.batch_execute("COMMIT").await.unwrap();
    // This round trip queues behind Transaction::drop's rollback, so an aborted DELETE cannot
    // leak into the next operation on the shared client or commit when it is closed.
    timeout(LIMIT, ops.renew(17, id, expiration()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(f.state(id).await.unwrap().callback, Some(f.callback_id));
    ops.fail(17, id, None).await.unwrap();
    let state = f.state(id).await.unwrap();
    assert_eq!(
        (state.owner, state.available, state.callback),
        (None, None, Some(f.callback_id))
    );
    ops.close().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn cancelling_close_keeps_the_driver_handle_and_still_awaits_exit() {
    let f = Fixture::new().await;
    let (config, host, port) = connection_config(&f.url()).unwrap();
    let (client, connection) = config
        .connect_raw(
            TcpStream::connect((host.as_str(), port)).await.unwrap(),
            NoTls,
        )
        .await
        .unwrap();
    let (exited_tx, exited_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let ops = PostgresTaskOperations::from_connection(
        client,
        async move {
            let result = connection.await;
            exited_tx.send(()).unwrap();
            release_rx.await.unwrap();
            result
        },
        Arc::from(f.table.as_str()),
    );
    let mut close = Box::pin(ops.close());
    pending(close.as_mut()).await;
    timeout(LIMIT, exited_rx).await.unwrap().unwrap();
    drop(close);
    let mut resumed = Box::pin(ops.close());
    pending(resumed.as_mut()).await;
    assert!(matches!(
        ops.client().await,
        Err(PostgresWorkerError::Closed)
    ));
    release_tx.send(()).unwrap();
    timeout(LIMIT, resumed).await.unwrap().unwrap();
    ops.close().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
async fn connection_driver_errors_are_observed_and_repeated_close_returns_the_error() {
    let f = Fixture::new().await;
    let ops = f.connect().await;
    let pid: i32 = ops
        .client()
        .await
        .unwrap()
        .query_typed_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let killed: bool = f
        .admin
        .query_typed_one("SELECT pg_terminate_backend($1)", &[(&pid, Type::INT4)])
        .await
        .unwrap()
        .get(0);
    assert!(killed);
    assert!(matches!(
        ops.claim_earliest_published::<Echo>(17, expiration()).await,
        Err(ClaimTaskError::Backend(_))
    ));
    let first = timeout(LIMIT, ops.close()).await.unwrap().unwrap_err();
    let second = ops.clone().close().await.unwrap_err();
    match (&first, &second) {
        (PostgresWorkerError::Postgres(a), PostgresWorkerError::Postgres(b)) => {
            assert!(Arc::ptr_eq(a, b))
        }
        _ => panic!("the driver's typed error must be retained: {first:?}, {second:?}"),
    }
    assert!(first.source().unwrap().is::<tokio_postgres::Error>());
    assert!(matches!(
        ops.client().await,
        Err(PostgresWorkerError::Closed)
    ));
    f.cleanup().await;
}
