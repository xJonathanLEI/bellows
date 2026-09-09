//! Direct publishing backend contracts and cancellation-safe example connection shutdown.

#[path = "../db.rs"]
mod db;

use futures_util::poll;
use serde_json::json;
use std::{pin::pin, time::Duration};
use tokio_postgres::types::Type;
use worker::{Env, Response};

use bellows::{
    PublishTrigger, TaskDefinition, TaskPublishingBackend,
    backends::postgres_publishing::{PostgresBackendOptions, PostgresPublishingBackend},
    time::Instant,
};

struct PublishingTask;
impl TaskDefinition for PublishingTask {
    const NAME: &str = "publishing_contract";
    type Callback = Vec<String>;
    type Trigger = PublishTrigger<(String, Vec<u32>)>;
}

struct UnitTask;
impl TaskDefinition for UnitTask {
    const NAME: &str = "publishing_contract_unit";
    type Callback = ();
    type Trigger = PublishTrigger<()>;
}

pub async fn publish(env: &Env, mode: &str) -> worker::Result<Response> {
    if !["immediate", "future", "gated", "cancelled"].contains(&mode) {
        return Response::error("not-found", 404);
    }
    let backend = PostgresPublishingBackend::connect_with_options(
        &env.hyperdrive("HYPERDRIVE")?.connection_string(),
        PostgresBackendOptions {
            schema: Some(env.var("BELLOWS_SCHEMA")?.to_string()),
        },
    )
    .await
    .map_err(|_| worker::Error::from("publishing backend connection failed"))?;
    let result = async {
        let payload = ("hello \"🦀\"\n".to_owned(), vec![1, 2, 3]);
        let before_ms = worker::Date::now().as_millis();
        let available = Instant::now() + Duration::from_secs(60);
        let task = match mode {
            "cancelled" => {
                {
                    let mut insert = pin!(backend.publish::<PublishingTask>(payload));
                    assert!(poll!(&mut insert).is_pending());
                    // Drop only the query consumer. Already-enqueued SQL can still commit.
                }
                {
                    let mut close = pin!(backend.close());
                    assert!(poll!(&mut close).is_pending());
                    // Cancelling close must retain driver ownership for the awaited close below.
                }
                return Ok(json!({ "cancelled": true, "closed": true }));
            }
            "gated" => {
                let mut insert = pin!(backend.publish::<PublishingTask>(payload));
                assert!(poll!(&mut insert).is_pending());
                {
                    let mut close = pin!(backend.close());
                    assert!(poll!(&mut close).is_pending());
                }
                insert.await?
            }
            "future" => {
                backend
                    .publish_future::<PublishingTask>(payload, available)
                    .await?
            }
            _ => backend.publish::<PublishingTask>(payload).await?,
        };
        let unit = match mode {
            "immediate" => Some(backend.publish::<UnitTask>(()).await?.task_id),
            "future" => Some(
                backend
                    .publish_future::<UnitTask>((), available)
                    .await?
                    .task_id,
            ),
            _ => None,
        };
        Ok::<_, bellows::backends::PublishTaskError>(json!({
            "taskId": task.task_id,
            "unitTaskId": unit,
            "beforeMs": before_ms,
            "afterMs": worker::Date::now().as_millis(),
            "closed": true
        }))
    }
    .await;
    // Close on success and error, including after cancellation of an active query or close future.
    backend
        .clone()
        .close()
        .await
        .map_err(|_| worker::Error::from("publishing backend shutdown failed"))?;
    backend
        .close()
        .await
        .map_err(|_| worker::Error::from("repeated publishing backend shutdown failed"))?;
    assert!(backend.publish::<UnitTask>(()).await.is_err());
    match result {
        Ok(body) => Response::from_json(&body),
        Err(_) => Response::from_json(&json!({ "failed": true, "closed": true }))
            .map(|response| response.with_status(500)),
    }
}

pub async fn run(env: &Env) -> worker::Result<Response> {
    let url = env.hyperdrive("HYPERDRIVE")?.connection_string();
    let schema = env.var("BELLOWS_SCHEMA")?.to_string();
    assert!(
        schema.starts_with("bellows_cf_")
            && schema
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    );
    let mut connection = db::Connection::connect(&url)
        .await
        .map_err(worker::Error::from)?;
    let sql = format!(
        r#"INSERT INTO "{schema}".processed_tasks (task_id, name, execution_count)
           VALUES ($1, $2, 1)"#
    );
    {
        let params: &[(&(dyn tokio_postgres::types::ToSql + Sync), Type)] = &[
            (&7_i64, Type::INT8),
            (&"cancelled query consumer", Type::TEXT),
        ];
        let query = connection.client().execute_typed(&sql, params);
        let mut query = pin!(query);
        // Enqueue real driver I/O, then drop its consumer exactly as aborting a Rust worker does.
        // PostgreSQL's external table gate, not a timing sleep, prevents completion.
        assert!(poll!(&mut query).is_pending());
    }
    {
        let mut close = pin!(connection.close());
        assert!(poll!(&mut close).is_pending());
        // Cancel close itself too; the borrowed driver handle must survive and be resumable.
    }
    connection.close().await.map_err(worker::Error::from)?;
    connection.close().await.map_err(worker::Error::from)?;
    Response::from_json(&json!({ "closed": true }))
}
