//! Cancellation-safe shutdown of the same request connection helper used by the examples.

#[path = "../db.rs"]
mod db;

use futures_util::poll;
use serde_json::json;
use std::pin::pin;
use tokio_postgres::types::Type;
use worker::{Env, Response};

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
