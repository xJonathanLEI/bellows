#![cfg(target_arch = "wasm32")]

#[path = "../db.rs"]
mod db;
#[path = "../http.rs"]
mod http;
#[path = "../task.rs"]
mod task;

use bellows::{
    TaskDefinition,
    cloudflare::{RetainedTaskDispatcher, dispatch_task, sdk::Dispatcher},
};
use serde_json::json;
use task::{GreetingPayload, GreetingTask};
use tokio_postgres::types::Type;
use worker::*;

// ECMAScript String.trim whitespace, including BOM (Rust's str::trim differs).
fn blank(name: &str) -> bool {
    name.chars().all(|ch| {
        matches!(ch, '\u{0009}'..='\u{000d}' | ' ' | '\u{00a0}' | '\u{1680}'
            | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}'
            | '\u{205f}' | '\u{3000}' | '\u{feff}')
    })
}

#[event(fetch)]
pub async fn fetch(mut request: Request, env: Env, _ctx: Context) -> Result<Response> {
    let body = match http::body(&mut request, "/tasks").await {
        Ok(body) => body,
        Err(response) => return Ok(response),
    };
    let Some(name) = body
        .as_object()
        .and_then(|body| body.get("name"))
        .and_then(serde_json::Value::as_str)
        .filter(|name| !blank(name) && name.encode_utf16().count() <= 200)
    else {
        return http::error(
            400,
            "body must be an object with a non-blank name of at most 200 characters",
        );
    };

    let mut task_id = None;
    let result = async {
        let schema = http::schema(&env)?;
        let url = env
            .hyperdrive("HYPERDRIVE")
            .map_err(|_| "missing Hyperdrive")?
            .connection_string();
        let mut connection = db::Connection::connect(&url).await?;
        let payload = serde_json::to_string(&GreetingPayload { name: name.into() })
            .map_err(|_| "payload encoding failed");
        let publication = async {
            let payload = payload?;
            let row = connection
                .client()
                .query_typed_one(
                    &format!(
                        r#"INSERT INTO "{schema}".bellows_tasks
                    (task_name, task_unique_key, payload_json, callback_id,
                     lease_worker_id, available_from_unix_ms)
                    VALUES ($1, NULL, $2, NULL, NULL, NULL) RETURNING task_id::text"#
                    ),
                    &[(&GreetingTask::NAME, Type::TEXT), (&payload, Type::TEXT)],
                )
                .await
                .map_err(|_| "publication failed")?;
            task_id = Some(row.get::<_, String>(0));
            Ok::<_, &'static str>(())
        }
        .await;
        let closed = connection.close().await;
        publication?;
        closed?;
        // The standalone INSERT has committed and its driver has exited before dispatch.
        let namespace = env
            .durable_object("DISPATCHER")
            .map_err(|_| "missing dispatcher")?;
        dispatch_task(&namespace, task_id.as_deref().unwrap())
            .await
            .map_err(|_| "dispatch failed")
    }
    .await;
    if result.is_err() {
        return Ok(Response::from_json(&match task_id {
            None => json!({ "error": "task publication failed" }),
            Some(task_id) => json!({
                "error": "task published, but dispatch acceptance was not confirmed",
                "taskId": task_id
            }),
        })?
        .with_status(503));
    }
    let mut response = Response::ok(task_id.unwrap())?.with_status(202);
    response
        .headers_mut()
        .set("content-type", "text/plain; charset=utf-8")?;
    response.headers_mut().set("cache-control", "no-store")?;
    Ok(response)
}

#[durable_object]
pub struct TaskDispatcher {
    dispatcher: Dispatcher,
}

impl DurableObject for TaskDispatcher {
    fn new(state: State, env: Env) -> Self {
        Self {
            dispatcher: RetainedTaskDispatcher::from_bindings(
                state.storage(),
                env.service("PROCESSOR")
                    .expect("PROCESSOR service binding is required"),
            ),
        }
    }

    async fn fetch(&self, request: Request) -> Result<Response> {
        self.dispatcher.fetch_worker(request).await
    }

    async fn alarm(&self) -> Result<Response> {
        self.dispatcher.alarm_worker().await
    }
}
