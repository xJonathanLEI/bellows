#![cfg(target_arch = "wasm32")]

#[path = "../db.rs"]
mod db;
#[path = "../http.rs"]
mod http;
#[path = "../task.rs"]
mod task;

use bellows::{
    PublishDispatchToken, TaskFailure, TaskResult, TaskSuccess, Worker, WorkerFactory,
    backends::postgres_execution::{PostgresBackendOptions, PostgresExecutionBackend},
    run_task_once,
};
use serde_json::json;
use std::sync::Arc;
use task::{GreetingPayload, GreetingTask};
use tokio::sync::Mutex;
use tokio_postgres::types::Type;
use worker::*;

struct GreetingWorker {
    hyperdrive_url: String,
    schema: String,
    side_effect: Arc<Mutex<Option<db::Connection>>>,
}

impl Worker for GreetingWorker {
    type Task = GreetingTask;

    #[worker::send]
    async fn process(self, task_id: u64, payload: GreetingPayload) -> TaskResult<()> {
        let result = async {
            // This separate side-effect connection is opened ONLY after a successful claim.
            let mut side_effect = self.side_effect.lock().await;
            *side_effect = Some(db::Connection::connect(&self.hyperdrive_url).await?);
            let connection = side_effect.as_mut().unwrap();
            let task_id = i64::try_from(task_id).expect("the processor validates safe integer IDs");
            let effect = connection.client().execute_typed(
                &format!(
                    r#"INSERT INTO "{}".processed_tasks AS processed (task_id, name, execution_count)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (task_id) DO UPDATE
                    SET name = EXCLUDED.name, execution_count = processed.execution_count + 1"#,
                    self.schema
                ),
                &[(&task_id, Type::INT8), (&payload.name, Type::TEXT)],
            ).await.map_err(|_| "side effect failed");
            let closed = connection.close().await;
            effect?;
            closed
        }.await;
        result
            .map(|()| TaskSuccess::done(()))
            .map_err(|_| TaskFailure::retry_immediately())
    }
}

struct GreetingFactory {
    hyperdrive_url: String,
    schema: String,
    side_effect: Arc<Mutex<Option<db::Connection>>>,
}

impl WorkerFactory for GreetingFactory {
    type Worker = GreetingWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        GreetingWorker {
            hyperdrive_url: self.hyperdrive_url.clone(),
            schema: self.schema.clone(),
            side_effect: self.side_effect.clone(),
        }
    }
}

fn worker_id() -> Result<u64> {
    use wasm_bindgen::JsCast;
    // Web Crypto, without Node compatibility or a native randomness backend.
    let crypto = js_sys::Reflect::get(&js_sys::global(), &"crypto".into())?;
    let random: js_sys::Function =
        js_sys::Reflect::get(&crypto, &"getRandomValues".into())?.dyn_into()?;
    loop {
        let bytes = js_sys::Uint8Array::new_with_length(6);
        random.call1(&crypto, &bytes)?;
        let value = bytes
            .to_vec()
            .into_iter()
            .fold(0_u64, |acc, byte| (acc << 8) | u64::from(byte));
        if value != 0 {
            return Ok(value);
        }
    }
}

#[event(fetch)]
pub async fn fetch(mut request: Request, env: Env, _ctx: Context) -> Result<Response> {
    let body = match http::body(&mut request, "/process").await {
        Ok(body) => body,
        Err(response) => return Ok(response),
    };
    let Some(task_id) = body
        .as_object()
        .and_then(|body| body.get("taskId"))
        .and_then(serde_json::Value::as_str)
        .filter(|id| {
            (1..=16).contains(&id.len())
                && matches!(id.as_bytes()[0], b'1'..=b'9')
                && id.bytes().all(|byte| byte.is_ascii_digit())
        })
    else {
        return http::error(400, "taskId must be a canonical positive decimal string");
    };
    let id: u64 = task_id.parse().expect("16 decimal digits fit u64");
    if id > 9_007_199_254_740_991 {
        return http::error(
            400,
            "taskId must encode a positive safe integer canonically",
        );
    }
    let attempt = async {
        let schema = http::schema(&env)?;
        let url = env
            .hyperdrive("HYPERDRIVE")
            .map_err(|_| "missing Hyperdrive")?
            .connection_string();
        let worker_id = worker_id().map_err(|_| "worker ID generation failed")?;
        let backend = PostgresExecutionBackend::connect_with_options(
            &url,
            PostgresBackendOptions {
                schema: Some(schema.clone()),
            },
        )
        .await
        .map_err(|_| "execution connection failed")?;
        let side_effect = Arc::new(Mutex::new(None));
        run_task_once(
            backend.clone(),
            GreetingFactory {
                hyperdrive_url: url,
                schema,
                side_effect: side_effect.clone(),
            },
            worker_id,
            PublishDispatchToken::Task(id),
        )
        .await;
        // Renewal loss can abort processing during query I/O or close(). Keep request-scoped
        // ownership outside the spawned worker and await that driver's exit on these paths too.
        let side_closed = match side_effect.lock().await.as_mut() {
            Some(connection) => connection.close().await,
            None => Ok(()),
        };
        let execution_closed = backend
            .close()
            .await
            .map_err(|_| "execution shutdown failed");
        side_closed?;
        execution_closed
    }
    .await;
    if attempt.is_err() {
        return http::error(500, "task processing attempt failed");
    }
    // No claim and handled processing failure also end an attempt. The DB establishes success.
    Response::from_json(&json!({ "taskId": task_id, "attemptFinished": true }))
}
