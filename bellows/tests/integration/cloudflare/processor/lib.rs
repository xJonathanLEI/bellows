#![cfg(target_arch = "wasm32")]

#[path = "../db.rs"]
mod db;
#[path = "../task.rs"]
mod task;

use bellows::{
    TaskFailure, TaskResult, TaskSuccess, Worker, WorkerFactory,
    backends::postgres_execution::PostgresBackendOptions,
    cloudflare::sdk::{PostgresProcessor, PostgresProcessorConfig},
};
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
            // Only a successful claim opens this separate business connection.
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

#[event(fetch)]
pub async fn fetch(request: Request, env: Env, _ctx: Context) -> Result<Response> {
    PostgresProcessor::new(|env: &Env| {
        let url = env.hyperdrive("HYPERDRIVE")?.connection_string();
        let schema = env.var("BELLOWS_SCHEMA")?.to_string();
        let side_effect = Arc::new(Mutex::new(None));
        Ok(PostgresProcessorConfig::new(
            url.clone(),
            PostgresBackendOptions {
                schema: Some(schema.clone()),
            },
            GreetingFactory {
                hyperdrive_url: url,
                schema,
                side_effect: side_effect.clone(),
            },
        )
        .with_cleanup(async move {
            // Retain the driver outside the worker so cleanup survives lease-loss aborts.
            match side_effect.lock().await.as_mut() {
                Some(connection) => connection.close().await.map_err(Error::from),
                None => Ok(()),
            }
        }))
    })
    .fetch_worker(request, &env)
    .await
}
