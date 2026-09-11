#![cfg(target_arch = "wasm32")]

#[path = "../db.rs"]
mod db;
#[path = "../task.rs"]
mod task;

use bellows::{
    TaskFailure, TaskResult, TaskSuccess, Worker, WorkerFactory,
    backends::postgres_execution::PostgresBackendOptions,
    cloudflare::sdk::{PostgresProcessor, PostgresProcessorConfig, PostgresProcessorTask},
};
use std::sync::Arc;
use task::{FullNamePayload, FullNameTask, GreetingPayload, GreetingTask};
use tokio::sync::Mutex;
use tokio_postgres::types::Type;
use worker::*;

#[derive(Clone)]
struct SideEffect {
    hyperdrive_url: String,
    schema: String,
    side_effect: Arc<Mutex<Option<db::Connection>>>,
}

impl SideEffect {
    async fn record(&self, task_id: u64, name: String) -> TaskResult<()> {
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
                &[(&task_id, Type::INT8), (&name, Type::TEXT)],
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

struct GreetingWorker(SideEffect);

impl Worker for GreetingWorker {
    type Task = GreetingTask;

    #[worker::send]
    async fn process(self, task_id: u64, payload: GreetingPayload) -> TaskResult<()> {
        self.0.record(task_id, payload.name).await
    }
}

struct GreetingFactory(SideEffect);

impl WorkerFactory for GreetingFactory {
    type Worker = GreetingWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        GreetingWorker(self.0.clone())
    }
}

struct FullNameWorker(SideEffect);

impl Worker for FullNameWorker {
    type Task = FullNameTask;

    #[worker::send]
    async fn process(self, task_id: u64, payload: FullNamePayload) -> TaskResult<()> {
        self.0
            .record(
                task_id,
                format!("{} {}", payload.first_name, payload.last_name),
            )
            .await
    }
}

struct FullNameFactory(SideEffect);

impl WorkerFactory for FullNameFactory {
    type Worker = FullNameWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        FullNameWorker(self.0.clone())
    }
}

#[event(fetch)]
pub async fn fetch(request: Request, env: Env, _ctx: Context) -> Result<Response> {
    PostgresProcessor::new(|env: &Env| {
        let url = env.hyperdrive("HYPERDRIVE")?.connection_string();
        let schema = env.var("BELLOWS_SCHEMA")?.to_string();
        let side_effect = Arc::new(Mutex::new(None));
        let effect = SideEffect {
            hyperdrive_url: url.clone(),
            schema: schema.clone(),
            side_effect: side_effect.clone(),
        };
        Ok(PostgresProcessorConfig::new(
            url,
            PostgresBackendOptions {
                schema: Some(schema),
            },
            vec![
                PostgresProcessorTask::new(GreetingFactory(effect.clone())),
                PostgresProcessorTask::new(FullNameFactory(effect)),
            ],
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
