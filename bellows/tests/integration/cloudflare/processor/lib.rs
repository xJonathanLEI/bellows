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
use task::{
    FullNamePayload, FullNameTask, GreetingPayload, GreetingTask, SchedulingMode,
    SchedulingPayload, SchedulingTask,
};
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
    async fn record(&self, task_id: u64, name: String) -> std::result::Result<i32, TaskFailure> {
        let result = async {
            // Only a successful claim opens this separate business connection.
            let mut side_effect = self.side_effect.lock().await;
            *side_effect = Some(db::Connection::connect(&self.hyperdrive_url).await?);
            let connection = side_effect.as_mut().unwrap();
            let task_id = i64::try_from(task_id).expect("the processor validates safe integer IDs");
            let effect = connection.client().query_typed(
                &format!(
                    r#"INSERT INTO "{}".processed_tasks AS processed (task_id, name, execution_count)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (task_id) DO UPDATE
                    SET name = EXCLUDED.name, execution_count = processed.execution_count + 1
                    RETURNING execution_count"#,
                    self.schema
                ),
                &[(&task_id, Type::INT8), (&name, Type::TEXT)],
            ).await.map_err(|_| "side effect failed");
            let closed = connection.close().await;
            let rows = effect?;
            closed?;
            Ok::<_, &str>(rows[0].get::<_, i32>(0))
        }.await;
        result.map_err(|_| TaskFailure::retry_immediately())
    }
}

struct GreetingWorker(SideEffect);

impl Worker for GreetingWorker {
    type Task = GreetingTask;

    #[worker::send]
    async fn process(self, task_id: u64, payload: GreetingPayload) -> TaskResult<()> {
        self.0
            .record(task_id, payload.name)
            .await
            .map(|_| TaskSuccess::done(()))
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
            .map(|_| TaskSuccess::done(()))
    }
}

struct FullNameFactory(SideEffect);

impl WorkerFactory for FullNameFactory {
    type Worker = FullNameWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        FullNameWorker(self.0.clone())
    }
}

struct SchedulingWorker(SideEffect);

impl Worker for SchedulingWorker {
    type Task = SchedulingTask;

    #[worker::send]
    async fn process(self, task_id: u64, payload: SchedulingPayload) -> TaskResult<()> {
        if self.0.record(task_id, payload.name).await? != 1 {
            return Ok(TaskSuccess::done(()));
        }
        match payload.mode {
            SchedulingMode::Failure => Err(TaskFailure::retry_at(task::deadline(
                payload.available_from_ms,
            ))),
            SchedulingMode::Success => Ok(TaskSuccess::schedule_next_run(
                (),
                task::deadline(payload.available_from_ms),
            )),
            SchedulingMode::Immediate => Err(TaskFailure::retry_immediately()),
        }
    }
}

struct SchedulingFactory(SideEffect);

impl WorkerFactory for SchedulingFactory {
    type Worker = SchedulingWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        SchedulingWorker(self.0.clone())
    }
}

thread_local! {
    // Fixture-only response loss after real finalization and cleanup, not a production route.
    static LOST_RESPONSES: std::cell::RefCell<std::collections::HashSet<String>> = Default::default();
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LostResponseRequest {
    task_id: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessorResponse {
    task_id: String,
    next_action: NextAction,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum NextAction {
    Done,
    RetryAt {
        #[serde(rename = "atMs")]
        at_ms: u64,
    },
}

#[event(fetch)]
pub async fn fetch(mut request: Request, env: Env, _ctx: Context) -> Result<Response> {
    if request.path() == "/__test/lose-response" {
        let body: LostResponseRequest = request.json().await?;
        LOST_RESPONSES.with(|ids| ids.borrow_mut().insert(body.task_id));
        return Response::empty();
    }
    let mut response = PostgresProcessor::new(|env: &Env| {
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
                PostgresProcessorTask::new(FullNameFactory(effect.clone())),
                PostgresProcessorTask::new(SchedulingFactory(effect)),
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
    .await?;
    if response.status_code() != 200 || LOST_RESPONSES.with(|ids| ids.borrow().is_empty()) {
        return Ok(response);
    }
    let body: ProcessorResponse = response.json().await?;
    if matches!(body.next_action, NextAction::Done)
        && LOST_RESPONSES.with(|ids| ids.borrow_mut().remove(&body.task_id))
    {
        return Response::error("fixture response lost", 503);
    }
    Response::from_json(&body)
}
