use std::{collections::BTreeSet, pin::Pin};

use http::{Request, Response, StatusCode, header::CONTENT_TYPE};
use serde_json::{Value, json};

use super::{BoxDispatchError, TaskIdentity, TextBody, json_response, validate_task_name};
use crate::{
    PublishActivationStrategy, PublishDispatchToken, SingletonTrigger, TaskAttemptOutcome,
    TaskDefinition, TaskExecutionBackend, Worker, WorkerFactory, run_task_once,
};

pub(super) type Cleanup = Pin<Box<dyn Future<Output = Result<(), BoxDispatchError>>>>;

type Attempt = Pin<Box<dyn Future<Output = TaskAttemptOutcome>>>;

// Erase the factory only after checking its definition, trigger, and callback types.
pub(super) struct ProcessorTask<B> {
    name: &'static str,
    kind: &'static str,
    execute: Box<dyn FnOnce(B, u64, Option<u64>) -> Attempt>,
}

impl<B: TaskExecutionBackend + 'static> ProcessorTask<B> {
    pub fn new<F>(factory: F) -> Self
    where
        F: WorkerFactory + 'static,
        <F::Worker as Worker>::Task: TaskDefinition<
            Trigger: PublishActivationStrategy<DispatchToken = PublishDispatchToken>,
        >,
    {
        Self {
            name: <F::Worker as Worker>::Task::NAME,
            kind: "published",
            execute: Box::new(move |backend, worker_id, task_id| {
                Box::pin(async move {
                    run_task_once(
                        backend,
                        factory,
                        worker_id,
                        PublishDispatchToken::Task(task_id.expect("validated published identity")),
                    )
                    .await
                })
            }),
        }
    }

    pub fn singleton<F>(factory: F) -> Self
    where
        F: WorkerFactory + 'static,
        <F::Worker as Worker>::Task: TaskDefinition<Trigger = SingletonTrigger>,
    {
        Self {
            name: <F::Worker as Worker>::Task::NAME,
            kind: "singleton",
            execute: Box::new(move |backend, worker_id, _| {
                Box::pin(run_task_once(backend, factory, worker_id, ()))
            }),
        }
    }

    pub async fn run(self, backend: B, worker_id: u64, task_id: Option<u64>) -> TaskAttemptOutcome {
        (self.execute)(backend, worker_id, task_id).await
    }
}

pub(super) struct Scope<S, B> {
    pub settings: S,
    pub tasks: Vec<ProcessorTask<B>>,
    pub cleanup: Option<Cleanup>,
}

// Private acquisition/closing seam: native contracts use the real runtime without PostgreSQL.
pub(super) trait Processor {
    type Settings;
    type Backend: TaskExecutionBackend + Clone + 'static;

    fn configure(&self) -> Result<Scope<Self::Settings, Self::Backend>, BoxDispatchError>;
    fn random_bytes(&self) -> Result<[u8; 6], BoxDispatchError>;
    async fn acquire(&self, settings: Self::Settings) -> Result<Self::Backend, BoxDispatchError>;
    async fn close(&self, backend: Self::Backend) -> Result<(), BoxDispatchError>;

    async fn attempt(
        &self,
        backend: Self::Backend,
        task: ProcessorTask<Self::Backend>,
        worker_id: u64,
        task_id: Option<u64>,
    ) -> Result<TaskAttemptOutcome, BoxDispatchError> {
        Ok(task.run(backend, worker_id, task_id).await)
    }

    fn log_failure(&self, kind: &str, stage: &'static str) {
        #[cfg(not(target_arch = "wasm32"))]
        tracing::error!(kind, stage, "task processing attempt failed");
        #[cfg(target_arch = "wasm32")]
        worker::console_error!("task processing attempt failed {} {}", kind, stage);
    }
}

pub(super) async fn fetch<P: Processor>(
    processor: &P,
    request: Request<TextBody>,
) -> Response<String> {
    let (identity, id) = match validate(request).await {
        Ok(id) => id,
        Err(response) => return response,
    };
    let Scope {
        settings,
        tasks,
        cleanup,
    } = match processor.configure() {
        Ok(scope) => scope,
        Err(_) => {
            processor.log_failure(identity.kind(), "configuration");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "task processing attempt failed",
            );
        }
    };

    let mut backend = None;
    let attempt = async {
        let mut names = BTreeSet::new();
        if tasks.is_empty()
            || tasks
                .iter()
                .any(|task| task.name.is_empty() || !names.insert(task.name))
        {
            return Err("configuration");
        }
        let Some(task) = tasks
            .into_iter()
            .find(|task| task.name == identity.name() && task.kind == identity.kind())
        else {
            return Ok(None);
        };
        let worker_id = random_worker_id(|| processor.random_bytes()).map_err(|_| "worker-id")?;
        let acquired = processor
            .acquire(settings)
            .await
            .map_err(|_| "acquisition")?;
        backend = Some(acquired.clone());
        let outcome = processor
            .attempt(acquired, task, worker_id, id)
            .await
            .map_err(|_| "attempt")?;
        let action = match outcome {
            TaskAttemptOutcome::Done => json!({ "type": "done" }),
            TaskAttemptOutcome::RetryAt { available_from } => {
                let at_ms = crate::time::deadlines::ClockSnapshot::now()
                    .to_wire_ms(available_from)
                    .ok_or("attempt")?;
                json!({ "type": "retryAt", "atMs": at_ms })
            }
            TaskAttemptOutcome::Retry => return Err("attempt"),
        };
        Ok(Some(action))
    }
    .await;
    let mut failed = false;
    if let Err(stage) = &attempt {
        failed = true;
        processor.log_failure(identity.kind(), stage);
    }
    // Collect each result: application failure must not skip Bellows shutdown.
    if let Some(cleanup) = cleanup
        && cleanup.await.is_err()
    {
        failed = true;
        processor.log_failure(identity.kind(), "application-cleanup");
    }
    if let Some(backend) = backend
        && processor.close(backend).await.is_err()
    {
        failed = true;
        processor.log_failure(identity.kind(), "backend-close");
    }
    if failed {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "task processing attempt failed",
        )
    } else if let Ok(Some(next_action)) = attempt {
        json_response(
            json!({ "task": identity, "nextAction": next_action }),
            StatusCode::OK,
        )
    } else {
        error(StatusCode::NOT_FOUND, "unknown task name")
    }
}

fn random_worker_id(
    mut random_bytes: impl FnMut() -> Result<[u8; 6], BoxDispatchError>,
) -> Result<u64, BoxDispatchError> {
    loop {
        let value = random_bytes()?
            .into_iter()
            .fold(0_u64, |value, byte| (value << 8) | u64::from(byte));
        if value != 0 {
            return Ok(value);
        }
    }
}

// Keep validation responses inline; this private result never crosses a task boundary.
#[allow(clippy::result_large_err)]
async fn validate(
    request: Request<TextBody>,
) -> Result<(TaskIdentity, Option<u64>), Response<String>> {
    if request.uri().path() != "/process" {
        return Err(error(StatusCode::NOT_FOUND, "not-found"));
    }
    if request.method() != http::Method::POST {
        let mut response = error(StatusCode::METHOD_NOT_ALLOWED, "method-not-allowed");
        response
            .headers_mut()
            .insert("allow", "POST".parse().unwrap());
        return Err(response);
    }
    if !request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("application/json"))
    {
        return Err(error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "content-type must be application/json",
        ));
    }
    let body: Value = match request.into_body().await {
        Ok(body) => serde_json::from_str(&body).map_err(|_| ()),
        Err(_) => Err(()),
    }
    .map_err(|()| error(StatusCode::BAD_REQUEST, "invalid JSON"))?;
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Envelope {
        task: TaskIdentity,
    }
    let identity = serde_json::from_value::<Envelope>(body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task identity"))?
        .task;
    validate_task_name(identity.name())
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task identity"))?;
    let TaskIdentity::Published { task_id, .. } = &identity else {
        return Ok((identity, None));
    };
    if !((1..=16).contains(&task_id.len())
        && matches!(task_id.as_bytes()[0], b'1'..=b'9')
        && task_id.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "taskId must be a canonical positive decimal string",
        ));
    };
    let id: u64 = task_id.parse().expect("16 decimal digits fit u64");
    if id > 9_007_199_254_740_991 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "taskId must encode a positive safe integer canonically",
        ));
    }
    Ok((identity, Some(id)))
}

fn error(status: StatusCode, message: &str) -> Response<String> {
    json_response(json!({ "error": message }), status)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
