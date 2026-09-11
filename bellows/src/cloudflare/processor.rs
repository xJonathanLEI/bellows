use std::{collections::BTreeSet, pin::Pin};

use http::{Request, Response, StatusCode, header::CONTENT_TYPE};
use serde_json::{Value, json};

use super::{BoxDispatchError, INVALID_TASK_NAME, TextBody, json_response, validate_task_name};
use crate::{
    PublishActivationStrategy, PublishDispatchToken, TaskDefinition, TaskExecutionBackend, Worker,
    WorkerFactory, run_task_once,
};

pub(super) type Cleanup = Pin<Box<dyn Future<Output = Result<(), BoxDispatchError>>>>;

type Attempt = Pin<Box<dyn Future<Output = ()>>>;

// Erase the factory only after checking its own published definition and callback types.
pub(super) struct ProcessorTask<B> {
    name: &'static str,
    execute: Box<dyn FnOnce(B, u64, u64) -> Attempt>,
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
            execute: Box::new(move |backend, worker_id, task_id| {
                Box::pin(run_task_once(
                    backend,
                    factory,
                    worker_id,
                    PublishDispatchToken::Task(task_id),
                ))
            }),
        }
    }

    pub async fn run(self, backend: B, worker_id: u64, task_id: u64) {
        (self.execute)(backend, worker_id, task_id).await;
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
        task_id: u64,
    ) -> Result<(), BoxDispatchError> {
        task.run(backend, worker_id, task_id).await;
        Ok(())
    }

    fn log_failure(&self, task_id: &str, stage: &'static str) {
        #[cfg(not(target_arch = "wasm32"))]
        tracing::error!(task_id, stage, "task processing attempt failed");
        #[cfg(target_arch = "wasm32")]
        worker::console_error!("task processing attempt failed {} {}", task_id, stage);
    }
}

pub(super) async fn fetch<P: Processor>(
    processor: &P,
    request: Request<TextBody>,
) -> Response<String> {
    let (task_id, id, task_name) = match validate(request).await {
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
            processor.log_failure(&task_id, "configuration");
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
        let Some(task) = tasks.into_iter().find(|task| task.name == task_name) else {
            return Ok(false);
        };
        let worker_id = random_worker_id(|| processor.random_bytes()).map_err(|_| "worker-id")?;
        let acquired = processor
            .acquire(settings)
            .await
            .map_err(|_| "acquisition")?;
        backend = Some(acquired.clone());
        processor
            .attempt(acquired, task, worker_id, id)
            .await
            .map_err(|_| "attempt")?;
        Ok(true)
    }
    .await;
    let mut failed = false;
    if let Err(stage) = attempt {
        failed = true;
        processor.log_failure(&task_id, stage);
    }
    // Collect each result: application failure must not skip Bellows shutdown.
    if let Some(cleanup) = cleanup
        && cleanup.await.is_err()
    {
        failed = true;
        processor.log_failure(&task_id, "application-cleanup");
    }
    if let Some(backend) = backend
        && processor.close(backend).await.is_err()
    {
        failed = true;
        processor.log_failure(&task_id, "backend-close");
    }
    if failed {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "task processing attempt failed",
        )
    } else if attempt == Ok(false) {
        error(StatusCode::NOT_FOUND, "unknown task name")
    } else {
        json_response(
            json!({ "taskId": task_id, "attemptFinished": true }),
            StatusCode::OK,
        )
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
async fn validate(request: Request<TextBody>) -> Result<(String, u64, String), Response<String>> {
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
    let Some(task_id) = body
        .as_object()
        .and_then(|body| body.get("taskId"))
        .and_then(Value::as_str)
        .filter(|id| {
            (1..=16).contains(&id.len())
                && matches!(id.as_bytes()[0], b'1'..=b'9')
                && id.bytes().all(|byte| byte.is_ascii_digit())
        })
    else {
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
    let task_name = body
        .get("taskName")
        .and_then(Value::as_str)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, INVALID_TASK_NAME))?;
    validate_task_name(task_name).map_err(|_| error(StatusCode::BAD_REQUEST, INVALID_TASK_NAME))?;
    Ok((task_id.to_owned(), id, task_name.to_owned()))
}

fn error(status: StatusCode, message: &str) -> Response<String> {
    json_response(json!({ "error": message }), status)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
