//! Retained Cloudflare dispatch using the TypeScript-compatible string-ID protocol.
//!
//! Keep one [`RetainedTaskDispatcher`] per Durable Object. It acknowledges early and suppresses
//! duplicates until the processor response is consumed. A response ends an attempt, not necessarily successfully.
//!
//! State is in-memory; the 30-second heartbeat provides no lease renewal, retry, or eviction recovery.
//! Publication gaps and rediscovery remain application concerns.
//!
//! Enable `cloudflare`, disabling defaults on wasm. Native use requires Tokio; wasm uses `sdk` adapters.
//! See the [Rust examples](https://github.com/xJonathanLEI/bellows/tree/master/bellows/tests/integration/cloudflare).

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
};

use http::{Request, Response, StatusCode, header::CONTENT_TYPE};
use serde_json::{Value, json};

use crate::{platform, time::clock};

#[cfg(target_arch = "wasm32")]
pub mod sdk;

const DISPATCHER_NAME: &str = "global";
const DISPATCH_URL: &str = "https://dispatcher/dispatch";
const PROCESSOR_URL: &str = "https://processor/process";
const MAX_TASK_ID_LENGTH: usize = 200;
const HEARTBEAT_INTERVAL_MS: i64 = 30_000;
const MAX_ERROR_LENGTH: usize = 500;
const INVALID_TASK_ID: &str = "taskId must be a non-empty string no longer than 200 characters";

/// An I/O or dispatch error. SDK adapters convert JS errors to Rust messages.
pub type BoxDispatchError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A lazy reader of the entire HTTP body, including error responses. Only diagnostics are truncated.
pub type TextBody =
    Pin<Box<dyn Future<Output = Result<String, BoxDispatchError>> + Send + 'static>>;

/// A service binding or Durable Object stub. Requests contain the complete wire envelope.
pub trait ProcessorFetcher: Send + Sync + 'static {
    fn fetch(
        &self,
        request: Request<String>,
    ) -> impl Future<Output = Result<Response<TextBody>, BoxDispatchError>> + Send;
}

/// The namespace operation needed by [`dispatch_task`].
pub trait DurableObjectNamespaceLike {
    type Stub: ProcessorFetcher;

    fn get_by_name(&self, name: &str) -> Result<Self::Stub, BoxDispatchError>;
}

/// Alarm storage with **absolute Unix millisecond timestamps**, not relative offsets.
pub trait AlarmStorage {
    fn get_alarm(&self) -> impl Future<Output = Result<Option<i64>, BoxDispatchError>> + Send;
    fn set_alarm(
        &self,
        alarm_time: i64,
    ) -> impl Future<Output = Result<(), BoxDispatchError>> + Send;
}

/// Dispatches an opaque ID of 1–200 UTF-16 code units to object `global`.
///
/// Consumes the full response and propagates errors, limiting HTTP error excerpts to 500 UTF-16 units.
/// Accepts `worker::ObjectNamespace` directly on wasm.
pub async fn dispatch_task(
    namespace: &impl DurableObjectNamespaceLike,
    task_id: &str,
) -> Result<(), BoxDispatchError> {
    validate_task_id(task_id)?;
    let dispatcher = namespace.get_by_name(DISPATCHER_NAME)?;
    consume_response(
        dispatcher
            .fetch(processor_request(DISPATCH_URL, task_id))
            .await?,
        "dispatcher",
    )
    .await
}

struct RetainedAttempt {
    token: Arc<()>,
    // Holding a handle alone does not execute a future: platform::spawn drives it separately.
    _handle: Option<platform::JoinHandle<()>>,
}

type InFlight = Arc<Mutex<HashMap<String, RetainedAttempt>>>;

/// An in-memory registry of concurrent processor requests, deduplicated through body consumption.
/// Native panics release IDs; wasm traps are not recoverable.
pub struct RetainedTaskDispatcher<S, P> {
    storage: S,
    processor: Arc<P>,
    in_flight: InFlight,
    now: fn() -> i64,
}

impl<S: AlarmStorage, P: ProcessorFetcher> RetainedTaskDispatcher<S, P> {
    pub fn new(storage: S, processor: P) -> Self {
        Self::with_clock(storage, processor, unix_ms)
    }

    /// Constructs a delegate with an absolute Unix millisecond clock, for deterministic tests.
    pub fn with_clock(storage: S, processor: P, now: fn() -> i64) -> Self {
        Self {
            storage,
            processor: Arc::new(processor),
            in_flight: Arc::default(),
            now,
        }
    }

    /// Handles POST `/dispatch`; returns 404 for other routes and 400 for validation/storage errors.
    pub async fn fetch(&self, request: Request<TextBody>) -> Response<String> {
        if request.method() != http::Method::POST || request.uri().path() != "/dispatch" {
            return json_response(
                json!({ "error": "not-found", "ok": false }),
                StatusCode::NOT_FOUND,
            );
        }

        match self.dispatch(request).await {
            Ok(response) => response,
            Err(error) => json_response(
                json!({ "error": truncate_text(&error.to_string(), MAX_ERROR_LENGTH), "ok": false }),
                StatusCode::BAD_REQUEST,
            ),
        }
    }

    /// Schedules a heartbeat, even when idle.
    pub async fn alarm(&self) -> Result<(), BoxDispatchError> {
        self.storage
            .set_alarm((self.now)() + HEARTBEAT_INTERVAL_MS)
            .await
    }

    async fn dispatch(
        &self,
        request: Request<TextBody>,
    ) -> Result<Response<String>, BoxDispatchError> {
        let content_type = request
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());
        if !content_type
            .unwrap_or_default()
            .to_ascii_lowercase()
            .contains("application/json")
        {
            return Err("request content-type must be application/json".into());
        }
        let body: Value = serde_json::from_str(&request.into_body().await?)?;
        let object = body
            .as_object()
            .ok_or("request body must be a JSON object")?;
        let task_id = object
            .get("taskId")
            .and_then(Value::as_str)
            .ok_or(INVALID_TASK_ID)?;
        validate_task_id(task_id)?;

        let duplicate = !self.launch_processor(task_id);
        self.schedule_heartbeat().await?;
        Ok(json_response(
            if duplicate {
                json!({ "duplicate": true, "ok": true, "taskId": task_id })
            } else {
                json!({ "ok": true, "taskId": task_id })
            },
            StatusCode::OK,
        ))
    }

    fn launch_processor(&self, task_id: &str) -> bool {
        let mut in_flight = self.in_flight.lock().unwrap();
        if in_flight.contains_key(task_id) {
            return false;
        }
        let token = Arc::new(());
        // Reserve before spawning, including on a multithreaded native executor. Insertion of the
        // handle and cleanup use the same short-lived lock, so immediate completion cannot race it.
        in_flight.insert(
            task_id.to_owned(),
            RetainedAttempt {
                token: token.clone(),
                _handle: None,
            },
        );
        let mut cleanup = AttemptCleanup {
            in_flight: self.in_flight.clone(),
            task_id: task_id.to_owned(),
            token,
            observed: false,
        };
        let processor = self.processor.clone();
        let handle = platform::spawn(async move {
            let result = async {
                let response = processor
                    .fetch(processor_request(PROCESSOR_URL, &cleanup.task_id))
                    .await?;
                consume_response(response, "processor").await
            }
            .await;
            if let Err(error) = result {
                log_processor_failure(&cleanup.task_id, &error.to_string());
            }
            cleanup.observed = true;
            // Cleanup also runs if a native fetch/body future panics or the executor cancels it.
            drop(cleanup);
        });
        in_flight.get_mut(task_id).unwrap()._handle = Some(handle);
        true
    }

    async fn schedule_heartbeat(&self) -> Result<(), BoxDispatchError> {
        let now = (self.now)();
        let next = now + HEARTBEAT_INTERVAL_MS;
        if self
            .storage
            .get_alarm()
            .await?
            .is_none_or(|current| current <= now || current > next)
        {
            self.storage.set_alarm(next).await?;
        }
        Ok(())
    }
}

struct AttemptCleanup {
    in_flight: InFlight,
    task_id: String,
    token: Arc<()>,
    observed: bool,
}

impl Drop for AttemptCleanup {
    fn drop(&mut self) {
        if !self.observed {
            log_processor_failure(
                &self.task_id,
                "processor task exited without an observed response",
            );
        }
        let mut in_flight = self.in_flight.lock().unwrap();
        if in_flight
            .get(&self.task_id)
            .is_some_and(|attempt| Arc::ptr_eq(&attempt.token, &self.token))
        {
            in_flight.remove(&self.task_id);
        }
    }
}

fn processor_request(url: &str, task_id: &str) -> Request<String> {
    Request::post(url)
        .header(CONTENT_TYPE, "application/json")
        .body(json!({ "taskId": task_id }).to_string())
        .expect("constant dispatch URL and headers are valid")
}

async fn consume_response(
    response: Response<TextBody>,
    role: &str,
) -> Result<(), BoxDispatchError> {
    let status = response.status();
    let body = response.into_body().await?;
    if !status.is_success() {
        return Err(format!(
            "task {role} returned HTTP {}: {}",
            status.as_u16(),
            truncate_text(&body, MAX_ERROR_LENGTH)
        )
        .into());
    }
    Ok(())
}

fn validate_task_id(task_id: &str) -> Result<(), BoxDispatchError> {
    if task_id.is_empty() || task_id.encode_utf16().count() > MAX_TASK_ID_LENGTH {
        Err(INVALID_TASK_ID.into())
    } else {
        Ok(())
    }
}

fn truncate_text(text: &str, limit: usize) -> &str {
    let mut units = 0;
    for (index, ch) in text.char_indices() {
        units += ch.len_utf16();
        if units > limit {
            return &text[..index];
        }
    }
    text
}

fn json_response(body: Value, status: StatusCode) -> Response<String> {
    Response::builder()
        .status(status)
        .header("cache-control", "no-store")
        .header(CONTENT_TYPE, "application/json; charset=utf-8")
        .header("x-content-type-options", "nosniff")
        .body(body.to_string())
        .expect("constant response headers are valid")
}

fn unix_ms() -> i64 {
    clock::SystemTime::now()
        .duration_since(clock::UNIX_EPOCH)
        .expect("current clock is after the Unix epoch")
        .as_millis()
        .try_into()
        .expect("current timestamp fits i64")
}

fn log_processor_failure(task_id: &str, error: &str) {
    let error = truncate_text(error, MAX_ERROR_LENGTH);
    #[cfg(not(target_arch = "wasm32"))]
    tracing::error!(task_id, error, "task processor failed");
    #[cfg(target_arch = "wasm32")]
    worker::console_error!("task processor failed {} {}", task_id, error);
}
