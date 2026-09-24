//! Cloudflare PostgreSQL publication, dispatch, processing, and read-only recovery.
//!
//! Keep one [`RetainedTaskDispatcher`] per SQLite-backed Durable Object, using the named object
//! `global`. Dispatch launches with in-memory tracking before checking the warming alarm; active same-ID duplicates
//! cannot change routing and remain suppressed through full response consumption and result persistence.
//! A pending ID can be explicitly redispatched immediately with a corrected name.
//! Generic dispatch accepts opaque IDs; the wasm `sdk::PostgresPublisher`, `sdk::PostgresProcessor`,
//! and `sdk::PostgresSweeper` delegates require canonical positive decimal IDs up to 9007199254740991.
//!
//! Bind a publisher to one published task and its dispatcher. Synchronous configuration runs once
//! per call, not at construction. It publishes once, retains an exact string receipt, validates
//! the ID, awaits listener-free backend shutdown, then awaits [`dispatch_task`]. Success confirms
//! in-memory acceptance, not durable tracking or completion. Typed errors retain the first stage/cause,
//! any known receipt, and any later close failure. Close/dispatch receipts permit trusted redispatch
//! with the original definition name without republishing; `task-id` receipts are unsupported by the processor.
//! Missing receipts do not prove rollback.
//! Applications own HTTP endpoints, business validation, and side-effect clients. Direct
//! `PostgresPublishingBackend` plus [`dispatch_task`] remains a caller-managed alternative.
//!
//! `publish_future` records availability but dispatches immediately so PostgreSQL supplies the hint.
//! Both dispatch hops require only `{ taskId, taskName }`, never payloads or scheduling metadata.
//! The processor selects a typed
//! registration by exact definition name; names must be non-empty and unique within the registry.
//! Unknown names return 404 without acquisition; the database claim still checks both ID and name.
//! The processor validates before calling your synchronous environment-to-config callback. For a
//! selected registration, it owns a fresh backend and awaits the runtime, application cleanup, and
//! backend shutdown. Applications still own business resources. Use `PostgresExecutionBackend` with
//! [`crate::run_task_once`] for lower-level integrations with caller-owned cleanup.
//!
//! Only a successful, fully consumed, matching-ID `nextAction: { type: "done" }` deletes tracking.
//! `retryAt` with absolute Unix `atMs` persists the hint and resets infrastructure backoff; HTTP 200
//! describes a known action, not business success. Invalid responses and transport/body failures
//! retry with exponential backoff from one to thirty seconds, in memory until a hint is persisted.
//! One alarm selects the earliest pending/60-second watchdog deadline or independent 30-second
//! heartbeat. Every due distinct ID launches without a Bellows concurrency cap, subject to platform
//! limits. The watchdog applies to scheduled attempts, not unsaved external dispatches.
//! Watchdog supersession ignores stale responses but does not guarantee business cancellation.
//! PostgreSQL remains the execution/lease authority; renewed leases can move hints later.
//! Durability starts when a scheduling hint is persisted. The wasm `sdk::PostgresSweeper` provides
//! read-only PostgreSQL rediscovery through the same dispatcher for earlier invocation gaps.
//! Applications must install their own scheduled entrypoint and Cron Trigger; its selected schema
//! must belong entirely to the target workload. Alarms and attempts are at-least-once, not exactly-once side effects.
//! Publication and dispatch are not atomic; there is no automatic republishing, callback delivery, or
//! application-transaction participation. Await delegate calls within requests; ordinary error
//! paths await shutdown, but future cancellation, abrupt termination, and wasm traps cannot guarantee it.
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

#[cfg(any(target_arch = "wasm32", test))]
mod processor;

#[cfg(any(target_arch = "wasm32", test))]
mod publisher;

#[cfg(any(target_arch = "wasm32", test))]
mod sweeper;

mod scheduler;
use scheduler::HEARTBEAT_INTERVAL_MS;
pub use scheduler::{
    DispatcherState, DispatcherStorage, DispatcherTask, ScheduleUpdate, SchedulerMetadata,
    TaskSchedule,
};

const DISPATCHER_NAME: &str = "global";
const DISPATCH_URL: &str = "https://dispatcher/dispatch";
const PROCESSOR_URL: &str = "https://processor/process";
const MAX_TASK_ID_LENGTH: usize = 200;
const MAX_ERROR_LENGTH: usize = 500;
const INVALID_TASK_ID: &str = "taskId must be a non-empty string no longer than 200 characters";
const INVALID_TASK_NAME: &str = "taskName must be a non-empty string";

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

fn deserialize_timestamp<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<i64, D::Error> {
    let value = <f64 as serde::Deserialize>::deserialize(deserializer)?;
    if !value.is_finite()
        || value.fract() != 0.0
        || !(0.0..=scheduler::MAX_DATE_MS as f64).contains(&value)
    {
        return Err(serde::de::Error::custom("invalid timestamp"));
    }
    Ok(value as i64)
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
enum NextAction {
    Done,
    RetryAt {
        #[serde(rename = "atMs", deserialize_with = "deserialize_timestamp")]
        at_ms: i64,
    },
}

async fn processor_action(
    response: Response<TextBody>,
    task_id: &str,
) -> Result<NextAction, BoxDispatchError> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Envelope {
        task_id: String,
        next_action: NextAction,
    }
    let status = response.status();
    let body = response.into_body().await?;
    if !status.is_success() {
        return Err(format!(
            "task processor returned HTTP {}: {}",
            status.as_u16(),
            truncate_text(&body, MAX_ERROR_LENGTH)
        )
        .into());
    }
    let envelope: Envelope = serde_json::from_str(&body)
        .map_err(|_| BoxDispatchError::from("invalid processor next action"))?;
    if envelope.task_id != task_id
        || matches!(envelope.next_action, NextAction::RetryAt { at_ms } if !scheduler::timestamp(at_ms))
    {
        return Err("invalid processor next action".into());
    }
    Ok(envelope.next_action)
}

fn validate_task_name(task_name: &str) -> Result<(), BoxDispatchError> {
    if task_name.is_empty() {
        Err(INVALID_TASK_NAME.into())
    } else {
        Ok(())
    }
}

/// The namespace operation needed by [`dispatch_task`].
pub trait DurableObjectNamespaceLike {
    type Stub: ProcessorFetcher;

    fn get_by_name(&self, name: &str) -> Result<Self::Stub, BoxDispatchError>;
}

/// Dispatches a definition's exact name and an opaque ID of 1–200 UTF-16 units to object `global`.
///
/// Consumes the full response and propagates errors, limiting HTTP error excerpts to 500 UTF-16 units.
/// Accepts `worker::ObjectNamespace` directly on wasm.
pub async fn dispatch_task(
    namespace: &impl DurableObjectNamespaceLike,
    task_name: &str,
    task_id: &str,
) -> Result<(), BoxDispatchError> {
    validate_task_id(task_id)?;
    validate_task_name(task_name)?;
    let dispatcher = namespace.get_by_name(DISPATCHER_NAME)?;
    consume_response(
        dispatcher
            .fetch(processor_request(DISPATCH_URL, task_name, task_id))
            .await?,
        "dispatcher",
    )
    .await
}

struct RetainedAttempt {
    task: Arc<DispatcherTask>,
    // Holding a handle alone does not execute a future: platform::spawn drives it separately.
    _handle: Option<platform::JoinHandle<()>>,
}

type InFlight = Arc<Mutex<HashMap<String, RetainedAttempt>>>;

/// In-memory dispatch and outcome-driven durable scheduling with unrestricted fan-out.
/// Dispatch only checks/repairs the warming alarm; it never persists task acceptance.
/// Uncertainty backs off in memory until a hint is saved. A sixty-second watchdog recovers
/// scheduled attempts. Superseding transport does not imply cancellation of business work.
pub struct RetainedTaskDispatcher<S, P> {
    core: Arc<DispatcherCore<S, P>>,
}

struct DispatcherCore<S, P> {
    storage: S,
    processor: Arc<P>,
    in_flight: InFlight,
    memory_retries: Mutex<HashMap<String, Arc<DispatcherTask>>>,
    bookkeeping: tokio::sync::Mutex<()>,
    now: fn() -> i64,
}

impl<S: DispatcherStorage, P: ProcessorFetcher> RetainedTaskDispatcher<S, P> {
    pub fn new(storage: S, processor: P) -> Self {
        Self::with_clock(storage, processor, unix_ms)
    }

    /// Constructs a delegate with an absolute Unix millisecond clock, for deterministic tests.
    pub fn with_clock(storage: S, processor: P, now: fn() -> i64) -> Self {
        Self {
            core: Arc::new(DispatcherCore {
                storage,
                processor: Arc::new(processor),
                in_flight: Arc::default(),
                memory_retries: Mutex::default(),
                bookkeeping: tokio::sync::Mutex::new(()),
                now,
            }),
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

    /// Persists every due transition and the shared alarm before launching any processor request.
    pub async fn alarm(&self) -> Result<(), BoxDispatchError> {
        let core = &self.core;
        let _lock = core.bookkeeping.lock().await;
        let now = (core.now)();
        let in_flight = core.in_flight.clone();
        let result = core
            .storage
            .transaction(Box::new(move |state| {
                state.validate(now)?;
                let metadata = state.metadata.as_mut().unwrap();
                if metadata.next_heartbeat_at_ms <= now {
                    metadata.next_heartbeat_at_ms = now + HEARTBEAT_INTERVAL_MS;
                }
                let due: Vec<_> = state
                    .tasks
                    .values()
                    .filter(|task| task.next_attempt_at_ms <= now)
                    .map(|task| task.task_id.clone())
                    .collect();
                let mut launches = Vec::new();
                let mut expired = Vec::new();
                for id in due {
                    let task = state.tasks.get_mut(&id).unwrap();
                    if in_flight
                        .lock()
                        .unwrap()
                        .get(&id)
                        .is_some_and(|active| active.task.attempt_id().is_none())
                    {
                        task.next_attempt_at_ms = now + scheduler::ATTEMPT_WATCHDOG_MS;
                        continue;
                    }
                    if let Some(attempt_id) = task.attempt_id() {
                        expired.push((id, attempt_id));
                        task.retry(now);
                    } else {
                        launches.push(state.start(&id, now)?);
                    }
                }
                Ok((launches, expired))
            }))
            .await;
        match result {
            Ok((launches, expired)) => {
                let mut active = core.in_flight.lock().unwrap();
                for (id, attempt_id) in expired {
                    if active
                        .get(&id)
                        .is_some_and(|attempt| attempt.task.attempt_id() == Some(attempt_id))
                        && let Some(attempt) = active.remove(&id)
                        && let Some(handle) = attempt._handle
                    {
                        handle.abort();
                    }
                }
                drop(active);
                for task in launches {
                    core.launch_processor(task);
                }
                Ok(())
            }
            Err(error) => {
                // Re-read durable state rather than replacing a concurrently established earlier alarm.
                let _ = core
                    .storage
                    .transaction(Box::new(move |state| {
                        state.validate(now)?;
                        let metadata = state.metadata.as_mut().unwrap();
                        metadata.next_heartbeat_at_ms =
                            metadata.next_heartbeat_at_ms.min(now + 1_000);
                        Ok(())
                    }))
                    .await;
                Err(error)
            }
        }
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
        let task_name = object
            .get("taskName")
            .and_then(Value::as_str)
            .ok_or(INVALID_TASK_NAME)?;
        validate_task_name(task_name)?;

        let core = &self.core;
        let now = (core.now)();
        let failures = core
            .memory_retries
            .lock()
            .unwrap()
            .get(task_id)
            .map_or(0, |task| task.infrastructure_failures);
        let duplicate = !core.launch_processor(DispatcherTask {
            task_id: task_id.to_owned(),
            task_name: task_name.to_owned(),
            next_attempt_at_ms: now,
            infrastructure_failures: failures,
            state: TaskSchedule::Pending,
        });
        // Launch before storage access. Serialize only the alarm check with other
        // bookkeeping so heartbeat repair cannot overwrite an earlier task alarm.
        let _lock = core.bookkeeping.lock().await;
        let deadline = (core.now)() + HEARTBEAT_INTERVAL_MS;
        if core
            .storage
            .get_alarm()
            .await?
            .is_none_or(|alarm| alarm > deadline)
        {
            core.storage.set_alarm(deadline).await?;
        }
        Ok(json_response(
            if duplicate {
                json!({ "duplicate": true, "ok": true, "taskId": task_id })
            } else {
                json!({ "ok": true, "taskId": task_id })
            },
            StatusCode::OK,
        ))
    }
}

impl<S: DispatcherStorage, P: ProcessorFetcher> DispatcherCore<S, P> {
    fn launch_processor(self: &Arc<Self>, task: DispatcherTask) -> bool {
        let task_id = task.task_id.clone();
        let task = Arc::new(task);
        let mut in_flight = self.in_flight.lock().unwrap();
        if in_flight.contains_key(&task_id) {
            return false;
        }
        self.memory_retries.lock().unwrap().remove(&task_id);
        // Reserve before spawning, including on a multithreaded native executor. Insertion of the
        // handle and cleanup use the same short-lived lock, so immediate completion cannot race it.
        in_flight.insert(
            task_id.clone(),
            RetainedAttempt {
                task: task.clone(),
                _handle: None,
            },
        );
        let mut cleanup = AttemptCleanup {
            core: self.clone(),
            task_id: task_id.clone(),
            task: task.clone(),
            observed: false,
        };
        let processor = self.processor.clone();
        let core = self.clone();
        let handle = platform::spawn(async move {
            let result = async {
                let response = processor
                    .fetch(processor_request(
                        PROCESSOR_URL,
                        &task.task_name,
                        &cleanup.task_id,
                    ))
                    .await?;
                processor_action(response, &cleanup.task_id).await
            }
            .await;
            if let Err(error) = &result {
                log_processor_failure(&cleanup.task_id, &error.to_string());
            }
            core.complete(task, result.ok()).await;
            cleanup.observed = true;
            // Cleanup also runs if a native fetch/body future panics or the executor cancels it.
            drop(cleanup);
        });
        in_flight.get_mut(&task_id).unwrap()._handle = Some(handle);
        true
    }

    async fn complete(self: &Arc<Self>, attempt: Arc<DispatcherTask>, action: Option<NextAction>) {
        let _lock = self.bookkeeping.lock().await;
        if !self
            .in_flight
            .lock()
            .unwrap()
            .get(&attempt.task_id)
            .is_some_and(|active| Arc::ptr_eq(&active.task, &attempt))
        {
            return;
        }
        let now = (self.now)();
        let task_id = attempt.task_id.clone();
        let task = attempt.clone();
        let result: Result<(), BoxDispatchError> = async {
            // Reconcile prior schedules only after a response, never during acceptance.
            if !matches!(action, Some(NextAction::RetryAt { .. }))
                && !self.storage.contains_task(&task_id).await?
            {
                if action.is_none() && attempt.attempt_id().is_none() {
                    self.retry_in_memory(&attempt);
                }
                return Ok(());
            }
            self.storage
                .transaction(Box::new(move |state| {
                    state.validate(now)?;
                    let id = &task.task_id;
                    if task.attempt_id().is_some()
                        && state.tasks.get(id).and_then(DispatcherTask::attempt_id)
                            != task.attempt_id()
                    {
                        return Ok(());
                    }
                    match action {
                        Some(NextAction::Done) => {
                            state.tasks.remove(id);
                        }
                        Some(NextAction::RetryAt { at_ms }) => {
                            state.tasks.insert(
                                id.clone(),
                                DispatcherTask {
                                    task_id: id.clone(),
                                    task_name: task.task_name.clone(),
                                    state: TaskSchedule::Pending,
                                    next_attempt_at_ms: at_ms,
                                    infrastructure_failures: 0,
                                },
                            );
                        }
                        None => {
                            if let Some(current) = state.tasks.get_mut(id) {
                                current.retry(now);
                            }
                        }
                    }
                    Ok(())
                }))
                .await
        }
        .await;
        if let Err(error) = result {
            log_processor_failure(&task_id, &error.to_string());
            self.retry_in_memory(&attempt);
        }
    }

    fn retry_in_memory(self: &Arc<Self>, task: &DispatcherTask) {
        let mut pending = task.clone();
        pending.retry((self.now)());
        let delay = (pending.next_attempt_at_ms - (self.now)()).max(0) as u64;
        let pending = Arc::new(pending);
        self.memory_retries
            .lock()
            .unwrap()
            .insert(pending.task_id.clone(), pending.clone());
        let core = self.clone();
        platform::spawn(async move {
            platform::sleep_until(
                crate::time::Instant::now() + std::time::Duration::from_millis(delay),
            )
            .await;
            let _lock = core.bookkeeping.lock().await;
            let current = core
                .memory_retries
                .lock()
                .unwrap()
                .get(&pending.task_id)
                .is_some_and(|task| Arc::ptr_eq(task, &pending));
            if current {
                core.launch_processor((*pending).clone());
            }
        });
    }
}

struct AttemptCleanup<S: DispatcherStorage, P: ProcessorFetcher> {
    core: Arc<DispatcherCore<S, P>>,
    task_id: String,
    task: Arc<DispatcherTask>,
    observed: bool,
}

impl<S: DispatcherStorage, P: ProcessorFetcher> Drop for AttemptCleanup<S, P> {
    fn drop(&mut self) {
        let mut in_flight = self.core.in_flight.lock().unwrap();
        if !in_flight
            .get(&self.task_id)
            .is_some_and(|attempt| Arc::ptr_eq(&attempt.task, &self.task))
        {
            return;
        }
        in_flight.remove(&self.task_id);
        drop(in_flight);
        if !self.observed {
            log_processor_failure(
                &self.task_id,
                "processor task exited without an observed response",
            );
            self.core.retry_in_memory(&self.task);
        }
    }
}

fn processor_request(url: &str, task_name: &str, task_id: &str) -> Request<String> {
    Request::post(url)
        .header(CONTENT_TYPE, "application/json")
        .body(json!({ "taskId": task_id, "taskName": task_name }).to_string())
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
