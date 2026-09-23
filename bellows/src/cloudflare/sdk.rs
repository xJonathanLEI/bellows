//! Workers SDK adapters (wasm + `cloudflare` only).
//!
//! Store one [`Dispatcher`] per SQLite-backed Durable Object, created with [`Dispatcher::from_bindings`],
//! and forward handlers to [`Dispatcher::fetch_worker`] and [`Dispatcher::alarm_worker`].
//!
//! For a producer, bind [`PostgresPublisher`] to one published task and its dispatcher. Its
//! synchronous configuration runs once per call; construction does no I/O. Await
//! [`PostgresPublisher::publish`] or [`PostgresPublisher::publish_future`] inside your handler:
//! either publishes once, retains the exact string
//! ID, validates the processor's safe-positive range, awaits backend shutdown, then consumes the
//! complete dispatch response. Applications own authentication, routing, validation, and responses.
//! Success confirms acceptance, not completion. [`PostgresPublisherError`] retains partial success
//! and causes; do not flatten it into a Worker error before inspecting its receipt.
//!
//! For a processor Worker, delegate to [`PostgresProcessor::fetch_worker`]. Its synchronous
//! configuration callback maps request bindings to [`PostgresProcessorConfig`] with typed
//! [`PostgresProcessorTask`] registrations, after validation. Construction performs no I/O; the delegate
//! owns a fresh listener-free execution backend for each selected attempt. Names come from each
//! definition and must be unique; claims check both the dispatched ID and persisted definition name.
//!
//! Register owned application cleanup with [`PostgresProcessorConfig::with_cleanup`], retaining
//! business-connection ownership outside the spawned worker to survive lease-loss aborts. Cleanup
//! runs once whenever configuration returned, even for invalid registrations or unknown names.
//! Backend shutdown is always awaited afterwards if acquired, even when application cleanup fails.
//! HTTP 200 reports a known next action, not business success. Applications still own arbitrary
//! side-effect resources.
//!
//! Direct [`crate::backends::postgres_execution::PostgresExecutionBackend`] with
//! [`crate::run_task_once`] remains available for custom integrations with caller-owned cleanup.
//! Likewise, [`crate::backends::postgres_publishing::PostgresPublishingBackend`] plus
//! [`super::dispatch_task`] supports caller-managed publication. The publisher's `publish_future`
//! records availability and immediately dispatches the ID/name for processor-driven scheduling.
//! It adds no awaitable publication, callback delivery, or application cleanup hooks.
//! The named object `global` launches external dispatches in memory, then checks the warming alarm.
//! It sets the alarm only when missing or later than now plus thirty seconds, without task writes.
//! Active same-ID requests stay deduplicated through full body consumption and result persistence.
//! Only a matching `done` removes tracking; `retryAt` persists an absolute hint. Uncertain responses
//! back off from one to thirty seconds. One alarm selects the earliest pending/watchdog deadline or
//! independent 30-second heartbeat, launching every due ID without a Bellows concurrency limit.
//! The 60-second watchdog supersedes interrupted/hung transport and ignores stale results, without
//! guaranteeing business cancellation. PostgreSQL controls execution eligibility; another invocation
//! may observe an extended lease. Alarm and transport redelivery do not imply exactly-once work.
//! Durability starts when `retryAt` is persisted; earlier uncertainty retries only in memory.
//! The watchdog applies to scheduled attempts. There is no PostgreSQL discovery or Cron. Publication and dispatch
//! are not atomic; cancellation, termination, and wasm traps have no
//! async-finally guarantee. Initialize schemas administratively, not during requests.

use http::{Request, Response};
use worker::send::{SendFuture, SendWrapper};

use super::{
    BoxDispatchError, DispatcherState, DispatcherStorage, DispatcherTask,
    DurableObjectNamespaceLike, ProcessorFetcher, RetainedTaskDispatcher, ScheduleUpdate, TextBody,
};

mod postgres;
mod postgres_publisher;
pub use postgres::{PostgresProcessor, PostgresProcessorConfig, PostgresProcessorTask};
pub use postgres_publisher::{
    PostgresPublisher, PostgresPublisherConfig, PostgresPublisherError, PostgresPublisherReceipt,
    PostgresPublisherStage,
};

/// A Workers service binding adapted to [`ProcessorFetcher`].
pub struct Service(SendWrapper<worker::Fetcher>);

impl From<worker::Fetcher> for Service {
    fn from(value: worker::Fetcher) -> Self {
        Self(SendWrapper::new(value))
    }
}

/// SQLite-backed Durable Object records and shared alarm, committed atomically.
pub struct Storage(SendWrapper<std::rc::Rc<worker::Storage>>);

impl From<worker::Storage> for Storage {
    fn from(value: worker::Storage) -> Self {
        Self(SendWrapper::new(std::rc::Rc::new(value)))
    }
}

/// The delegate stored by a Rust Durable Object.
pub type Dispatcher = RetainedTaskDispatcher<Storage, Service>;

impl Dispatcher {
    pub fn from_bindings(storage: worker::Storage, processor: worker::Fetcher) -> Self {
        Self::new(storage.into(), processor.into())
    }

    /// Forwards an SDK request, validating route and content type before reading the body.
    pub async fn fetch_worker(&self, request: worker::Request) -> worker::Result<worker::Response> {
        outgoing_response(self.fetch(incoming_request(request)?).await)
    }

    /// Forwards an alarm and returns an empty response.
    pub async fn alarm_worker(&self) -> worker::Result<worker::Response> {
        self.alarm()
            .await
            .map_err(|error| worker::Error::RustError(error.to_string()))?;
        worker::Response::empty()
    }
}

impl DurableObjectNamespaceLike for worker::ObjectNamespace {
    type Stub = worker::Stub;

    fn get_by_name(&self, name: &str) -> Result<Self::Stub, BoxDispatchError> {
        self.get_by_name(name).map_err(sdk_error)
    }
}

impl ProcessorFetcher for worker::Stub {
    fn fetch(
        &self,
        request: Request<String>,
    ) -> impl Future<Output = Result<Response<TextBody>, BoxDispatchError>> + Send {
        SendFuture::new(async move {
            let response = self
                .fetch_with_request(outgoing_request(request)?)
                .await
                .map_err(sdk_error)?;
            incoming_response(response)
        })
    }
}

impl ProcessorFetcher for Service {
    fn fetch(
        &self,
        request: Request<String>,
    ) -> impl Future<Output = Result<Response<TextBody>, BoxDispatchError>> + Send {
        SendFuture::new(async move {
            let response = self
                .0
                .fetch_request(outgoing_request(request)?)
                .await
                .map_err(sdk_error)?;
            // Also works when an application's other dependencies enable the SDK's `http` feature.
            #[allow(clippy::useless_conversion)]
            let response = response.try_into().map_err(|error| -> BoxDispatchError {
                format!("Workers response conversion failed: {error}").into()
            })?;
            incoming_response(response)
        })
    }
}

impl DispatcherStorage for Storage {
    fn get_alarm(&self) -> impl Future<Output = Result<Option<i64>, BoxDispatchError>> + Send {
        SendFuture::new(async move { self.0.get_alarm().await.map_err(sdk_error) })
    }

    fn set_alarm(&self, at_ms: i64) -> impl Future<Output = Result<(), BoxDispatchError>> + Send {
        SendFuture::new(async move {
            let date =
                worker::js_sys::Date::new(&worker::wasm_bindgen::JsValue::from_f64(at_ms as f64));
            self.0
                .set_alarm(worker::ScheduledTime::new(date))
                .await
                .map_err(sdk_error)
        })
    }

    fn contains_task(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<bool, BoxDispatchError>> + Send {
        SendFuture::new(async move {
            let key = format!("task:{id}");
            let records = self
                .0
                .get_multiple(vec![key.as_str()])
                .await
                .map_err(sdk_error)?;
            Ok(records.has(&worker::wasm_bindgen::JsValue::from_str(&key)))
        })
    }

    fn transaction<T>(
        &self,
        update: ScheduleUpdate<T>,
    ) -> impl Future<Output = Result<T, BoxDispatchError>> + Send
    where
        T: Send + 'static,
    {
        SendFuture::new(async move {
            let storage = std::rc::Rc::clone(&self.0);
            let result = std::rc::Rc::new(std::cell::RefCell::new(None));
            let output = result.clone();
            self.0
                .transaction(move |_transaction| async move {
                    // Top-level operations on this same SQLite storage join the transaction,
                    // including set_alarm, which the SDK Transaction wrapper does not expose.
                    // Storage::get deserializes Option<T>, conflating stored null with absence.
                    // Membership must be checked separately so corrupt metadata cannot reset IDs.
                    let metadata = storage.get_multiple(vec!["scheduler"]).await?;
                    let key = worker::wasm_bindgen::JsValue::from_str("scheduler");
                    let mut state = DispatcherState {
                        alarm: storage.get_alarm().await?,
                        metadata: if metadata.has(&key) {
                            Some(stored_value(&metadata.get(&key))?)
                        } else {
                            None
                        },
                        ..Default::default()
                    };
                    let records = storage
                        .list_with_options(worker::ListOptions::new().prefix("task:"))
                        .await?;
                    for entry in records.entries() {
                        let entry = worker::js_sys::Array::from(&entry?);
                        let key = entry.get(0).as_string().ok_or_else(|| {
                            worker::Error::RustError("invalid dispatcher storage key".into())
                        })?;
                        let task: DispatcherTask = stored_value(&entry.get(1))?;
                        if key != format!("task:{}", task.task_id) {
                            return Err(worker::Error::RustError(
                                "invalid dispatcher storage key".into(),
                            ));
                        }
                        state.tasks.insert(task.task_id.clone(), task);
                    }
                    let previous = state.tasks.clone();
                    let value = update(&mut state)
                        .map_err(|error| worker::Error::RustError(error.to_string()))?;
                    for (id, task) in &state.tasks {
                        if previous.get(id) != Some(task) {
                            storage.put(&format!("task:{id}"), task).await?;
                        }
                    }
                    for id in previous.keys() {
                        if !state.tasks.contains_key(id) {
                            storage.delete(&format!("task:{id}")).await?;
                        }
                    }
                    storage.put("scheduler", &state.metadata).await?;
                    // An integer passed to workers-rs set_alarm is a relative OFFSET.
                    let date = worker::js_sys::Date::new(&worker::wasm_bindgen::JsValue::from_f64(
                        state.alarm_at_ms() as f64,
                    ));
                    storage.set_alarm(worker::ScheduledTime::new(date)).await?;
                    *output.borrow_mut() = Some(value);
                    Ok(())
                })
                .await
                .map_err(sdk_error)?;
            let value = result
                .borrow_mut()
                .take()
                .ok_or("dispatcher transaction returned no result")?;
            Ok(value)
        })
    }
}

fn stored_value<T: serde::de::DeserializeOwned>(
    value: &worker::wasm_bindgen::JsValue,
) -> worker::Result<T> {
    let json = worker::js_sys::JSON::stringify(value)?
        .as_string()
        .ok_or_else(|| worker::Error::RustError("invalid dispatcher storage value".into()))?;
    Ok(serde_json::from_str(&json)?)
}

fn incoming_request(mut request: worker::Request) -> worker::Result<Request<TextBody>> {
    let mut builder = Request::builder()
        .method(request.method().as_ref())
        .uri(request.url()?.as_str());
    for (name, value) in request.headers().entries() {
        builder = builder.header(name, value);
    }
    let body: TextBody = Box::pin(SendFuture::new(async move {
        request.text().await.map_err(sdk_error)
    }));
    builder
        .body(body)
        .map_err(|error| worker::Error::RustError(error.to_string()))
}

fn outgoing_response(response: Response<String>) -> worker::Result<worker::Response> {
    let headers = worker::Headers::new();
    for (name, value) in response.headers() {
        headers.set(
            name.as_str(),
            value.to_str().expect("response headers are ASCII"),
        )?;
    }
    let status = response.status().as_u16();
    Ok(worker::Response::ok(response.into_body())?
        .with_status(status)
        .with_headers(headers))
}

fn outgoing_request(request: Request<String>) -> Result<worker::Request, BoxDispatchError> {
    let headers = worker::Headers::new();
    for (name, value) in request.headers() {
        headers
            .set(name.as_str(), value.to_str()?)
            .map_err(sdk_error)?;
    }
    let mut init = worker::RequestInit::new();
    init.with_method(request.method().as_str().to_owned().into())
        .with_headers(headers);
    let url = request.uri().to_string();
    init.with_body(Some(request.into_body().into()));
    worker::Request::new_with_init(&url, &init).map_err(sdk_error)
}

fn incoming_response(response: worker::Response) -> Result<Response<TextBody>, BoxDispatchError> {
    let status = response.status_code();
    let body: TextBody = Box::pin(SendFuture::new(async move {
        // Use Fetch's text decoder (including UTF-8 replacement/BOM handling), rather than the
        // SDK Response::text's strict String::from_utf8. Read the entire stream before truncating.
        let response: worker::web_sys::Response = response.into();
        let text = response.text().map_err(|error| sdk_error(error.into()))?;
        let text = worker::wasm_bindgen_futures::JsFuture::from(text)
            .await
            .map_err(|error| sdk_error(error.into()))?;
        Ok(text
            .as_string()
            .expect("Response.text() resolves to a string"))
    }));
    Ok(Response::builder().status(status).body(body)?)
}

// Do not let JS-affine error values escape into the portable error/future contracts.
fn sdk_error(error: worker::Error) -> BoxDispatchError {
    error.to_string().into()
}
