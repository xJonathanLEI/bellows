#![cfg(all(feature = "cloudflare", not(target_arch = "wasm32")))]

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use bellows::cloudflare::{
    AlarmStorage, BoxDispatchError, DurableObjectNamespaceLike, ProcessorFetcher,
    RetainedTaskDispatcher, TextBody, dispatch_task,
};
use http::{Request, Response};
use serde_json::{Value, json};
use tokio::sync::oneshot;

type IoResult<T> = Result<T, BoxDispatchError>;
const NOW: i64 = 1_700_000_000_000;

fn now() -> i64 {
    NOW
}

#[derive(Default)]
struct AlarmState {
    alarm: Option<i64>,
    scheduled: Vec<i64>,
    get_error: Option<&'static str>,
    set_error: Option<&'static str>,
    get_gate: Option<oneshot::Receiver<()>>,
}

#[derive(Clone, Default)]
struct FakeAlarmStorage(Arc<Mutex<AlarmState>>);

impl AlarmStorage for FakeAlarmStorage {
    async fn get_alarm(&self) -> IoResult<Option<i64>> {
        let gate = self.0.lock().unwrap().get_gate.take();
        if let Some(gate) = gate {
            gate.await?;
        }
        let state = self.0.lock().unwrap();
        if let Some(error) = state.get_error {
            return Err(error.into());
        }
        Ok(state.alarm)
    }

    async fn set_alarm(&self, alarm_time: i64) -> IoResult<()> {
        let mut state = self.0.lock().unwrap();
        if let Some(error) = state.set_error {
            return Err(error.into());
        }
        state.alarm = Some(alarm_time);
        state.scheduled.push(alarm_time);
        Ok(())
    }
}

struct ProcessorCall {
    request: Request<String>,
    response: Option<oneshot::Sender<IoResult<Response<TextBody>>>>,
}

#[derive(Clone, Default)]
struct DeferredProcessor(Arc<Mutex<Vec<ProcessorCall>>>);

impl DeferredProcessor {
    fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    fn complete(&self, index: usize, response: IoResult<Response<TextBody>>) {
        let sender = self.0.lock().unwrap()[index].response.take().unwrap();
        assert!(sender.send(response).is_ok());
    }
}

impl ProcessorFetcher for DeferredProcessor {
    async fn fetch(&self, request: Request<String>) -> IoResult<Response<TextBody>> {
        let (sender, receiver) = oneshot::channel();
        self.0.lock().unwrap().push(ProcessorCall {
            request,
            response: Some(sender),
        });
        receiver.await?
    }
}

#[derive(Default)]
struct StubState {
    requests: Vec<Request<String>>,
    response: Option<IoResult<Response<TextBody>>>,
}

#[derive(Clone, Default)]
struct RecordingStub(Arc<Mutex<StubState>>);

impl ProcessorFetcher for RecordingStub {
    async fn fetch(&self, request: Request<String>) -> IoResult<Response<TextBody>> {
        let mut state = self.0.lock().unwrap();
        state.requests.push(request);
        state.response.take().unwrap()
    }
}

#[derive(Default)]
struct RecordingNamespace {
    stub: RecordingStub,
    names: Mutex<Vec<String>>,
    error: Option<&'static str>,
}

impl DurableObjectNamespaceLike for RecordingNamespace {
    type Stub = RecordingStub;

    fn get_by_name(&self, name: &str) -> IoResult<Self::Stub> {
        self.names.lock().unwrap().push(name.to_owned());
        if let Some(error) = self.error {
            return Err(error.into());
        }
        Ok(self.stub.clone())
    }
}

struct BodyControl {
    started: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    sender: oneshot::Sender<IoResult<String>>,
}

fn controlled_body() -> (TextBody, BodyControl) {
    let (sender, receiver) = oneshot::channel();
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let body_started = started.clone();
    let body_finished = finished.clone();
    let body = Box::pin(async move {
        body_started.store(true, Ordering::SeqCst);
        let result = receiver.await?;
        body_finished.store(true, Ordering::SeqCst);
        result
    });
    (
        body,
        BodyControl {
            started,
            finished,
            sender,
        },
    )
}

fn text_body(text: impl Into<String>) -> TextBody {
    let text = text.into();
    Box::pin(async { Ok(text) })
}

fn response(status: u16, body: TextBody) -> Response<TextBody> {
    Response::builder().status(status).body(body).unwrap()
}

fn dispatch_request(task_id: &str) -> Request<TextBody> {
    named_request("contract", task_id)
}

fn named_request(task_name: &str, task_id: &str) -> Request<TextBody> {
    Request::post("https://dispatcher/dispatch")
        .header("content-type", "application/json")
        .body(text_body(
            json!({ "taskId": task_id, "taskName": task_name }).to_string(),
        ))
        .unwrap()
}

fn assert_json(response: Response<String>, status: u16, body: Value) {
    assert_eq!(response.status(), status);
    assert_eq!(response.headers().len(), 3);
    assert_eq!(
        response.headers()["content-type"],
        "application/json; charset=utf-8"
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(
        serde_json::from_str::<Value>(response.body()).unwrap(),
        body
    );
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    for _ in 0..100 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition was not met");
}

async fn finish(processor: &DeferredProcessor, index: usize) {
    wait_for(|| processor.count() > index).await;
    let (body, control) = controlled_body();
    processor.complete(index, Ok(response(200, body)));
    control.sender.send(Ok("finished".into())).unwrap();
    wait_for(|| control.finished.load(Ordering::SeqCst)).await;
}

// A thread-local subscriber is sufficient: these deterministic tests use Tokio's current-thread
// executor. It records actual library diagnostics, not an injected replacement logger.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<HashMap<String, String>>>>);

struct Fields(HashMap<String, String>);

impl tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}

impl tracing::Subscriber for Logs {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::ERROR
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = Fields(HashMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

#[tokio::test]
async fn dispatch_targets_global_and_fully_consumes_the_response() {
    let namespace = Arc::new(RecordingNamespace::default());
    let (body, control) = controlled_body();
    namespace.stub.0.lock().unwrap().response = Some(Ok(response(200, body)));
    let dispatch = tokio::spawn({
        let namespace = namespace.clone();
        async move { dispatch_task(namespace.as_ref(), "contract", "123").await }
    });
    wait_for(|| control.started.load(Ordering::SeqCst)).await;
    assert!(!dispatch.is_finished());
    assert!(!control.finished.load(Ordering::SeqCst));
    assert_eq!(*namespace.names.lock().unwrap(), ["global"]);
    {
        let stub = namespace.stub.0.lock().unwrap();
        assert_eq!(stub.requests.len(), 1);
        let request = &stub.requests[0];
        assert_eq!(request.method(), "POST");
        assert_eq!(request.uri(), "https://dispatcher/dispatch");
        assert_eq!(request.headers()["content-type"], "application/json");
        assert_eq!(
            serde_json::from_str::<Value>(request.body()).unwrap(),
            json!({ "taskId": "123", "taskName": "contract" })
        );
    }
    control.sender.send(Ok("accepted".into())).unwrap();
    dispatch.await.unwrap().unwrap();
    assert!(control.finished.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dispatch_consumes_an_error_response_before_rejecting() {
    let namespace = Arc::new(RecordingNamespace::default());
    let (body, control) = controlled_body();
    namespace.stub.0.lock().unwrap().response = Some(Ok(response(503, body)));
    let dispatch = tokio::spawn({
        let namespace = namespace.clone();
        async move { dispatch_task(namespace.as_ref(), "contract", "123").await }
    });
    wait_for(|| control.started.load(Ordering::SeqCst)).await;
    assert!(!dispatch.is_finished());
    control
        .sender
        .send(Ok("dispatcher unavailable".into()))
        .unwrap();
    assert_eq!(
        dispatch.await.unwrap().unwrap_err().to_string(),
        "task dispatcher returned HTTP 503: dispatcher unavailable"
    );
    assert!(control.finished.load(Ordering::SeqCst));
}

#[tokio::test]
async fn retained_dispatch_accepts_early_and_suppresses_duplicates_through_body_consumption() {
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
    assert_json(
        dispatcher.fetch(dispatch_request("task-1")).await,
        200,
        json!({ "ok": true, "taskId": "task-1" }),
    );
    wait_for(|| processor.count() == 1).await;
    {
        let calls = processor.0.lock().unwrap();
        let request = &calls[0].request;
        assert_eq!(request.method(), "POST");
        assert_eq!(request.uri(), "https://processor/process");
        assert_eq!(request.headers()["content-type"], "application/json");
        assert_eq!(
            serde_json::from_str::<Value>(request.body()).unwrap(),
            json!({ "taskId": "task-1", "taskName": "contract" })
        );
    }
    let duplicate = json!({ "duplicate": true, "ok": true, "taskId": "task-1" });
    assert_json(
        dispatcher.fetch(named_request("other", "task-1")).await,
        200,
        duplicate.clone(),
    );
    let (body, control) = controlled_body();
    processor.complete(0, Ok(response(200, body)));
    wait_for(|| control.started.load(Ordering::SeqCst)).await;
    assert_json(
        dispatcher.fetch(dispatch_request("task-1")).await,
        200,
        duplicate,
    );
    assert_eq!(processor.count(), 1);
    control.sender.send(Ok("finished".into())).unwrap();
    wait_for(|| control.finished.load(Ordering::SeqCst)).await;

    // Successful responses release IDs too: a later dispatch is a new attempt.
    assert_json(
        dispatcher.fetch(dispatch_request("task-1")).await,
        200,
        json!({ "ok": true, "taskId": "task-1" }),
    );
    finish(&processor, 1).await;
}

#[tokio::test]
async fn different_task_ids_retain_concurrent_processor_requests() {
    let processor = DeferredProcessor::default();
    let dispatcher =
        RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
    let (first, second) = tokio::join!(
        dispatcher.fetch(dispatch_request("task-1")),
        dispatcher.fetch(named_request("other", "task-2")),
    );
    assert_json(first, 200, json!({ "ok": true, "taskId": "task-1" }));
    assert_json(second, 200, json!({ "ok": true, "taskId": "task-2" }));
    wait_for(|| processor.count() == 2).await;
    assert!(
        processor
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|call| call.request.uri() == "https://processor/process")
    );
    let bodies: Vec<Value> = processor
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|call| serde_json::from_str(call.request.body()).unwrap())
        .collect();
    assert!(bodies.contains(&json!({ "taskId": "task-1", "taskName": "contract" })));
    assert!(bodies.contains(&json!({ "taskId": "task-2", "taskName": "other" })));
    finish(&processor, 0).await;
    finish(&processor, 1).await;
}

#[tokio::test]
async fn dispatch_and_alarm_schedule_the_30_second_heartbeat_even_when_idle() {
    let idle_storage = FakeAlarmStorage::default();
    let idle =
        RetainedTaskDispatcher::with_clock(idle_storage.clone(), DeferredProcessor::default(), now);
    idle.alarm().await.unwrap();
    assert_eq!(idle_storage.0.lock().unwrap().scheduled, [NOW + 30_000]);

    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    dispatcher.fetch(dispatch_request("task-1")).await;
    assert_eq!(storage.0.lock().unwrap().scheduled, [NOW + 30_000]);
    dispatcher.alarm().await.unwrap();
    assert_eq!(
        storage.0.lock().unwrap().scheduled,
        [NOW + 30_000, NOW + 30_000]
    );
    finish(&processor, 0).await;
    dispatcher.alarm().await.unwrap();
    assert_eq!(storage.0.lock().unwrap().scheduled, [NOW + 30_000; 3]);
}

#[tokio::test]
async fn processor_http_failures_are_consumed_observed_and_release_the_id() {
    for (status, message) in [(500, "failed"), (404, r#"{"error":"unknown task name"}"#)] {
        let logs = Logs::default();
        let _subscriber = tracing::subscriber::set_default(logs.clone());
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        dispatcher.fetch(named_request("unknown", "task-1")).await;
        wait_for(|| processor.count() == 1).await;
        let (body, control) = controlled_body();
        processor.complete(0, Ok(response(status, body)));
        wait_for(|| control.started.load(Ordering::SeqCst)).await;
        assert!(logs.0.lock().unwrap().is_empty());
        assert_json(
            dispatcher.fetch(dispatch_request("task-1")).await,
            200,
            json!({ "duplicate": true, "ok": true, "taskId": "task-1" }),
        );
        control.sender.send(Ok(message.into())).unwrap();
        wait_for(|| logs.0.lock().unwrap().len() == 1).await;
        {
            let logs = logs.0.lock().unwrap();
            assert_eq!(logs[0]["message"], "task processor failed");
            assert_eq!(logs[0]["task_id"], "task-1");
            assert_eq!(
                logs[0]["error"],
                format!("task processor returned HTTP {status}: {message}")
            );
        }
        assert!(control.finished.load(Ordering::SeqCst));
        assert_json(
            dispatcher.fetch(dispatch_request("task-1")).await,
            200,
            json!({ "ok": true, "taskId": "task-1" }),
        );
        finish(&processor, 1).await;
    }
}

#[tokio::test]
async fn generic_ids_use_utf16_limits_and_round_trip_without_numeric_parsing() {
    for id in ["".into(), "a".repeat(201), format!("{}a", "😀".repeat(100))] {
        let namespace = RecordingNamespace::default();
        assert_eq!(
            dispatch_task(&namespace, "contract", &id)
                .await
                .unwrap_err()
                .to_string(),
            "taskId must be a non-empty string no longer than 200 characters"
        );
        assert!(namespace.names.lock().unwrap().is_empty());
    }
    for id in [
        "0".into(),
        " ".into(),
        "opaque/\"\\\n雪".into(),
        "a".repeat(200),
        "😀".repeat(100),
        format!("{}😀", "a".repeat(198)),
    ] {
        let namespace = RecordingNamespace::default();
        namespace.stub.0.lock().unwrap().response = Some(Ok(response(200, text_body("accepted"))));
        dispatch_task(&namespace, "contract", &id).await.unwrap();
        let encoded = namespace.stub.0.lock().unwrap().requests[0].body().clone();
        assert_eq!(
            serde_json::from_str::<Value>(&encoded).unwrap(),
            json!({ "taskId": id, "taskName": "contract" })
        );
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        // Like TypeScript, routing ignores the query and media-type matching is case-insensitive.
        let request = Request::post("https://dispatcher/dispatch?ignored=true")
            .header("content-type", "Application/JSON; charset=utf-8")
            .body(text_body(encoded))
            .unwrap();
        assert_json(
            dispatcher.fetch(request).await,
            200,
            json!({ "ok": true, "taskId": id }),
        );
        finish(&processor, 0).await;
    }
}

#[tokio::test]
async fn invalid_routes_and_inputs_do_not_launch_work_or_schedule_alarms() {
    let storage = FakeAlarmStorage::default();
    let processor = DeferredProcessor::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    for (method, path) in [
        ("GET", "/dispatch"),
        ("POST", "/dispatch/"),
        ("POST", "/process"),
    ] {
        let unread: TextBody = Box::pin(async { panic!("invalid route must not read the body") });
        let request = Request::builder()
            .method(method)
            .uri(path)
            .body(unread)
            .unwrap();
        assert_json(
            dispatcher.fetch(request).await,
            404,
            json!({ "error": "not-found", "ok": false }),
        );
    }
    for content_type in [None, Some("text/plain")] {
        let mut request = Request::post("/dispatch");
        if let Some(value) = content_type {
            request = request.header("content-type", value);
        }
        let unread: TextBody =
            Box::pin(async { panic!("invalid media type must not read the body") });
        assert_json(
            dispatcher.fetch(request.body(unread).unwrap()).await,
            400,
            json!({ "error": "request content-type must be application/json", "ok": false }),
        );
    }
    for body in [
        "null".into(),
        "[]".into(),
        "[{\"taskId\":\"id\"}]".into(),
        "true".into(),
        "1".into(),
        "\"id\"".into(),
        "{}".into(),
        "{\"taskId\":null}".into(),
        "{\"taskId\":1}".into(),
        "{\"taskId\":false}".into(),
        "{\"taskId\":[]}".into(),
        "{\"taskId\":{}}".into(),
        "{\"taskId\":\"\"}".into(),
        json!({ "taskId": "a".repeat(201) }).to_string(),
        json!({ "taskId": format!("{}a", "😀".repeat(100)) }).to_string(),
    ] {
        let parsed: Value = serde_json::from_str(&body).unwrap();
        let error = if parsed.is_object() {
            "taskId must be a non-empty string no longer than 200 characters"
        } else {
            "request body must be a JSON object"
        };
        let request = Request::post("/dispatch")
            .header("content-type", "application/json")
            .body(text_body(body))
            .unwrap();
        assert_json(
            dispatcher.fetch(request).await,
            400,
            json!({ "error": error, "ok": false }),
        );
    }
    let malformed = Request::post("/dispatch")
        .header("content-type", "application/json")
        .body(text_body("{"))
        .unwrap();
    let result = dispatcher.fetch(malformed).await;
    assert_eq!(result.status(), 400);
    let body: Value = serde_json::from_str(result.body()).unwrap();
    assert_eq!(body["ok"], false);
    assert!(!body["error"].as_str().unwrap().is_empty());
    assert_eq!(processor.count(), 0);
    assert!(storage.0.lock().unwrap().scheduled.is_empty());
}

#[tokio::test]
async fn invalid_names_reject_before_lookup_launch_or_alarm() {
    let namespace = RecordingNamespace::default();
    assert_eq!(
        dispatch_task(&namespace, "", "17")
            .await
            .unwrap_err()
            .to_string(),
        "taskName must be a non-empty string"
    );
    assert_eq!(
        dispatch_task(&namespace, "", "")
            .await
            .unwrap_err()
            .to_string(),
        "taskId must be a non-empty string no longer than 200 characters"
    );
    assert!(namespace.names.lock().unwrap().is_empty());
    let processor = DeferredProcessor::default();
    let storage = FakeAlarmStorage::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    let mut bodies = vec![json!({ "taskId": "17" })];
    bodies.extend(
        [
            json!(""),
            json!(null),
            json!(17),
            json!([]),
            json!({}),
            json!(false),
        ]
        .map(|name| json!({ "taskId": "17", "taskName": name })),
    );
    for body in bodies {
        let request = Request::post("/dispatch")
            .header("content-type", "application/json")
            .body(text_body(body.to_string()))
            .unwrap();
        assert_json(
            dispatcher.fetch(request).await,
            400,
            json!({
                "error": "taskName must be a non-empty string", "ok": false
            }),
        );
    }
    assert_eq!(processor.count(), 0);
    assert!(storage.0.lock().unwrap().scheduled.is_empty());
}

#[tokio::test]
async fn exact_names_round_trip_both_hops_without_forwarding_payloads() {
    for name in [
        "contract".to_owned(),
        " ".to_owned(),
        "Name/\"\\\n雪🦀".to_owned(),
        "x".repeat(1000),
    ] {
        let namespace = RecordingNamespace::default();
        namespace.stub.0.lock().unwrap().response = Some(Ok(response(200, text_body("accepted"))));
        dispatch_task(&namespace, &name, "opaque").await.unwrap();
        let body: Value =
            serde_json::from_str(namespace.stub.0.lock().unwrap().requests[0].body()).unwrap();
        assert_eq!(body, json!({ "taskId": "opaque", "taskName": name }));
        let mut incoming = body.clone();
        incoming["payload"] = json!({ "untrusted": true });
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        let request = Request::post("/dispatch")
            .header("content-type", "application/json")
            .body(text_body(incoming.to_string()))
            .unwrap();
        assert_json(
            dispatcher.fetch(request).await,
            200,
            json!({ "ok": true, "taskId": "opaque" }),
        );
        wait_for(|| processor.count() == 1).await;
        assert_eq!(
            serde_json::from_str::<Value>(processor.0.lock().unwrap()[0].request.body()).unwrap(),
            body
        );
        finish(&processor, 0).await;
    }
}

#[tokio::test]
async fn heartbeat_preserves_only_future_alarms_no_later_than_the_next_heartbeat() {
    for alarm in [
        None,
        Some(NOW - 1),
        Some(NOW),
        Some(NOW + 1),
        Some(NOW + 30_000),
        Some(NOW + 30_001),
    ] {
        let storage = FakeAlarmStorage::default();
        storage.0.lock().unwrap().alarm = alarm;
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
        let expected = if alarm.is_some_and(|time| time > NOW && time <= NOW + 30_000) {
            vec![]
        } else {
            vec![NOW + 30_000]
        };
        for duplicate in [false, true] {
            {
                let mut state = storage.0.lock().unwrap();
                state.alarm = alarm;
                state.scheduled.clear();
            }
            let result = dispatcher.fetch(dispatch_request("task-1")).await;
            let body: Value = serde_json::from_str(result.body()).unwrap();
            assert_eq!(
                body.get("duplicate"),
                duplicate.then_some(&Value::Bool(true))
            );
            assert_eq!(storage.0.lock().unwrap().scheduled, expected);
        }
        finish(&processor, 0).await;
    }
}

#[tokio::test]
async fn rejected_processor_fetches_and_body_reads_are_observed_and_release_the_id() {
    for status in [None, Some(200), Some(503)] {
        let logs = Logs::default();
        let _subscriber = tracing::subscriber::set_default(logs.clone());
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        dispatcher.fetch(dispatch_request("task-1")).await;
        wait_for(|| processor.count() == 1).await;
        if let Some(status) = status {
            let body: TextBody = Box::pin(async { Err("body read rejected".into()) });
            processor.complete(0, Ok(response(status, body)));
        } else {
            processor.complete(0, Err("fetch rejected".into()));
        }
        wait_for(|| logs.0.lock().unwrap().len() == 1).await;
        assert_eq!(
            logs.0.lock().unwrap()[0]["error"],
            if status.is_some() {
                "body read rejected"
            } else {
                "fetch rejected"
            }
        );
        assert_json(
            dispatcher.fetch(dispatch_request("task-1")).await,
            200,
            json!({ "ok": true, "taskId": "task-1" }),
        );
        finish(&processor, 1).await;
    }
}

#[tokio::test]
async fn dispatch_propagates_namespace_fetch_and_body_read_errors() {
    let namespace = RecordingNamespace {
        error: Some("namespace unavailable"),
        ..Default::default()
    };
    assert_eq!(
        dispatch_task(&namespace, "contract", "id")
            .await
            .unwrap_err()
            .to_string(),
        "namespace unavailable"
    );
    for status in [None, Some(200), Some(503)] {
        let namespace = RecordingNamespace::default();
        let error: BoxDispatchError = std::io::Error::other("transport or body failed").into();
        namespace.stub.0.lock().unwrap().response = Some(if let Some(status) = status {
            Ok(response(status, Box::pin(async { Err(error) })))
        } else {
            Err(error)
        });
        let error = dispatch_task(&namespace, "contract", "id")
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "transport or body failed");
        assert!(error.downcast_ref::<std::io::Error>().is_some());
    }
}

#[tokio::test]
async fn diagnostic_truncation_is_unicode_safe_and_never_limits_body_consumption() {
    for (text, expected) in [
        ("a".repeat(600), "a".repeat(500)),
        ("雪".repeat(600), "雪".repeat(500)),
        (format!("{}😀tail", "a".repeat(499)), "a".repeat(499)),
        (format!("{}tail", "😀".repeat(250)), "😀".repeat(250)),
    ] {
        let namespace = RecordingNamespace::default();
        let (body, control) = controlled_body();
        namespace.stub.0.lock().unwrap().response = Some(Ok(response(503, body)));
        control.sender.send(Ok(text)).unwrap();
        assert_eq!(
            dispatch_task(&namespace, "contract", "id")
                .await
                .unwrap_err()
                .to_string(),
            format!("task dispatcher returned HTTP 503: {expected}")
        );
        assert!(control.finished.load(Ordering::SeqCst));
    }
    let prefix = "task processor returned HTTP 500: ";
    for status in [None, Some(500)] {
        let logs = Logs::default();
        let _subscriber = tracing::subscriber::set_default(logs.clone());
        let processor = DeferredProcessor::default();
        let dispatcher =
            RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), processor.clone(), now);
        dispatcher.fetch(dispatch_request("id")).await;
        wait_for(|| processor.count() == 1).await;
        let prefix = if status.is_some() { prefix } else { "" };
        let text = format!("{}😀tail", "a".repeat(499 - prefix.len()));
        processor.complete(
            0,
            if let Some(status) = status {
                Ok(response(status, text_body(text)))
            } else {
                Err(text.into())
            },
        );
        wait_for(|| logs.0.lock().unwrap().len() == 1).await;
        assert_eq!(
            logs.0.lock().unwrap()[0]["error"],
            format!("{prefix}{}", "a".repeat(499 - prefix.len()))
        );
    }
}

#[tokio::test]
async fn request_read_errors_and_storage_errors_use_the_existing_error_envelope() {
    let processor = DeferredProcessor::default();
    let storage = FakeAlarmStorage::default();
    let dispatcher = RetainedTaskDispatcher::with_clock(storage.clone(), processor.clone(), now);
    let body: TextBody = Box::pin(async { Err(format!("{}😀tail", "a".repeat(499)).into()) });
    let request = Request::post("/dispatch")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    assert_json(
        dispatcher.fetch(request).await,
        400,
        json!({ "error": "a".repeat(499), "ok": false }),
    );
    assert_eq!(processor.count(), 0);
    assert!(storage.0.lock().unwrap().scheduled.is_empty());
    for get in [true, false] {
        {
            let mut state = storage.0.lock().unwrap();
            state.get_error = get.then_some("storage failed");
            state.set_error = (!get).then_some("storage failed");
        }
        assert_json(
            dispatcher.fetch(dispatch_request("id")).await,
            400,
            json!({ "error": "storage failed", "ok": false }),
        );
    }
    assert_eq!(
        dispatcher.alarm().await.unwrap_err().to_string(),
        "storage failed"
    );
    {
        let mut state = storage.0.lock().unwrap();
        state.set_error = None;
        state.get_error = None;
    }
    assert_json(
        dispatcher.fetch(dispatch_request("id")).await,
        200,
        json!({ "duplicate": true, "ok": true, "taskId": "id" }),
    );
    finish(&processor, 0).await;
}

#[tokio::test]
async fn pending_storage_io_does_not_hold_the_registry_lock_or_delay_processor_progress() {
    let storage = FakeAlarmStorage::default();
    let (release, gate) = oneshot::channel();
    storage.0.lock().unwrap().get_gate = Some(gate);
    let processor = DeferredProcessor::default();
    let dispatcher = Arc::new(RetainedTaskDispatcher::with_clock(
        storage,
        processor.clone(),
        now,
    ));
    let acceptance = tokio::spawn({
        let dispatcher = dispatcher.clone();
        async move { dispatcher.fetch(dispatch_request("id")).await }
    });
    wait_for(|| processor.count() == 1).await;
    assert!(!acceptance.is_finished());
    assert_json(
        dispatcher.fetch(dispatch_request("id")).await,
        200,
        json!({ "duplicate": true, "ok": true, "taskId": "id" }),
    );
    assert_json(
        dispatcher.fetch(dispatch_request("other")).await,
        200,
        json!({ "ok": true, "taskId": "other" }),
    );
    finish(&processor, 0).await;
    finish(&processor, 1).await;
    release.send(()).unwrap();
    assert_json(
        acceptance.await.unwrap(),
        200,
        json!({ "ok": true, "taskId": "id" }),
    );
}

// Native-only coverage of the unwind guard, rather than a claim that wasm traps are recoverable.
#[tokio::test]
async fn native_processor_panics_release_the_id_and_are_logged() {
    struct PanickingProcessor;
    impl ProcessorFetcher for PanickingProcessor {
        async fn fetch(&self, _: Request<String>) -> IoResult<Response<TextBody>> {
            panic!("test transport panic");
        }
    }
    let logs = Logs::default();
    let _subscriber = tracing::subscriber::set_default(logs.clone());
    let dispatcher =
        RetainedTaskDispatcher::with_clock(FakeAlarmStorage::default(), PanickingProcessor, now);
    for count in 1..=2 {
        assert_json(
            dispatcher.fetch(dispatch_request("id")).await,
            200,
            json!({ "ok": true, "taskId": "id" }),
        );
        wait_for(|| logs.0.lock().unwrap().len() == count).await;
        assert_eq!(
            logs.0.lock().unwrap()[count - 1]["error"],
            "processor task exited without an observed response"
        );
    }
}
