use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use super::*;
use crate::{
    PublishTrigger, TaskFailure, TaskResult, TaskSuccess,
    backends::{
        ClaimTaskError, ClaimedTask, FailTaskError, FailedTask, FinishTaskError, FinishedTask,
        RenewTaskError, RenewedTaskLease,
    },
    time::Instant,
};

const SECRET: &str = "postgres://user:secret@private/database";
const CANONICAL: &str = "taskId must be a canonical positive decimal string";
const SAFE: &str = "taskId must encode a positive safe integer canonically";

#[derive(Serialize, Deserialize)]
struct Payload {
    name: String,
}

#[tokio::test]
async fn application_cleanup_is_optional() {
    let state = Arc::new(State::default());
    let mut harness = Harness::new(vec![state.clone()]);
    harness.omit_cleanup = true;
    envelope(
        fetch(
            &harness,
            request(json!({ "taskName": Task::NAME, "taskId": "17" })),
        )
        .await,
        200,
        json!({ "taskId": "17", "attemptFinished": true }),
    );
    assert_eq!(state.count("cleanup"), 0);
    assert_eq!(state.count("close"), 1);
    assert!(harness.logs.lock().unwrap().is_empty());
}

struct Task;

impl TaskDefinition for Task {
    const NAME: &str = "processor_contract";
    type Trigger = PublishTrigger<Payload>;
    type Callback = String;
}

#[derive(Serialize, Deserialize)]
struct CountPayload {
    count: u32,
}

struct CountTask;

impl TaskDefinition for CountTask {
    const NAME: &str = "Count/\"\\\n雪🦀";
    type Trigger = PublishTrigger<CountPayload>;
    type Callback = Vec<u32>;
}

struct EmptyNameTask;
struct EmptyNameFactory;

impl TaskDefinition for EmptyNameTask {
    const NAME: &str = "";
    type Trigger = PublishTrigger<()>;
    type Callback = ();
}

impl WorkerFactory for EmptyNameFactory {
    type Worker = Self;

    fn build(&self, _: u64) -> Self {
        panic!("invalid registrations must not build a worker")
    }
}

impl Worker for EmptyNameFactory {
    type Task = EmptyNameTask;

    async fn process(self, _: u64, _: ()) -> TaskResult<()> {
        panic!("invalid registrations must not execute")
    }
}

#[derive(Clone, Copy, Default)]
enum Claim {
    #[default]
    Found,
    Missing,
    Leased,
    Unavailable,
    Error,
}

#[derive(Clone, Copy, Default)]
enum Outcome {
    #[default]
    Success,
    Failure,
    Panic,
}

struct State {
    events: Mutex<Vec<&'static str>>,
    claims: Mutex<Vec<(String, u64, u64)>>,
    builds: Mutex<Vec<u64>>,
    payloads: Mutex<Vec<(u64, String)>>,
    callbacks: Mutex<Vec<Value>>,
    decoded: Mutex<Vec<&'static str>>,
    persisted_name: &'static str,
    persisted_payload: Value,
    claim: Claim,
    outcome: Outcome,
    finalization_error: bool,
    acquisition_error: bool,
    attempt_error: bool,
    cleanup_error: bool,
    close_error: bool,
    renewal_loss: bool,
    started: Semaphore,
    processing_gate: Option<Arc<Semaphore>>,
    recording: Semaphore,
    recording_gate: Option<Arc<Semaphore>>,
    renewing: Semaphore,
    renewal_gate: Semaphore,
    cleaning: Semaphore,
    cleanup_gate: Option<Arc<Semaphore>>,
    closing: Semaphore,
    close_gate: Option<Arc<Semaphore>>,
    // Mirrors application ownership outside the abortable worker.
    resource: tokio::sync::Mutex<Option<bool>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            events: Mutex::default(),
            claims: Mutex::default(),
            builds: Mutex::default(),
            payloads: Mutex::default(),
            callbacks: Mutex::default(),
            decoded: Mutex::default(),
            persisted_name: Task::NAME,
            persisted_payload: json!({ "name": "claimed" }),
            claim: Claim::Found,
            outcome: Outcome::Success,
            finalization_error: false,
            acquisition_error: false,
            attempt_error: false,
            cleanup_error: false,
            close_error: false,
            renewal_loss: false,
            started: Semaphore::new(0),
            processing_gate: None,
            recording: Semaphore::new(0),
            recording_gate: None,
            renewing: Semaphore::new(0),
            renewal_gate: Semaphore::new(0),
            cleaning: Semaphore::new(0),
            cleanup_gate: None,
            closing: Semaphore::new(0),
            close_gate: None,
            resource: tokio::sync::Mutex::new(None),
        }
    }
}

impl State {
    fn event(&self, name: &'static str) {
        self.events.lock().unwrap().push(name);
    }

    fn count(&self, name: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == name)
            .count()
    }

    async fn record(&self, name: &'static str) {
        self.event(name);
        self.recording.add_permits(1);
        if let Some(gate) = &self.recording_gate {
            wait(gate).await;
        }
    }
}

#[derive(Clone)]
struct Factory(Arc<State>);

struct BusinessWorker(Arc<State>);

impl WorkerFactory for Factory {
    type Worker = BusinessWorker;

    fn build(&self, worker_id: u64) -> BusinessWorker {
        self.0.event("build");
        self.0.builds.lock().unwrap().push(worker_id);
        BusinessWorker(self.0.clone())
    }
}

impl Worker for BusinessWorker {
    type Task = Task;

    async fn process(self, id: u64, payload: Payload) -> TaskResult<String> {
        let mut resource = self.0.resource.lock().await;
        *resource = Some(false);
        self.0.payloads.lock().unwrap().push((id, payload.name));
        self.0.started.add_permits(1);
        if let Some(gate) = &self.0.processing_gate {
            wait(gate).await;
        }
        *resource = Some(true);
        match self.0.outcome {
            Outcome::Success => Ok(TaskSuccess::done("claimed".to_owned())),
            Outcome::Failure => Err(TaskFailure::retry_immediately()),
            Outcome::Panic => panic!("injected business panic"),
        }
    }
}

impl Drop for BusinessWorker {
    fn drop(&mut self) {
        self.0.event("worker-dropped");
    }
}

struct CountFactory(Arc<State>);
struct CountWorker(Arc<State>);

impl WorkerFactory for CountFactory {
    type Worker = CountWorker;

    fn build(&self, worker_id: u64) -> CountWorker {
        self.0.event("count-build");
        self.0.builds.lock().unwrap().push(worker_id);
        CountWorker(self.0.clone())
    }
}

impl Worker for CountWorker {
    type Task = CountTask;

    async fn process(self, id: u64, payload: CountPayload) -> TaskResult<Vec<u32>> {
        self.0.event("count-process");
        self.0
            .payloads
            .lock()
            .unwrap()
            .push((id, payload.count.to_string()));
        Ok(TaskSuccess::done(vec![payload.count]))
    }
}

#[derive(Clone)]
struct Execution(Arc<State>);

impl TaskExecutionBackend for Execution {
    async fn claim_published<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<<T::Trigger as PublishActivationStrategy>::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.0
            .claims
            .lock()
            .unwrap()
            .push((T::NAME.to_owned(), worker_id, task_id));
        if T::NAME != self.0.persisted_name {
            return Err(ClaimTaskError::TaskNotFound);
        }
        match self.0.claim {
            Claim::Missing => return Err(ClaimTaskError::TaskNotFound),
            Claim::Leased => {
                return Err(ClaimTaskError::TaskLeased {
                    expiration: lease_expiration,
                });
            }
            Claim::Unavailable => {
                return Err(ClaimTaskError::TaskUnavailable {
                    available_from: None,
                });
            }
            Claim::Error => return Err(ClaimTaskError::Backend(SECRET.into())),
            Claim::Found => {}
        }
        self.0.decoded.lock().unwrap().push(T::NAME);
        Ok(ClaimedTask {
            task_id,
            task_payload: serde_json::from_value(self.0.persisted_payload.clone()).unwrap(),
            lease_expiration: if self.0.renewal_loss {
                Instant::now()
            } else {
                lease_expiration
            },
        })
    }

    async fn claim_earliest_published<T>(
        &self,
        _: u64,
        _: Instant,
    ) -> Result<ClaimedTask<<T::Trigger as PublishActivationStrategy>::Payload>, ClaimTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        panic!("must claim the explicit ID")
    }

    async fn claim_singleton<T>(
        &self,
        _: u64,
        _: Instant,
    ) -> Result<ClaimedTask<()>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        panic!("must claim a published task")
    }

    async fn renew(&self, _: u64, _: u64, _: Instant) -> Result<RenewedTaskLease, RenewTaskError> {
        self.0.renewing.add_permits(1);
        wait(&self.0.renewal_gate).await;
        Err(RenewTaskError::LeaseLost)
    }

    async fn fail(
        &self,
        _: u64,
        task_id: u64,
        _: Option<Instant>,
    ) -> Result<FailedTask, FailTaskError> {
        self.0.record("fail").await;
        if self.0.finalization_error {
            Err(FailTaskError::Backend(SECRET.into()))
        } else {
            Ok(FailedTask { task_id })
        }
    }

    async fn finish<T>(
        &self,
        _: u64,
        task_id: u64,
        callback: T::Callback,
        _: Option<Instant>,
    ) -> Result<FinishedTask, FinishTaskError>
    where
        T: TaskDefinition,
    {
        self.0
            .callbacks
            .lock()
            .unwrap()
            .push(serde_json::to_value(callback).unwrap());
        self.0.record("finish").await;
        if self.0.finalization_error {
            Err(FinishTaskError::Backend(SECRET.into()))
        } else {
            Ok(FinishedTask { task_id })
        }
    }
}

#[derive(Clone, Copy, Default)]
enum Registry {
    #[default]
    Both,
    One,
    Empty,
    EmptyName,
    Duplicate,
    UnrelatedDuplicate,
}

struct Harness {
    scopes: Mutex<VecDeque<Arc<State>>>,
    configured: AtomicUsize,
    random_calls: AtomicUsize,
    samples: Mutex<VecDeque<Result<[u8; 6], BoxDispatchError>>>,
    configuration_error: bool,
    omit_cleanup: bool,
    registry: Registry,
    logs: Mutex<Vec<(String, &'static str)>>,
}

impl Harness {
    fn new(scopes: Vec<Arc<State>>) -> Self {
        Self {
            scopes: Mutex::new(scopes.into()),
            configured: AtomicUsize::new(0),
            random_calls: AtomicUsize::new(0),
            samples: Mutex::new(VecDeque::from([Ok([0, 0, 0, 0, 0, 23])])),
            configuration_error: false,
            omit_cleanup: false,
            registry: Registry::Both,
            logs: Mutex::default(),
        }
    }
}

impl Processor for Harness {
    type Settings = Arc<State>;
    type Backend = Execution;

    fn configure(&self) -> Result<Scope<Arc<State>, Execution>, BoxDispatchError> {
        self.configured.fetch_add(1, Ordering::SeqCst);
        if self.configuration_error {
            return Err(SECRET.into());
        }
        let state = self
            .scopes
            .lock()
            .unwrap()
            .pop_front()
            .expect("one scope per request");
        state.event("configure");
        let cleanup = state.clone();
        let first = || ProcessorTask::new(Factory(state.clone()));
        let second = || ProcessorTask::new(CountFactory(state.clone()));
        let tasks = match self.registry {
            Registry::Both => vec![first(), second()],
            Registry::One => vec![first()],
            Registry::Empty => vec![],
            Registry::EmptyName => vec![first(), ProcessorTask::new(EmptyNameFactory)],
            Registry::Duplicate => vec![first(), first()],
            Registry::UnrelatedDuplicate => vec![first(), second(), second()],
        };
        let mut scope = Scope {
            settings: state.clone(),
            tasks,
            cleanup: Some(Box::pin(async move {
                cleanup.event("cleanup");
                // Lock acquisition waits for worker abort to release its application resource.
                let mut resource = cleanup.resource.lock().await;
                cleanup.cleaning.add_permits(1);
                if let Some(gate) = &cleanup.cleanup_gate {
                    wait(gate).await;
                }
                if let Some(closed) = resource.as_mut() {
                    *closed = true;
                }
                if cleanup.cleanup_error {
                    Err(SECRET.into())
                } else {
                    Ok(())
                }
            }) as Cleanup),
        };
        if self.omit_cleanup {
            scope.cleanup = None;
        }
        Ok(scope)
    }

    fn random_bytes(&self) -> Result<[u8; 6], BoxDispatchError> {
        self.random_calls.fetch_add(1, Ordering::SeqCst);
        self.samples
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected random sample")
    }

    async fn acquire(&self, state: Arc<State>) -> Result<Execution, BoxDispatchError> {
        state.event("acquire");
        if state.acquisition_error {
            Err(SECRET.into())
        } else {
            Ok(Execution(state))
        }
    }

    async fn attempt(
        &self,
        backend: Execution,
        task: ProcessorTask<Execution>,
        worker_id: u64,
        task_id: u64,
    ) -> Result<(), BoxDispatchError> {
        // The production runtime returns unit. This private seam covers an adapter-visible error.
        if backend.0.attempt_error {
            return Err(SECRET.into());
        }
        task.run(backend, worker_id, task_id).await;
        Ok(())
    }

    async fn close(&self, backend: Execution) -> Result<(), BoxDispatchError> {
        backend.0.event("close");
        backend.0.closing.add_permits(1);
        if let Some(gate) = &backend.0.close_gate {
            wait(gate).await;
        }
        if backend.0.close_error {
            Err(SECRET.into())
        } else {
            Ok(())
        }
    }

    fn log_failure(&self, task_id: &str, stage: &'static str) {
        self.logs.lock().unwrap().push((task_id.to_owned(), stage));
    }
}

fn request(body: Value) -> Request<TextBody> {
    raw_request(
        "/process",
        "POST",
        Some("application/json"),
        Ok(body.to_string()),
    )
}

fn raw_request(
    path: &str,
    method: &str,
    content_type: Option<&str>,
    body: Result<String, BoxDispatchError>,
) -> Request<TextBody> {
    let mut builder = Request::builder()
        .uri(format!("https://processor{path}"))
        .method(method);
    if let Some(content_type) = content_type {
        builder = builder.header(CONTENT_TYPE, content_type);
    }
    builder
        .body(Box::pin(async move { body }) as TextBody)
        .unwrap()
}

fn envelope(response: Response<String>, status: u16, body: Value) {
    assert_eq!(response.status().as_u16(), status);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_eq!(
        response.headers()[CONTENT_TYPE],
        "application/json; charset=utf-8"
    );
    assert_eq!(
        serde_json::from_str::<Value>(response.body()).unwrap(),
        body
    );
}

async fn wait(gate: &Semaphore) {
    tokio::time::timeout(Duration::from_secs(1), gate.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

fn gate() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(0))
}

fn untouched(harness: &Harness, state: &State) {
    assert_eq!(harness.configured.load(Ordering::SeqCst), 0);
    assert_eq!(harness.random_calls.load(Ordering::SeqCst), 0);
    assert!(state.events.lock().unwrap().is_empty());
    assert!(state.builds.lock().unwrap().is_empty());
    assert!(harness.logs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn routes_methods_and_media_types_reject_without_reading() {
    for (path, method, media_type, status, error) in [
        ("/other", "GET", None, 404, "not-found"),
        (
            "/process/",
            "POST",
            Some("application/json"),
            404,
            "not-found",
        ),
        ("/process", "GET", None, 405, "method-not-allowed"),
        (
            "/process",
            "PUT",
            Some("application/json"),
            405,
            "method-not-allowed",
        ),
        (
            "/process",
            "POST",
            None,
            415,
            "content-type must be application/json",
        ),
        (
            "/process",
            "POST",
            Some("text/plain"),
            415,
            "content-type must be application/json",
        ),
        (
            "/process",
            "POST",
            Some("application/problem+json"),
            415,
            "content-type must be application/json",
        ),
    ] {
        let state = Arc::new(State::default());
        let harness = Harness::new(vec![state.clone()]);
        let mut input = raw_request(path, method, media_type, Ok(String::new()));
        *input.body_mut() = Box::pin(async { panic!("body must remain unread") });
        let response = fetch(&harness, input).await;
        assert_eq!(
            response
                .headers()
                .get("allow")
                .map(|value| value.to_str().unwrap()),
            (status == 405).then_some("POST")
        );
        envelope(response, status, json!({ "error": error }));
        untouched(&harness, &state);
    }
}

#[tokio::test]
async fn malformed_and_unreadable_json_reject_before_configuration() {
    for body in [Ok("{".to_owned()), Err(SECRET.into())] {
        let state = Arc::new(State::default());
        let harness = Harness::new(vec![state.clone()]);
        envelope(
            fetch(
                &harness,
                raw_request("/process", "POST", Some("application/json"), body),
            )
            .await,
            400,
            json!({ "error": "invalid JSON" }),
        );
        untouched(&harness, &state);
    }
}

#[tokio::test]
async fn body_shape_and_canonical_id_validation_have_no_side_effects() {
    let mut bodies = vec![
        json!(null),
        json!([]),
        json!(true),
        json!(1),
        json!("17"),
        json!({}),
        json!({ "taskId": null }),
        json!({ "taskId": 17 }),
    ];
    bodies.extend(
        [
            "",
            "0",
            "-1",
            "+1",
            "01",
            " 1",
            "1 ",
            "1\n",
            "1.0",
            "1.5",
            "1e1",
            "１",
            "١",
            "90071992547409910",
            "10000000000000000",
        ]
        .map(|task_id| json!({ "taskId": task_id })),
    );
    for body in bodies {
        let state = Arc::new(State::default());
        let harness = Harness::new(vec![state.clone()]);
        envelope(
            fetch(&harness, request(body)).await,
            400,
            json!({ "error": CANONICAL }),
        );
        untouched(&harness, &state);
    }
}

#[tokio::test]
async fn unsafe_ids_reject_before_configuration() {
    for task_id in ["9007199254740992", "9999999999999999"] {
        let state = Arc::new(State::default());
        let harness = Harness::new(vec![state.clone()]);
        envelope(
            fetch(
                &harness,
                request(json!({ "taskName": Task::NAME, "taskId": task_id })),
            )
            .await,
            400,
            json!({ "error": SAFE }),
        );
        untouched(&harness, &state);
    }
}

#[tokio::test]
async fn boundary_ids_use_explicit_tokens_and_only_claimed_payloads() {
    for task_id in ["1", "9007199254740991"] {
        let state = Arc::new(State::default());
        let harness = Harness::new(vec![state.clone()]);
        *harness.samples.lock().unwrap() = VecDeque::from([Ok([0; 6]), Ok([255; 6])]);
        let mut input = request(
            json!({ "taskName": Task::NAME, "taskId": task_id, "payload": { "name": "untrusted" } }),
        );
        input.headers_mut().insert(
            CONTENT_TYPE,
            "Application/JSON; charset=utf-8".parse().unwrap(),
        );
        envelope(
            fetch(&harness, input).await,
            200,
            json!({ "taskId": task_id, "attemptFinished": true }),
        );
        let worker_id = (1_u64 << 48) - 1;
        let id = task_id.parse::<u64>().unwrap();
        assert_eq!(
            *state.claims.lock().unwrap(),
            [(Task::NAME.to_owned(), worker_id, id)]
        );
        assert_eq!(*state.builds.lock().unwrap(), [worker_id]);
        assert_eq!(
            *state.payloads.lock().unwrap(),
            [(id, "claimed".to_owned())]
        );
        assert_eq!(*state.callbacks.lock().unwrap(), [json!("claimed")]);
        assert_eq!(harness.configured.load(Ordering::SeqCst), 1);
        assert_eq!(harness.random_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            *state.events.lock().unwrap(),
            [
                "configure",
                "acquire",
                "build",
                "worker-dropped",
                "finish",
                "cleanup",
                "close"
            ]
        );
        assert!(harness.logs.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn invalid_names_reject_even_with_one_registration_before_configuration() {
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
        let state = Arc::new(State::default());
        let mut harness = Harness::new(vec![state.clone()]);
        harness.registry = Registry::One;
        envelope(
            fetch(&harness, request(body)).await,
            400,
            json!({ "error": INVALID_TASK_NAME }),
        );
        untouched(&harness, &state);
    }
}

#[tokio::test]
async fn registry_validation_and_unknown_names_cleanup_without_acquisition() {
    for (registry, name, invalid) in [
        (Registry::Empty, Task::NAME, true),
        (Registry::EmptyName, Task::NAME, true),
        (Registry::Duplicate, Task::NAME, true),
        (Registry::UnrelatedDuplicate, Task::NAME, true),
        (Registry::UnrelatedDuplicate, "unknown", true),
        (Registry::Both, "unknown", false),
        (Registry::Both, "PROCESSOR_CONTRACT", false),
        (Registry::Both, " processor_contract ", false),
        (Registry::Both, "__proto__", false),
    ] {
        for cleanup_error in [false, true] {
            let state = Arc::new(State {
                cleanup_error,
                ..State::default()
            });
            let mut harness = Harness::new(vec![state.clone()]);
            harness.registry = registry;
            let failed = invalid || cleanup_error;
            envelope(
                fetch(
                    &harness,
                    request(json!({ "taskId": "17", "taskName": name })),
                )
                .await,
                if failed { 500 } else { 404 },
                json!({ "error": if failed { "task processing attempt failed" } else { "unknown task name" } }),
            );
            assert_eq!(harness.configured.load(Ordering::SeqCst), 1);
            assert_eq!(harness.random_calls.load(Ordering::SeqCst), 0);
            assert_eq!(*state.events.lock().unwrap(), ["configure", "cleanup"]);
            assert!(state.claims.lock().unwrap().is_empty());
            assert!(state.builds.lock().unwrap().is_empty());
            let mut stages = vec![];
            if invalid {
                stages.push(("17".to_owned(), "configuration"));
            }
            if cleanup_error {
                stages.push(("17".to_owned(), "application-cleanup"));
            }
            assert_eq!(*harness.logs.lock().unwrap(), stages);
        }
    }
}

#[tokio::test]
async fn heterogeneous_routing_uses_each_definition_and_its_claimed_payload() {
    for (name, payload, recorded, callback, build) in [
        (
            Task::NAME,
            json!({ "name": "claimed" }),
            "claimed",
            json!("claimed"),
            "build",
        ),
        (
            CountTask::NAME,
            json!({ "count": 42 }),
            "42",
            json!([42]),
            "count-build",
        ),
    ] {
        let state = Arc::new(State {
            persisted_name: name,
            persisted_payload: payload,
            ..State::default()
        });
        let harness = Harness::new(vec![state.clone()]);
        envelope(
            fetch(&harness, request(json!({
                "taskId": "17", "taskName": name, "payload": { "name": "untrusted", "count": 99 }
            }))).await,
            200, json!({ "taskId": "17", "attemptFinished": true }),
        );
        assert_eq!(*state.claims.lock().unwrap(), [(name.to_owned(), 23, 17)]);
        assert_eq!(*state.decoded.lock().unwrap(), [name]);
        assert_eq!(*state.builds.lock().unwrap(), [23]);
        assert_eq!(state.count(build), 1);
        assert_eq!(
            state.count(if build == "build" {
                "count-build"
            } else {
                "build"
            }),
            0
        );
        assert_eq!(*state.payloads.lock().unwrap(), [(17, recorded.to_owned())]);
        assert_eq!(*state.callbacks.lock().unwrap(), [callback]);
        assert_eq!(state.count("finish"), 1);
        assert_eq!(state.count("cleanup"), 1);
        assert_eq!(state.count("close"), 1);
        assert!(harness.logs.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn registry_rejections_await_owned_cleanup_before_responding() {
    for invalid in [false, true] {
        let cleanup = gate();
        let state = Arc::new(State {
            cleanup_gate: Some(cleanup.clone()),
            ..State::default()
        });
        let mut harness = Harness::new(vec![state.clone()]);
        if invalid {
            harness.registry = Registry::Duplicate;
        }
        let response = fetch(
            &harness,
            request(json!({ "taskId": "17", "taskName": "unknown" })),
        );
        tokio::pin!(response);
        tokio::select! {
            _ = &mut response => panic!("registry rejection before application cleanup"),
            _ = wait(&state.cleaning) => {}
        }
        assert_eq!(harness.random_calls.load(Ordering::SeqCst), 0);
        assert_eq!(state.count("acquire"), 0);
        cleanup.add_permits(1);
        envelope(
            response.await,
            if invalid { 500 } else { 404 },
            json!({
                "error": if invalid { "task processing attempt failed" } else { "unknown task name" }
            }),
        );
        assert_eq!(state.count("cleanup"), 1);
        assert_eq!(state.count("close"), 0);
    }
}

#[tokio::test]
async fn persisted_name_mismatch_never_decodes_or_builds_either_worker() {
    let state = Arc::new(State {
        persisted_name: CountTask::NAME,
        persisted_payload: json!({ "count": 42 }),
        ..State::default()
    });
    let harness = Harness::new(vec![state.clone()]);
    envelope(
        fetch(
            &harness,
            request(json!({ "taskId": "17", "taskName": Task::NAME })),
        )
        .await,
        200,
        json!({ "taskId": "17", "attemptFinished": true }),
    );
    assert_eq!(
        *state.claims.lock().unwrap(),
        [(Task::NAME.to_owned(), 23, 17)]
    );
    assert!(state.decoded.lock().unwrap().is_empty());
    assert!(state.builds.lock().unwrap().is_empty());
    assert!(state.payloads.lock().unwrap().is_empty());
    assert!(state.callbacks.lock().unwrap().is_empty());
    assert_eq!(
        *state.events.lock().unwrap(),
        ["configure", "acquire", "cleanup", "close"]
    );
    assert!(harness.logs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn no_claim_and_swallowed_claim_errors_still_cleanup_normally() {
    for claim in [
        Claim::Missing,
        Claim::Leased,
        Claim::Unavailable,
        Claim::Error,
    ] {
        let state = Arc::new(State {
            claim,
            ..State::default()
        });
        let harness = Harness::new(vec![state.clone()]);
        envelope(
            fetch(
                &harness,
                request(json!({ "taskName": Task::NAME, "taskId": "17" })),
            )
            .await,
            200,
            json!({ "taskId": "17", "attemptFinished": true }),
        );
        assert_eq!(
            *state.events.lock().unwrap(),
            ["configure", "acquire", "cleanup", "close"]
        );
        assert!(state.builds.lock().unwrap().is_empty());
        assert!(harness.logs.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn attempt_finalization_cleanup_and_close_each_hold_the_response() {
    for outcome in [Outcome::Success, Outcome::Failure, Outcome::Panic] {
        let processing = gate();
        let recording = gate();
        let cleanup = gate();
        let close = gate();
        let state = Arc::new(State {
            outcome,
            processing_gate: Some(processing.clone()),
            recording_gate: Some(recording.clone()),
            cleanup_gate: Some(cleanup.clone()),
            close_gate: Some(close.clone()),
            ..State::default()
        });
        let harness = Harness::new(vec![state.clone()]);
        let response = fetch(
            &harness,
            request(json!({ "taskName": Task::NAME, "taskId": "17" })),
        );
        tokio::pin!(response);
        tokio::select! {
            _ = &mut response => panic!("response before worker completion"),
            _ = wait(&state.started) => {}
        }
        processing.add_permits(1);
        tokio::select! {
            _ = &mut response => panic!("response before finalization"),
            _ = wait(&state.recording) => {}
        }
        assert_eq!(state.count("cleanup"), 0);
        recording.add_permits(1);
        tokio::select! {
            _ = &mut response => panic!("response before application cleanup"),
            _ = wait(&state.cleaning) => {}
        }
        assert_eq!(state.count("close"), 0);
        cleanup.add_permits(1);
        tokio::select! {
            _ = &mut response => panic!("response before backend close"),
            _ = wait(&state.closing) => {}
        }
        close.add_permits(1);
        envelope(
            response.await,
            200,
            json!({ "taskId": "17", "attemptFinished": true }),
        );
        assert_eq!(
            state.count(if matches!(outcome, Outcome::Success) {
                "finish"
            } else {
                "fail"
            }),
            1
        );
        assert_eq!(state.count("cleanup"), 1);
        assert_eq!(state.count("close"), 1);
        assert!(harness.logs.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn swallowed_finalization_errors_keep_normal_attempt_semantics() {
    for outcome in [Outcome::Success, Outcome::Failure] {
        let state = Arc::new(State {
            outcome,
            finalization_error: true,
            ..State::default()
        });
        let harness = Harness::new(vec![state.clone()]);
        envelope(
            fetch(
                &harness,
                request(json!({ "taskName": Task::NAME, "taskId": "17" })),
            )
            .await,
            200,
            json!({ "taskId": "17", "attemptFinished": true }),
        );
        assert_eq!(state.count("cleanup"), 1);
        assert_eq!(state.count("close"), 1);
        assert!(harness.logs.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn infrastructure_failures_are_sanitized_and_cleanup_owned_resources() {
    for stage in [
        "configuration",
        "worker-id",
        "acquisition",
        "attempt",
        "application-cleanup",
        "backend-close",
    ] {
        let state = Arc::new(State {
            acquisition_error: stage == "acquisition",
            attempt_error: stage == "attempt",
            cleanup_error: stage == "application-cleanup",
            close_error: stage == "backend-close",
            ..State::default()
        });
        let mut harness = Harness::new(vec![state.clone()]);
        harness.configuration_error = stage == "configuration";
        if stage == "worker-id" {
            *harness.samples.lock().unwrap() = VecDeque::from([Err(SECRET.into())]);
        }
        envelope(
            fetch(
                &harness,
                request(json!({ "taskName": Task::NAME, "taskId": "17" })),
            )
            .await,
            500,
            json!({ "error": "task processing attempt failed" }),
        );
        assert_eq!(*harness.logs.lock().unwrap(), [("17".to_owned(), stage)]);
        assert_eq!(
            state.count("cleanup"),
            usize::from(stage != "configuration")
        );
        assert_eq!(
            state.count("close"),
            usize::from(!["configuration", "worker-id", "acquisition"].contains(&stage))
        );
    }
}

#[tokio::test]
async fn multiple_failures_do_not_skip_shutdown_or_diagnostics() {
    let state = Arc::new(State {
        attempt_error: true,
        cleanup_error: true,
        close_error: true,
        ..State::default()
    });
    let harness = Harness::new(vec![state.clone()]);
    envelope(
        fetch(
            &harness,
            request(json!({ "taskName": Task::NAME, "taskId": "17" })),
        )
        .await,
        500,
        json!({ "error": "task processing attempt failed" }),
    );
    assert_eq!(
        *harness.logs.lock().unwrap(),
        ["attempt", "application-cleanup", "backend-close"].map(|stage| ("17".to_owned(), stage))
    );
    assert_eq!(
        *state.events.lock().unwrap(),
        ["configure", "acquire", "cleanup", "close"]
    );
}

#[tokio::test]
async fn concurrent_requests_have_fresh_scopes_and_backends() {
    let first_gate = gate();
    let second_gate = gate();
    let first = Arc::new(State {
        close_gate: Some(first_gate.clone()),
        ..State::default()
    });
    let second = Arc::new(State {
        close_gate: Some(second_gate.clone()),
        persisted_name: CountTask::NAME,
        persisted_payload: json!({ "count": 42 }),
        ..State::default()
    });
    let harness = Harness::new(vec![first.clone(), second.clone()]);
    *harness.samples.lock().unwrap() =
        VecDeque::from([Ok([0, 0, 0, 0, 0, 1]), Ok([0, 0, 0, 0, 0, 2])]);
    assert_eq!(harness.configured.load(Ordering::SeqCst), 0);
    let first_response = fetch(
        &harness,
        request(json!({ "taskName": Task::NAME, "taskId": "17" })),
    );
    let second_response = fetch(
        &harness,
        request(json!({ "taskName": CountTask::NAME, "taskId": "18" })),
    );
    tokio::pin!(first_response, second_response);
    tokio::select! {
        biased;
        _ = &mut first_response => panic!("first response before close"),
        _ = &mut second_response => panic!("second response before close"),
        _ = async { wait(&first.closing).await; wait(&second.closing).await; } => {}
    }
    second_gate.add_permits(1);
    tokio::select! {
        _ = &mut first_response => panic!("first response released with second"),
        response = &mut second_response => envelope(response, 200, json!({ "taskId": "18", "attemptFinished": true })),
    }
    first_gate.add_permits(1);
    envelope(
        first_response.await,
        200,
        json!({ "taskId": "17", "attemptFinished": true }),
    );
    assert_eq!(*first.builds.lock().unwrap(), [1]);
    assert_eq!(*second.builds.lock().unwrap(), [2]);
    assert_eq!(first.count("count-build"), 0);
    assert_eq!(second.count("build"), 0);
    assert_eq!(
        *first.claims.lock().unwrap(),
        [(Task::NAME.to_owned(), 1, 17)]
    );
    assert_eq!(
        *second.claims.lock().unwrap(),
        [(CountTask::NAME.to_owned(), 2, 18)]
    );
    assert_eq!(harness.configured.load(Ordering::SeqCst), 2);
    for state in [first, second] {
        assert_eq!(state.count("configure"), 1);
        assert_eq!(state.count("acquire"), 1);
        assert_eq!(state.count("cleanup"), 1);
        assert_eq!(state.count("close"), 1);
    }
}

#[tokio::test]
async fn renewal_loss_aborts_worker_but_retains_and_awaits_application_cleanup() {
    let cleanup_gate = gate();
    let state = Arc::new(State {
        renewal_loss: true,
        processing_gate: Some(gate()),
        cleanup_gate: Some(cleanup_gate.clone()),
        ..State::default()
    });
    let harness = Harness::new(vec![state.clone()]);
    let response = fetch(
        &harness,
        request(json!({ "taskName": Task::NAME, "taskId": "17" })),
    );
    tokio::pin!(response);
    tokio::select! {
        _ = &mut response => panic!("response before lease loss"),
        _ = async { wait(&state.started).await; wait(&state.renewing).await; } => {}
    }
    state.renewal_gate.add_permits(1);
    tokio::select! {
        _ = &mut response => panic!("response before cleanup"),
        _ = wait(&state.cleaning) => {}
    }
    assert_eq!(state.count("worker-dropped"), 1);
    assert_eq!(state.count("close"), 0);
    cleanup_gate.add_permits(1);
    envelope(
        response.await,
        200,
        json!({ "taskId": "17", "attemptFinished": true }),
    );
    assert_eq!(*state.resource.lock().await, Some(true));
    assert_eq!(state.count("cleanup"), 1);
    assert_eq!(state.count("close"), 1);
    assert_eq!(state.count("finish"), 0);
    assert_eq!(state.count("fail"), 0);
    assert!(harness.logs.lock().unwrap().is_empty());
}
