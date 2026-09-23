use std::{
    cell::RefCell,
    collections::VecDeque,
    marker::PhantomData,
    pin::{Pin, pin},
    rc::Rc,
    sync::{Arc, Mutex},
    task::Poll,
};

use http::{Request, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use super::*;
use crate::{
    PublishTrigger,
    backends::{PublishTaskError, PublishedTask},
    cloudflare::{ProcessorFetcher, TextBody},
    time::Instant,
};

const SECRET: &str = "postgres://user:secret@private/database";

#[derive(Serialize, Deserialize)]
struct Payload {
    name: String,
}

struct Task;
impl TaskDefinition for Task {
    const NAME: &'static str = "publisher_contract/\"\\\n雪🦀";
    type Trigger = PublishTrigger<Payload>;
    // Its failing codec must never be used by plain publication.
    type Callback = BadPayload;
}

struct UnitTask;
impl TaskDefinition for UnitTask {
    const NAME: &'static str = "unit";
    type Trigger = PublishTrigger<()>;
    type Callback = ();
}

#[derive(Deserialize)]
struct BadPayload;
impl Serialize for BadPayload {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(SECRET))
    }
}

struct BadTask;
impl TaskDefinition for BadTask {
    const NAME: &'static str = "bad_codec";
    type Trigger = PublishTrigger<BadPayload>;
    type Callback = ();
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    Configuration,
    Acquisition,
    Publication,
    Close,
    Lookup,
    Fetch,
    Body,
}

#[derive(Debug)]
struct Cause(Fault);
impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{SECRET} {:?}", self.0)
    }
}
impl Error for Cause {}

struct State {
    id: u64,
    url: String,
    schema: String,
    fault: Option<Fault>,
    close_failure: bool,
    status: u16,
    events: Mutex<Vec<&'static str>>,
    publications: Mutex<Vec<(String, Value)>>,
    deadlines: Mutex<Vec<Option<Instant>>>,
    names: Mutex<Vec<String>>,
    requests: Mutex<Vec<Request<String>>>,
    publish_gate: Option<Arc<Semaphore>>,
    close_gate: Option<Arc<Semaphore>>,
    body_gate: Option<Arc<Semaphore>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            id: 17,
            url: "hyperdrive-url".into(),
            schema: "request_schema".into(),
            fault: None,
            close_failure: false,
            status: 200,
            events: Mutex::default(),
            publications: Mutex::default(),
            deadlines: Mutex::default(),
            names: Mutex::default(),
            requests: Mutex::default(),
            publish_gate: None,
            close_gate: None,
            body_gate: None,
        }
    }
}

impl State {
    fn event(&self, event: &'static str) {
        self.events.lock().unwrap().push(event);
    }

    fn count(&self, event: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|value| **value == event)
            .count()
    }

    fn fail(&self, fault: Fault) -> Result<(), BoxDispatchError> {
        if self.fault == Some(fault) || (fault == Fault::Close && self.close_failure) {
            Err(Box::new(Cause(fault)))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
struct Publishing(Arc<State>);
impl TaskPublishingBackend for Publishing {
    async fn publish<T>(
        &self,
        payload: <T::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.record::<T>(payload, None).await
    }

    async fn publish_future<T>(
        &self,
        payload: <T::Trigger as PublishActivationStrategy>::Payload,
        available_from: Instant,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.record::<T>(payload, Some(available_from)).await
    }
}

impl Publishing {
    async fn record<T>(
        &self,
        payload: <T::Trigger as PublishActivationStrategy>::Payload,
        available_from: Option<Instant>,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.0.deadlines.lock().unwrap().push(available_from);
        self.0.event("publish");
        wait(&self.0.publish_gate).await;
        let payload = serde_json::to_value(payload)
            .map_err(|error| PublishTaskError::Backend(error.into()))?;
        self.0
            .publications
            .lock()
            .unwrap()
            .push((T::NAME.into(), payload));
        self.0
            .fail(Fault::Publication)
            .map_err(PublishTaskError::Backend)?;
        Ok(PublishedTask { task_id: self.0.id })
    }
}

struct Namespace(Arc<State>);
impl DurableObjectNamespaceLike for Namespace {
    type Stub = Stub;

    fn get_by_name(&self, name: &str) -> Result<Self::Stub, BoxDispatchError> {
        self.0.event("lookup");
        self.0.names.lock().unwrap().push(name.into());
        self.0.fail(Fault::Lookup)?;
        Ok(Stub(self.0.clone()))
    }
}

struct Stub(Arc<State>);
impl ProcessorFetcher for Stub {
    async fn fetch(
        &self,
        request: Request<String>,
    ) -> Result<Response<TextBody>, BoxDispatchError> {
        self.0.event("dispatch");
        self.0.requests.lock().unwrap().push(request);
        self.0.fail(Fault::Fetch)?;
        let state = self.0.clone();
        let body: TextBody = Box::pin(async move {
            state.event("body-start");
            wait(&state.body_gate).await;
            state.fail(Fault::Body)?;
            state.event("body-end");
            Ok(response_body())
        });
        Ok(Response::builder().status(self.0.status).body(body)?)
    }
}

type Settings = (String, String, Arc<State>);

struct Harness<T, C> {
    configure: C,
    task: PhantomData<fn() -> T>,
}

impl<T, C> Publisher for Harness<T, C>
where
    T: TaskDefinition<Trigger: PublishActivationStrategy<DispatchToken = PublishDispatchToken>>,
    C: Fn() -> Result<Scope<Settings, Namespace>, BoxDispatchError>,
{
    type Task = T;
    type Settings = Settings;
    type Backend = Publishing;
    type Namespace = Namespace;

    fn configure(&self) -> Result<Scope<Settings, Namespace>, BoxDispatchError> {
        (self.configure)()
    }

    async fn acquire(
        &self,
        (url, schema, state): Settings,
    ) -> Result<Publishing, BoxDispatchError> {
        assert_eq!(url, state.url);
        assert_eq!(schema, state.schema);
        state.event("acquire");
        state.fail(Fault::Acquisition)?;
        Ok(Publishing(state))
    }

    async fn close(&self, backend: Publishing) -> Result<(), BoxDispatchError> {
        backend.0.event("close");
        wait(&backend.0.close_gate).await;
        backend.0.fail(Fault::Close)?;
        backend.0.event("closed");
        Ok(())
    }
}

fn harness<T>(
    states: Vec<Arc<State>>,
) -> Harness<T, impl Fn() -> Result<Scope<Settings, Namespace>, BoxDispatchError>> {
    // A JS-affine SDK callback need not be Send or Sync.
    let states = Rc::new(RefCell::new(VecDeque::from(states)));
    Harness {
        configure: move || {
            let state = states.borrow_mut().pop_front().expect("one scope per call");
            state.event("configure");
            state.fail(Fault::Configuration)?;
            Ok(Scope {
                settings: (state.url.clone(), state.schema.clone(), state.clone()),
                dispatcher: Namespace(state),
            })
        },
        task: PhantomData,
    }
}

fn payload(name: &str) -> Payload {
    Payload { name: name.into() }
}

fn response_body() -> String {
    format!("{SECRET}{}tail", "x".repeat(2000))
}

async fn wait(gate: &Option<Arc<Semaphore>>) {
    if let Some(gate) = gate {
        gate.acquire().await.unwrap().forget();
    }
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

fn assert_error(error: &PostgresPublisherError, stage: PostgresPublisherStage, id: Option<&str>) {
    assert_eq!(error.stage, stage);
    assert_eq!(
        error.to_string(),
        format!("PostgreSQL publisher failed at {stage}")
    );
    assert!(!error.to_string().contains(SECRET));
    assert_eq!(
        error
            .receipt
            .as_ref()
            .map(|receipt| receipt.task_id.as_str()),
        id
    );
    assert!(std::ptr::eq(
        error.source().unwrap(),
        error.cause.as_ref() as &dyn Error
    ));
}

fn assert_dispatch(state: &State, name: &str, id: &str) {
    assert_eq!(*state.names.lock().unwrap(), ["global"]);
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].uri(), "https://dispatcher/dispatch");
    assert_eq!(requests[0].method(), "POST");
    assert_eq!(requests[0].headers()["content-type"], "application/json");
    assert_eq!(
        serde_json::from_str::<Value>(requests[0].body()).unwrap(),
        json!({ "taskId": id, "taskName": name })
    );
}

#[tokio::test]
async fn construction_is_inert_and_publication_forwards_the_bound_task_and_payload() {
    for available_from in [
        None,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    ] {
        let _logs = tracing::subscriber::set_default(NoLogs);
        let state = Arc::new(State::default());
        let publisher = harness::<Task>(vec![state.clone()]);
        assert!(state.events.lock().unwrap().is_empty());
        let receipt = publish(&publisher, payload("Ada"), available_from)
            .await
            .unwrap();
        assert_eq!(receipt.task_id, "17");
        assert_eq!(
            *state.publications.lock().unwrap(),
            [(Task::NAME.into(), json!({ "name": "Ada" }))]
        );
        assert_dispatch(&state, Task::NAME, "17");
        assert_eq!(*state.deadlines.lock().unwrap(), [available_from]);
        assert_eq!(
            *state.events.lock().unwrap(),
            [
                "configure",
                "acquire",
                "publish",
                "close",
                "closed",
                "lookup",
                "dispatch",
                "body-start",
                "body-end"
            ]
        );
    }
}

#[tokio::test]
async fn unit_payload_is_plain_publication() {
    for available_from in [
        None,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    ] {
        let state = Arc::new(State::default());
        let publisher = harness::<UnitTask>(vec![state.clone()]);
        assert_eq!(
            publish(&publisher, (), available_from)
                .await
                .unwrap()
                .task_id,
            "17"
        );
        assert_dispatch(&state, UnitTask::NAME, "17");
        assert_eq!(
            *state.publications.lock().unwrap(),
            [(UnitTask::NAME.into(), Value::Null)]
        );
        assert_eq!(state.count("close"), 1);
    }
}

#[tokio::test]
async fn safe_ids_dispatch_exactly() {
    for available_from in [
        None,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    ] {
        for id in [1, 9_007_199_254_740_991] {
            let state = Arc::new(State {
                id,
                ..State::default()
            });
            let publisher = harness::<Task>(vec![state.clone()]);
            let receipt = publish(&publisher, payload("Ada"), available_from)
                .await
                .unwrap();
            assert_eq!(receipt.task_id, id.to_string());
            assert_dispatch(&state, Task::NAME, &receipt.task_id);
        }
    }
}

#[tokio::test]
async fn unsupported_ids_retain_exact_receipts_close_once_and_never_dispatch() {
    for available_from in [
        None,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    ] {
        for id in [
            0,
            9_007_199_254_740_992,
            9_007_199_254_740_993,
            9_223_372_036_854_775_807,
            u64::MAX,
        ] {
            let state = Arc::new(State {
                id,
                ..State::default()
            });
            let publisher = harness::<Task>(vec![state.clone()]);
            let error = publish(&publisher, payload("Ada"), available_from)
                .await
                .unwrap_err();
            assert_error(
                &error,
                PostgresPublisherStage::TaskId,
                Some(&id.to_string()),
            );
            assert!(error.backend_close_error.is_none());
            assert_eq!(state.count("publish"), 1);
            assert_eq!(state.count("close"), 1);
            assert_eq!(state.count("lookup"), 0);
        }
    }
}

#[tokio::test]
async fn publication_close_and_full_success_or_error_body_are_awaited_in_order() {
    for available_from in [
        None,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    ] {
        for status in [200, 503] {
            let publication = Arc::new(Semaphore::new(0));
            let close = Arc::new(Semaphore::new(0));
            let body = Arc::new(Semaphore::new(0));
            let state = Arc::new(State {
                status,
                publish_gate: Some(publication.clone()),
                close_gate: Some(close.clone()),
                body_gate: Some(body.clone()),
                ..State::default()
            });
            let publisher = harness::<Task>(vec![state.clone()]);
            let mut operation = pin!(publish(&publisher, payload("Ada"), available_from));
            assert_pending(operation.as_mut()).await;
            assert_eq!(state.count("publish"), 1);
            assert_eq!(state.count("close"), 0);
            assert_eq!(state.count("dispatch"), 0);
            publication.add_permits(1);
            assert_pending(operation.as_mut()).await;
            assert_eq!(state.count("close"), 1);
            assert_eq!(state.count("dispatch"), 0);
            close.add_permits(1);
            assert_pending(operation.as_mut()).await;
            assert_eq!(state.count("closed"), 1);
            assert_eq!(state.count("dispatch"), 1);
            assert_eq!(state.count("body-start"), 1);
            assert_eq!(state.count("body-end"), 0);
            body.add_permits(1);
            let result = operation.await;
            if status == 200 {
                assert_eq!(result.unwrap().task_id, "17");
            } else {
                let error = result.unwrap_err();
                assert_error(&error, PostgresPublisherStage::Dispatch, Some("17"));
                assert_eq!(
                    error.cause.to_string(),
                    format!(
                        "task dispatcher returned HTTP 503: {}",
                        &response_body()[..500]
                    )
                );
                assert!(error.backend_close_error.is_none());
            }
            assert_eq!(state.count("body-end"), 1);
            assert_eq!(state.count("publish"), 1);
            assert_eq!(state.count("close"), 1);
            assert_dispatch(&state, Task::NAME, "17");
        }
    }
}

#[tokio::test]
async fn failures_preserve_sources_receipts_and_cleanup_without_logging() {
    for available_from in [
        None,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    ] {
        use PostgresPublisherStage::*;
        let _logs = tracing::subscriber::set_default(NoLogs);
        for (fault, stage, acquired, known) in [
            (Fault::Configuration, Configuration, false, false),
            (Fault::Acquisition, Acquisition, false, false),
            (Fault::Publication, Publication, true, false),
            (Fault::Close, BackendClose, true, true),
            (Fault::Lookup, Dispatch, true, true),
            (Fault::Fetch, Dispatch, true, true),
            (Fault::Body, Dispatch, true, true),
        ] {
            let state = Arc::new(State {
                fault: Some(fault),
                ..State::default()
            });
            let publisher = harness::<Task>(vec![state.clone()]);
            let error = publish(&publisher, payload("Ada"), available_from)
                .await
                .unwrap_err();
            assert_error(&error, stage, known.then_some("17"));
            let cause = if fault == Fault::Publication {
                error
                    .cause
                    .downcast_ref::<PublishTaskError>()
                    .unwrap()
                    .source()
                    .unwrap()
            } else {
                error.cause.as_ref()
            };
            assert_eq!(cause.downcast_ref::<Cause>().unwrap().0, fault);
            assert!(error.backend_close_error.is_none());
            assert_eq!(state.count("configure"), 1);
            assert_eq!(state.count("publish"), usize::from(acquired));
            assert_eq!(state.count("close"), usize::from(acquired));
            assert_eq!(state.count("lookup"), usize::from(stage == Dispatch));
        }
    }
}

#[tokio::test]
async fn serialization_failure_is_an_unknown_publication_outcome() {
    for available_from in [
        None,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    ] {
        let state = Arc::new(State::default());
        let publisher = harness::<BadTask>(vec![state.clone()]);
        let error = publish(&publisher, BadPayload, available_from)
            .await
            .unwrap_err();
        assert_error(&error, PostgresPublisherStage::Publication, None);
        let source = error
            .cause
            .downcast_ref::<PublishTaskError>()
            .unwrap()
            .source()
            .unwrap();
        assert_eq!(
            source
                .downcast_ref::<serde_json::Error>()
                .unwrap()
                .to_string(),
            SECRET
        );
        assert_eq!(state.count("publish"), 1);
        assert_eq!(state.count("close"), 1);
        assert_eq!(state.count("lookup"), 0);
    }
}

#[tokio::test]
async fn primary_publication_and_id_failures_survive_a_later_close_failure() {
    for available_from in [
        None,
        Some(Instant::now() + std::time::Duration::from_secs(3600)),
    ] {
        for id in [17, 9_007_199_254_740_993] {
            let close = Arc::new(Semaphore::new(0));
            let state = Arc::new(State {
                id,
                fault: (id == 17).then_some(Fault::Publication),
                close_failure: true,
                close_gate: Some(close.clone()),
                ..State::default()
            });
            let publisher = harness::<Task>(vec![state.clone()]);
            let mut operation = pin!(publish(&publisher, payload("Ada"), available_from));
            assert_pending(operation.as_mut()).await;
            assert_eq!(state.count("close"), 1);
            assert_eq!(state.count("lookup"), 0);
            close.add_permits(1);
            let error = operation.await.unwrap_err();
            if id == 17 {
                assert_error(&error, PostgresPublisherStage::Publication, None);
                let source = error
                    .cause
                    .downcast_ref::<PublishTaskError>()
                    .unwrap()
                    .source()
                    .unwrap();
                assert_eq!(
                    source.downcast_ref::<Cause>().unwrap().0,
                    Fault::Publication
                );
            } else {
                assert_error(
                    &error,
                    PostgresPublisherStage::TaskId,
                    Some("9007199254740993"),
                );
                assert_eq!(
                    error.cause.to_string(),
                    "task ID must be a canonical positive safe integer"
                );
            }
            assert_eq!(
                error
                    .backend_close_error
                    .unwrap()
                    .downcast_ref::<Cause>()
                    .unwrap()
                    .0,
                Fault::Close
            );
            assert_eq!(state.count("publish"), 1);
            assert_eq!(state.count("close"), 1);
            assert_eq!(state.count("lookup"), 0);
        }
    }
}

#[tokio::test]
async fn concurrent_and_sequential_calls_keep_scopes_connections_receipts_and_failures_local() {
    let states: Vec<_> = (1..=3)
        .map(|id| {
            Arc::new(State {
                id,
                url: format!("url_{id}"),
                schema: format!("schema_{id}"),
                close_failure: id == 1,
                close_gate: Some(Arc::new(Semaphore::new(0))),
                ..State::default()
            })
        })
        .collect();
    let publisher = harness::<Task>(states.clone());
    let deadline = Instant::now() + std::time::Duration::from_secs(3600);
    let mut first = pin!(publish(&publisher, payload("first"), None));
    let mut second = pin!(publish(&publisher, payload("second"), Some(deadline)));
    assert_pending(first.as_mut()).await;
    assert_pending(second.as_mut()).await;
    states[1].close_gate.as_ref().unwrap().add_permits(1);
    assert_eq!(second.await.unwrap().task_id, "2");
    assert_pending(first.as_mut()).await;
    states[0].close_gate.as_ref().unwrap().add_permits(1);
    assert_error(
        &first.await.unwrap_err(),
        PostgresPublisherStage::BackendClose,
        Some("1"),
    );
    states[2].close_gate.as_ref().unwrap().add_permits(1);
    assert_eq!(
        publish(&publisher, payload("third"), None)
            .await
            .unwrap()
            .task_id,
        "3"
    );
    for (i, name) in ["first", "second", "third"].into_iter().enumerate() {
        assert_eq!(
            *states[i].deadlines.lock().unwrap(),
            [if i == 1 { Some(deadline) } else { None }]
        );
        assert_eq!(states[i].count("configure"), 1);
        assert_eq!(states[i].count("acquire"), 1);
        assert_eq!(states[i].count("publish"), 1);
        assert_eq!(states[i].count("close"), 1);
        assert_eq!(states[i].count("dispatch"), usize::from(i != 0));
        assert_eq!(
            *states[i].publications.lock().unwrap(),
            [(Task::NAME.into(), json!({ "name": name }))]
        );
    }
}

struct NoLogs;
impl tracing::Subscriber for NoLogs {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        panic!("publisher must not log");
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
