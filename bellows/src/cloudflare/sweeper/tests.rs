use std::{
    cell::RefCell,
    collections::VecDeque,
    pin::pin,
    rc::Rc,
    sync::{Arc, Mutex},
};

use http::{Request, Response};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use super::*;
use crate::cloudflare::{ProcessorFetcher, TextBody};

const SECRET: &str = "postgres://user:secret@private/database";
const NAME: &str = "unregistered/\"\\\n雪🦀";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    Configuration,
    Acquisition,
    Window,
    Page,
    Lookup,
    Transport,
    Body,
    Status,
    Close,
}

#[derive(Debug)]
struct Cause(Fault);
impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{SECRET} {:?}", self.0)
    }
}
impl Error for Cause {}

struct Backend {
    pages: Mutex<VecDeque<Vec<(i64, String)>>>,
    cursors: Mutex<Vec<Option<i64>>>,
    events: Mutex<Vec<&'static str>>,
    requests: Mutex<Vec<Value>>,
    fault: Option<Fault>,
    close_fault: bool,
    page_gate: Option<Arc<Semaphore>>,
    body_gate: Option<Arc<Semaphore>>,
    close_gate: Option<Arc<Semaphore>>,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            pages: Mutex::new(VecDeque::from([vec![row(1)], vec![]])),
            cursors: Mutex::default(),
            events: Mutex::default(),
            requests: Mutex::default(),
            fault: None,
            close_fault: false,
            page_gate: None,
            body_gate: None,
            close_gate: None,
        }
    }
}

fn row(id: i64) -> (i64, String) {
    (id, NAME.into())
}

impl Backend {
    fn event(&self, event: &'static str) {
        self.events.lock().unwrap().push(event);
    }
    fn count(&self, event: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|v| **v == event)
            .count()
    }
    fn fail(&self, fault: Fault) -> Result<(), BoxDispatchError> {
        if self.fault == Some(fault) || (fault == Fault::Close && self.close_fault) {
            Err(Box::new(Cause(fault)))
        } else {
            Ok(())
        }
    }
}

struct Namespace(Arc<Backend>);
impl DurableObjectNamespaceLike for Namespace {
    type Stub = Stub;
    fn get_by_name(&self, name: &str) -> Result<Stub, BoxDispatchError> {
        assert_eq!(name, "global");
        self.0.event("lookup");
        if self.0.count("lookup") == 1 {
            self.0.fail(Fault::Lookup)?;
        }
        Ok(Stub(self.0.clone()))
    }
}

struct Stub(Arc<Backend>);
impl ProcessorFetcher for Stub {
    async fn fetch(
        &self,
        request: Request<String>,
    ) -> Result<Response<TextBody>, BoxDispatchError> {
        assert_eq!(request.uri(), "https://dispatcher/dispatch");
        assert_eq!(request.method(), "POST");
        assert_eq!(request.headers()["content-type"], "application/json");
        let body = serde_json::from_str::<Value>(request.body())?;
        let first = body["taskId"] == "1";
        self.0.requests.lock().unwrap().push(body);
        if first {
            self.0.fail(Fault::Transport)?;
        }
        let state = self.0.clone();
        let body: TextBody = Box::pin(async move {
            state.event("body-start");
            wait(&state.body_gate).await;
            if first {
                state.fail(Fault::Body)?;
            }
            state.event("body-end");
            Ok(format!("{SECRET}{}tail", "x".repeat(5000)))
        });
        Ok(Response::builder()
            .status(if first && self.0.fault == Some(Fault::Status) {
                503
            } else {
                200
            })
            .body(body)?)
    }
}

// Rc deliberately models a non-Send, JS-affine configuration closure.
struct Harness(Rc<RefCell<VecDeque<Arc<Backend>>>>);
fn harness(states: Vec<Arc<Backend>>) -> Harness {
    Harness(Rc::new(RefCell::new(states.into())))
}
impl Sweeper for Harness {
    type Settings = (String, String, Arc<Backend>);
    type Backend = Arc<Backend>;
    type Window = (i64, i64);
    type Namespace = Namespace;

    fn configure(&self) -> Result<Scope<Self::Settings, Namespace>, BoxDispatchError> {
        let backend = self.0.borrow_mut().pop_front().expect("once per call");
        backend.event("configure");
        backend.fail(Fault::Configuration)?;
        Ok(Scope {
            settings: (
                "hyperdrive-url".into(),
                "sweep_schema".into(),
                backend.clone(),
            ),
            dispatcher: Namespace(backend),
        })
    }
    async fn acquire(
        &self,
        (url, schema, backend): Self::Settings,
    ) -> Result<Arc<Backend>, BoxDispatchError> {
        assert_eq!(url, "hyperdrive-url");
        assert_eq!(schema, "sweep_schema");
        backend.event("acquire");
        backend.fail(Fault::Acquisition)?;
        Ok(backend)
    }
    async fn begin(&self, backend: &Arc<Backend>) -> Result<Self::Window, BoxDispatchError> {
        backend.event("window");
        backend.fail(Fault::Window)?;
        Ok((1234, i64::MAX))
    }
    async fn page(
        &self,
        backend: &Arc<Backend>,
        window: &Self::Window,
        cursor: Option<i64>,
    ) -> Result<Vec<(i64, String)>, BoxDispatchError> {
        assert_eq!(*window, (1234, i64::MAX));
        backend.cursors.lock().unwrap().push(cursor);
        if cursor.is_some() {
            wait(&backend.page_gate).await;
            backend.fail(Fault::Page)?;
        }
        Ok(backend
            .pages
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_default())
    }
    async fn close(&self, backend: Arc<Backend>) -> Result<(), BoxDispatchError> {
        backend.event("close");
        wait(&backend.close_gate).await;
        backend.fail(Fault::Close)?;
        backend.event("closed");
        Ok(())
    }
}

async fn wait(gate: &Option<Arc<Semaphore>>) {
    if let Some(gate) = gate {
        gate.acquire().await.unwrap().forget();
    }
}
async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    // Manual polling must still let Tokio replenish its cooperative semaphore budget.
    tokio::task::yield_now().await;
}
fn report(discovered: u64, accepted: u64, failed: u64) -> PostgresSweepReport {
    PostgresSweepReport {
        discovered,
        accepted,
        failed,
    }
}
fn check_error(
    error: &PostgresSweeperError,
    stage: PostgresSweeperStage,
    report: PostgresSweepReport,
) {
    assert_eq!(error.stage, stage);
    assert_eq!(error.report, report);
    assert_eq!(report.discovered, report.accepted + report.failed);
    assert_eq!(
        error.to_string(),
        format!("PostgreSQL sweeper failed at {stage}")
    );
    assert!(!error.to_string().contains(SECRET));
    assert!(std::ptr::eq(
        error.source().unwrap(),
        error.cause.as_ref() as &dyn Error
    ));
}

#[tokio::test]
async fn inert_construction_exact_dispatch_and_empty_sweeps() {
    let state = Arc::new(Backend::default());
    let empty = Arc::new(Backend {
        pages: Mutex::default(),
        ..Backend::default()
    });
    let sweeper = harness(vec![state.clone(), empty.clone()]);
    assert!(state.events.lock().unwrap().is_empty());
    assert_eq!(sweep(&sweeper).await.unwrap(), report(1, 1, 0));
    assert_eq!(
        *state.requests.lock().unwrap(),
        [json!({"taskId": "1", "taskName": NAME})]
    );
    assert_eq!(*state.cursors.lock().unwrap(), [None, Some(1)]);
    for event in [
        "configure",
        "acquire",
        "window",
        "close",
        "closed",
        "body-end",
    ] {
        assert_eq!(state.count(event), 1);
    }
    assert_eq!(sweep(&sweeper).await.unwrap(), report(0, 0, 0));
    assert_eq!(empty.count("closed"), 1);
    assert_eq!(empty.count("lookup"), 0);
}

#[tokio::test]
async fn three_hundred_dispatches_launch_during_queries_and_settle_during_close() {
    let state = Arc::new(Backend {
        pages: Mutex::new(
            (0..3)
                .map(|page| {
                    (1..=100)
                        .map(|id| {
                            (
                                page * 100 + id,
                                if id % 2 == 0 {
                                    NAME.into()
                                } else {
                                    "other unregistered".into()
                                },
                            )
                        })
                        .collect()
                })
                .collect(),
        ),
        page_gate: Some(Arc::new(Semaphore::new(0))),
        body_gate: Some(Arc::new(Semaphore::new(0))),
        close_gate: Some(Arc::new(Semaphore::new(0))),
        ..Backend::default()
    });
    let sweeper = harness(vec![state.clone()]);
    let mut call = pin!(sweep(&sweeper));
    // FuturesUnordered may yield cooperatively; poll until every first-page body has started.
    for _ in 0..20 {
        assert_pending(call.as_mut()).await;
    }
    assert_eq!(state.count("body-start"), 100);
    assert_eq!(*state.cursors.lock().unwrap(), [None, Some(100)]);
    assert_eq!(state.count("body-end"), 0);
    state.page_gate.as_ref().unwrap().add_permits(3);
    for _ in 0..40 {
        assert_pending(call.as_mut()).await;
    }
    assert_eq!(state.count("body-start"), 300);
    assert_eq!(state.count("close"), 1);
    assert_eq!(
        *state.cursors.lock().unwrap(),
        [None, Some(100), Some(200), Some(300)]
    );
    let ids: std::collections::HashSet<_> = state
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|value| value["taskId"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids.len(), 300);
    state.body_gate.as_ref().unwrap().add_permits(300);
    for _ in 0..40 {
        assert_pending(call.as_mut()).await;
    }
    assert_eq!(state.count("body-end"), 300);
    assert_eq!(state.count("closed"), 0);
    state.close_gate.as_ref().unwrap().add_permits(1);
    assert_eq!(call.await.unwrap(), report(300, 300, 0));
}

#[tokio::test]
async fn unsupported_ids_preserve_identity_and_cursor_and_do_not_starve_later_pages() {
    for id in [
        i64::MIN,
        -1,
        0,
        9_007_199_254_740_992,
        9_007_199_254_740_993,
        i64::MAX,
    ] {
        let state = Arc::new(Backend {
            pages: Mutex::new(VecDeque::from([
                vec![row(id)],
                vec![row(9_007_199_254_740_991)],
                vec![],
            ])),
            ..Backend::default()
        });
        let error = sweep(&harness(vec![state.clone()])).await.unwrap_err();
        check_error(&error, PostgresSweeperStage::Candidate, report(2, 1, 1));
        assert_eq!(
            error.candidate,
            Some(PostgresSweepCandidate {
                task_id: id.to_string(),
                task_name: NAME.into()
            })
        );
        assert_eq!(
            *state.cursors.lock().unwrap(),
            [None, Some(id), Some(9_007_199_254_740_991)]
        );
        assert_eq!(
            *state.requests.lock().unwrap(),
            [json!({"taskId": "9007199254740991", "taskName": NAME})]
        );
    }
}

#[tokio::test]
async fn empty_names_fail_but_other_names_are_exact() {
    let state = Arc::new(Backend {
        pages: Mutex::new(VecDeque::from([
            vec![(1, "".into()), (2, " ".into()), row(3)],
            vec![],
        ])),
        ..Backend::default()
    });
    let error = sweep(&harness(vec![state.clone()])).await.unwrap_err();
    check_error(&error, PostgresSweeperStage::Candidate, report(3, 2, 1));
    assert_eq!(error.candidate.unwrap().task_name, "");
    assert_eq!(
        *state.requests.lock().unwrap(),
        [
            json!({"taskId": "2", "taskName": " "}),
            json!({"taskId": "3", "taskName": NAME})
        ]
    );
}

#[tokio::test]
async fn dispatch_failures_continue_and_consume_bodies_without_retrying() {
    let _logs = tracing::subscriber::set_default(NoLogs);
    for fault in [Fault::Lookup, Fault::Transport, Fault::Status, Fault::Body] {
        let state = Arc::new(Backend {
            pages: Mutex::new(VecDeque::from([vec![row(1), row(2)], vec![row(3)], vec![]])),
            fault: Some(fault),
            close_fault: true,
            ..Backend::default()
        });
        let error = sweep(&harness(vec![state.clone()])).await.unwrap_err();
        check_error(&error, PostgresSweeperStage::Dispatch, report(3, 2, 1));
        assert_eq!(error.candidate.unwrap().task_id, "1");
        if fault != Fault::Status {
            assert_eq!(error.cause.downcast_ref::<Cause>().unwrap().0, fault);
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
        assert_eq!(state.count("lookup"), 3);
        assert_eq!(
            state.count("body-end"),
            if fault == Fault::Status { 3 } else { 2 }
        );
        assert_eq!(state.count("close"), 1);
        assert_eq!(state.cursors.lock().unwrap().len(), 3);
    }
}

#[tokio::test]
async fn configuration_acquisition_and_window_failures() {
    let _logs = tracing::subscriber::set_default(NoLogs);
    for (fault, stage) in [
        (Fault::Configuration, PostgresSweeperStage::Configuration),
        (Fault::Acquisition, PostgresSweeperStage::Acquisition),
        (Fault::Window, PostgresSweeperStage::Discovery),
    ] {
        let state = Arc::new(Backend {
            fault: Some(fault),
            close_fault: true,
            ..Backend::default()
        });
        let error = sweep(&harness(vec![state.clone()])).await.unwrap_err();
        check_error(&error, stage, report(0, 0, 0));
        assert_eq!(error.cause.downcast_ref::<Cause>().unwrap().0, fault);
        assert_eq!(state.count("close"), usize::from(fault == Fault::Window));
        assert_eq!(error.backend_close_error.is_some(), fault == Fault::Window);
        assert_eq!(state.count("lookup"), 0);
    }
}

#[tokio::test]
async fn discovery_failure_closes_before_slow_dispatch_and_waits_for_every_body() {
    let state = Arc::new(Backend {
        pages: Mutex::new(VecDeque::from([vec![row(1), row(2)], vec![row(3)]])),
        fault: Some(Fault::Page),
        close_fault: true,
        body_gate: Some(Arc::new(Semaphore::new(0))),
        ..Backend::default()
    });
    let sweeper = harness(vec![state.clone()]);
    let mut call = pin!(sweep(&sweeper));
    assert_pending(call.as_mut()).await;
    assert_eq!(state.count("close"), 1);
    assert_eq!(state.count("body-start"), 2);
    state.body_gate.as_ref().unwrap().add_permits(1);
    assert_pending(call.as_mut()).await;
    state.body_gate.as_ref().unwrap().add_permits(1);
    let error = call.await.unwrap_err();
    check_error(&error, PostgresSweeperStage::Discovery, report(2, 2, 0));
    assert_eq!(error.cause.downcast_ref::<Cause>().unwrap().0, Fault::Page);
    assert!(error.candidate.is_none());
    assert!(error.backend_close_error.is_some());
    assert_eq!(state.count("lookup"), 2);
}

#[tokio::test]
async fn close_failure_alone_or_after_invalid_candidate() {
    for invalid in [false, true] {
        let state = Arc::new(Backend {
            pages: Mutex::new(VecDeque::from([vec![row(if invalid { 0 } else { 1 })]])),
            close_fault: true,
            ..Backend::default()
        });
        let error = sweep(&harness(vec![state])).await.unwrap_err();
        check_error(
            &error,
            if invalid {
                PostgresSweeperStage::Candidate
            } else {
                PostgresSweeperStage::BackendClose
            },
            report(1, u64::from(!invalid), u64::from(invalid)),
        );
        assert_eq!(error.backend_close_error.is_some(), invalid);
    }
}

#[tokio::test]
async fn close_failure_observed_before_dispatch_failure_stays_primary() {
    let state = Arc::new(Backend {
        fault: Some(Fault::Body),
        close_fault: true,
        body_gate: Some(Arc::new(Semaphore::new(0))),
        ..Backend::default()
    });
    let sweeper = harness(vec![state.clone()]);
    let mut call = pin!(sweep(&sweeper));
    assert_pending(call.as_mut()).await;
    assert_eq!(state.count("close"), 1);
    state.body_gate.as_ref().unwrap().add_permits(1);
    let error = call.await.unwrap_err();
    check_error(&error, PostgresSweeperStage::BackendClose, report(1, 0, 1));
    assert!(error.backend_close_error.is_none());
    assert!(error.candidate.is_none());
}

#[tokio::test]
async fn independent_overlapping_scopes() {
    let first = Arc::new(Backend {
        pages: Mutex::new(VecDeque::from([vec![row(0)]])),
        close_gate: Some(Arc::new(Semaphore::new(0))),
        ..Backend::default()
    });
    let second = Arc::new(Backend {
        pages: Mutex::new(VecDeque::from([vec![row(3)]])),
        ..Backend::default()
    });
    let sweeper = harness(vec![first.clone(), second.clone()]);
    let mut call = pin!(sweep(&sweeper));
    assert_pending(call.as_mut()).await;
    assert_eq!(sweep(&sweeper).await.unwrap(), report(1, 1, 0));
    assert_pending(call.as_mut()).await;
    first.close_gate.as_ref().unwrap().add_permits(1);
    check_error(
        &call.await.unwrap_err(),
        PostgresSweeperStage::Candidate,
        report(1, 0, 1),
    );
    assert_eq!(*second.cursors.lock().unwrap(), [None, Some(3)]);
    assert_eq!(first.count("configure"), 1);
    assert_eq!(second.count("configure"), 1);
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
        panic!("sweeper must not log");
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
