#![cfg(target_arch = "wasm32")]

mod postgres;
mod publisher;
mod runtime;

pub use publisher::PublisherReceiver;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::{cell::RefCell, rc::Rc};

use bellows::cloudflare::{RetainedTaskDispatcher, dispatch_task, sdk::Dispatcher};
use futures_util::stream;
use serde_json::json;
use tokio::sync::Semaphore;
use worker::*;

#[event(fetch)]
pub async fn fetch(request: Request, env: Env, _ctx: Context) -> Result<Response> {
    let path = request.path();
    if path.starts_with("/publisher/") {
        return publisher::fetch(request, &env).await;
    }
    if path == "/postgres/close" {
        return postgres::run(&env).await;
    }
    if let Some(mode) = path.strip_prefix("/postgres/publish/") {
        return postgres::publish(&env, mode).await;
    }
    if let Some(mode) = path.strip_prefix("/runtime/") {
        if ![
            "finish",
            "finish-running",
            "fail",
            "lost",
            "error",
            "lost-completed",
            "error-completed",
            "no-claim",
        ]
        .contains(&mode)
        {
            return Response::error("not-found", 404);
        }
        return Response::from_json(&runtime::run(mode).await);
    }
    if let Some(task_id) = path.strip_prefix("/dispatch-task/") {
        let result = dispatch_task(&env.durable_object("BODY")?, "body_contract", task_id).await;
        return Response::from_json(&match result {
            Ok(()) => json!({ "ok": true }),
            Err(error) => json!({ "error": error.to_string() }),
        });
    }
    let binding = if path.starts_with("/dispatcher/") || path == "/dispatch" {
        "DISPATCHER"
    } else {
        "BODY"
    };
    env.durable_object(binding)?
        .get_by_name("global")?
        .fetch_with_request(request)
        .await
}

#[durable_object]
pub struct ContractDispatcher {
    dispatcher: RefCell<Rc<Dispatcher>>,
    state: State,
    env: Env,
}

impl DurableObject for ContractDispatcher {
    fn new(state: State, env: Env) -> Self {
        Self {
            dispatcher: RefCell::new(Rc::new(RetainedTaskDispatcher::from_bindings(
                state.storage(),
                env.service("PROCESSOR").unwrap(),
            ))),
            state,
            env,
        }
    }

    async fn fetch(&self, request: Request) -> Result<Response> {
        let dispatcher = self.dispatcher.borrow().clone();
        match request.path().as_str() {
            "/dispatcher/state" => {
                let records = self
                    .state
                    .storage()
                    .list_with_options(ListOptions::new().prefix("task:"))
                    .await?;
                let mut tasks = serde_json::Map::new();
                for entry in records.entries() {
                    let entry = js_sys::Array::from(&entry?);
                    tasks.insert(
                        entry.get(0).as_string().unwrap(),
                        serde_json::from_str::<serde_json::Value>(
                            &js_sys::JSON::stringify(&entry.get(1))?.as_string().unwrap(),
                        )?,
                    );
                }
                Response::from_json(&json!({
                    "tasks": tasks,
                    "alarm": self.state.storage().get_alarm().await?,
                    "now": js_sys::Date::now()
                }))
            }
            "/dispatcher/reconstruct" => {
                *self.dispatcher.borrow_mut() = Rc::new(RetainedTaskDispatcher::from_bindings(
                    self.state.storage(),
                    self.env.service("PROCESSOR")?,
                ));
                Response::empty()
            }
            "/dispatcher/alarm" => match dispatcher.alarm_worker().await {
                Ok(response) => Ok(response),
                Err(error) => Response::error(error.to_string(), 400),
            },
            "/dispatcher/corrupt" => {
                self.state
                    .storage()
                    .put_raw("scheduler", wasm_bindgen::JsValue::NULL)
                    .await?;
                Response::empty()
            }
            "/dispatcher/clear" => {
                self.state.storage().delete_all().await?;
                self.state.storage().delete_alarm().await?;
                Response::empty()
            }
            _ => dispatcher.fetch_worker(request).await,
        }
    }

    async fn alarm(&self) -> Result<Response> {
        let dispatcher = self.dispatcher.borrow().clone();
        dispatcher.alarm_worker().await
    }
}

#[durable_object]
pub struct BodySource {
    release: Arc<Semaphore>,
    fetched: AtomicUsize,
    drained: Arc<AtomicUsize>,
    next_action: RefCell<serde_json::Value>,
}

impl DurableObject for BodySource {
    fn new(_: State, _: Env) -> Self {
        Self {
            release: Arc::new(Semaphore::new(0)),
            fetched: AtomicUsize::new(0),
            drained: Arc::new(AtomicUsize::new(0)),
            next_action: RefCell::new(json!({"type": "done"})),
        }
    }

    async fn fetch(&self, mut request: Request) -> Result<Response> {
        match request.path().as_str() {
            "/source/action" => {
                *self.next_action.borrow_mut() = request.json().await?;
                Response::empty()
            }
            "/source/state" => Response::from_json(&json!({
                "fetched": self.fetched.load(Ordering::SeqCst),
                "drained": self.drained.load(Ordering::SeqCst),
            })),
            "/source/release" => {
                self.release.add_permits(1);
                Response::empty()
            }
            "/source/drain" => {
                self.release.add_permits(100);
                Response::empty()
            }
            "/process" | "/dispatch" => {
                let body: serde_json::Value = request.json().await?;
                let is_processor = request.path() == "/process";
                if is_processor
                    && serde_json::from_value::<bellows::cloudflare::TaskIdentity>(
                        body["task"].clone(),
                    )
                    .is_err()
                {
                    return Response::error("invalid task identity", 400);
                }
                let error = if is_processor {
                    &body["task"]["taskId"]
                } else {
                    let entries = dispatch_entries(&body)?;
                    if entries.is_empty() {
                        return Response::from_json(&json!({"ok": true}));
                    }
                    &body["tasks"][0]["task"]["taskId"]
                } == "error";
                let bytes = if request.path() == "/process" && !error {
                    json!({ "task": body["task"], "nextAction": *self.next_action.borrow() })
                        .to_string()
                        .into_bytes()
                } else if !error {
                    format!("{{\"ok\":true}}{}", " ".repeat(10_000)).into_bytes()
                } else {
                    // Larger than the diagnostic excerpt, spanning its UTF-16 boundary.
                    format!("{}🦀{}", "a".repeat(499), "z".repeat(10_000)).into_bytes()
                };
                self.fetched.fetch_add(1, Ordering::SeqCst);
                let release = self.release.clone();
                let drained = self.drained.clone();
                let stream = stream::unfold(
                    (false, release, drained, bytes),
                    |(sent, release, drained, bytes)| async move {
                        if sent {
                            drained.fetch_add(1, Ordering::SeqCst);
                            None
                        } else {
                            release.acquire().await.unwrap().forget();
                            Some((
                                Ok::<_, worker::Error>(bytes),
                                (true, release, drained, Vec::new()),
                            ))
                        }
                    },
                );
                Ok(Response::from_stream(stream)?.with_status(if error { 503 } else { 200 }))
            }
            _ => Response::error("not-found", 404),
        }
    }
}

fn dispatch_entries(body: &serde_json::Value) -> Result<Vec<bellows::cloudflare::DispatchTask>> {
    if body
        .as_object()
        .is_none_or(|object| object.len() != 1 || !object.contains_key("tasks"))
    {
        return Err(Error::RustError("invalid dispatch batch".into()));
    }
    Ok(serde_json::from_value(body["tasks"].clone())?)
}
