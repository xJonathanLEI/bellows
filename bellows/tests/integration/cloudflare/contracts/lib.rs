#![cfg(target_arch = "wasm32")]

mod postgres;
mod publisher;
mod runtime;

pub use publisher::PublisherReceiver;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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
    dispatcher: Dispatcher,
    storage: Storage,
}

impl DurableObject for ContractDispatcher {
    fn new(state: State, env: Env) -> Self {
        Self {
            dispatcher: RetainedTaskDispatcher::from_bindings(
                state.storage(),
                env.service("PROCESSOR").unwrap(),
            ),
            storage: state.storage(),
        }
    }

    async fn fetch(&self, request: Request) -> Result<Response> {
        match request.path().as_str() {
            "/dispatcher/state" => Response::from_json(&json!({
                "alarm": self.storage.get_alarm().await?,
                "now": js_sys::Date::now()
            })),
            "/dispatcher/alarm" => self.dispatcher.alarm_worker().await,
            _ => self.dispatcher.fetch_worker(request).await,
        }
    }

    async fn alarm(&self) -> Result<Response> {
        self.dispatcher.alarm_worker().await
    }
}

#[durable_object]
pub struct BodySource {
    release: Arc<Semaphore>,
    fetched: AtomicUsize,
    drained: Arc<AtomicUsize>,
}

impl DurableObject for BodySource {
    fn new(_: State, _: Env) -> Self {
        Self {
            release: Arc::new(Semaphore::new(0)),
            fetched: AtomicUsize::new(0),
            drained: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn fetch(&self, mut request: Request) -> Result<Response> {
        match request.path().as_str() {
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
                let error = body["taskId"] == "error";
                self.fetched.fetch_add(1, Ordering::SeqCst);
                let release = self.release.clone();
                let drained = self.drained.clone();
                let stream = stream::unfold(
                    (false, release, drained),
                    |(sent, release, drained)| async move {
                        if sent {
                            drained.fetch_add(1, Ordering::SeqCst);
                            None
                        } else {
                            release.acquire().await.unwrap().forget();
                            // Larger than the error excerpt, with a scalar spanning the UTF-16 boundary.
                            let bytes =
                                format!("{}🦀{}", "a".repeat(499), "z".repeat(10_000)).into_bytes();
                            Some((Ok::<_, worker::Error>(bytes), (true, release, drained)))
                        }
                    },
                );
                Ok(Response::from_stream(stream)?.with_status(if error { 503 } else { 200 }))
            }
            _ => Response::error("not-found", 404),
        }
    }
}
