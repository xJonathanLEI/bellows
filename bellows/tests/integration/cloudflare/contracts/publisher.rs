//! Test-only publisher and controllable dispatch receiver, without a processor binding.

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bellows::{
    PublishTrigger, TaskDefinition,
    backends::postgres_publishing::PostgresBackendOptions,
    cloudflare::{
        dispatch_task,
        sdk::{PostgresPublisher, PostgresPublisherConfig, PostgresSweeper, PostgresSweeperConfig},
    },
    time::Instant,
};
use futures_util::stream;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use worker::*;

struct PublisherTask;
impl TaskDefinition for PublisherTask {
    const NAME: &str = "publisher_contract";
    type Callback = Vec<String>;
    type Trigger = PublishTrigger<(String, Vec<u32>)>;
}

// Like the producer, reject the actual event rather than losing Result in worker 0.8.5's macro.
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn scheduled(
    _event: worker_sys::ScheduledEvent,
    env: Env,
    _ctx: worker_sys::ScheduleContext,
) -> js_sys::Promise {
    js_sys::futures::future_to_promise(std::panic::AssertUnwindSafe(async move {
        PostgresSweeper::new(|env: &Env| {
            Ok(PostgresSweeperConfig::new(
                env.hyperdrive("HYPERDRIVE")?.connection_string(),
                PostgresBackendOptions {
                    schema: Some(env.var("BELLOWS_SCHEMA")?.to_string()),
                },
                env.durable_object("DISPATCHER")?,
            ))
        })
        .sweep(&env)
        .await
        .map_err(|error| js_sys::Error::new(&error.to_string()))?;
        Ok(wasm_bindgen::JsValue::UNDEFINED)
    }))
}

pub async fn fetch(mut request: Request, env: &Env) -> Result<Response> {
    match request.path().as_str() {
        "/publisher/publish" | "/publisher/publish-future" => {
            let available_from = if request.path() == "/publisher/publish-future" {
                let body: Value = request.json().await?;
                let at_ms = body["availableFromMs"].as_u64().unwrap();
                // Pair the millisecond wall and monotonic samples without crossing a clock tick.
                let (now_ms, now) = loop {
                    let before = Date::now().as_millis();
                    let now = Instant::now();
                    if before == Date::now().as_millis() {
                        break (before, now);
                    }
                };
                Some(now + Duration::from_millis(at_ms - now_ms))
            } else {
                None
            };
            let publisher = PostgresPublisher::<PublisherTask, _>::new(|env: &Env| {
                Ok(PostgresPublisherConfig::new(
                    env.hyperdrive("HYPERDRIVE")?.connection_string(),
                    PostgresBackendOptions {
                        schema: Some(env.var("BELLOWS_SCHEMA")?.to_string()),
                    },
                    env.durable_object("DISPATCHER")?,
                ))
            });
            let payload = ("hello \"🦀\"\n".into(), vec![1, 2, 3]);
            let result = match available_from {
                Some(deadline) => publisher.publish_future(env, payload, deadline).await,
                None => publisher.publish(env, payload).await,
            };
            match result {
                Ok(receipt) => Response::from_json(&json!({ "taskId": receipt.task_id })),
                Err(error) => Ok(Response::from_json(&json!({
                    "stage": error.stage.as_str(),
                    "receipt": error.receipt.as_ref().map(|receipt| json!({
                        "taskId": receipt.task_id
                    })),
                    "error": error.to_string(),
                }))?
                .with_status(503)),
            }
        }
        "/publisher/redispatch" => {
            let body: Value = request.json().await?;
            let task_id = body["taskId"].as_str().unwrap();
            dispatch_task(
                &env.durable_object("DISPATCHER")?,
                PublisherTask::NAME,
                task_id,
            )
            .await
            .map_err(|_| Error::from("contract redispatch failed"))?;
            Response::from_json(&json!({ "taskId": task_id }))
        }
        _ => {
            env.durable_object("DISPATCHER")?
                .get_by_name("global")?
                .fetch_with_request(request)
                .await
        }
    }
}

#[durable_object]
pub struct PublisherReceiver {
    status: Cell<u16>,
    statuses: RefCell<VecDeque<u16>>,
    dispatches: RefCell<Vec<Value>>,
    requests: Cell<usize>,
    drained: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
}

impl DurableObject for PublisherReceiver {
    fn new(_: State, _: Env) -> Self {
        Self {
            status: Cell::new(200),
            statuses: RefCell::new(VecDeque::new()),
            dispatches: RefCell::new(Vec::new()),
            requests: Cell::new(0),
            drained: Arc::new(AtomicUsize::new(0)),
            release: Arc::new(Semaphore::new(0)),
        }
    }

    async fn fetch(&self, mut request: Request) -> Result<Response> {
        match request.path().as_str() {
            "/publisher/state" => Response::from_json(&json!({
                "dispatches": *self.dispatches.borrow(),
                "drained": self.drained.load(Ordering::SeqCst),
                "requests": self.requests.get(),
            })),
            "/publisher/response" => {
                let body: Value = request.json().await?;
                self.status.set(body["status"].as_u64().unwrap() as u16);
                *self.statuses.borrow_mut() = body["statuses"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|status| status.as_u64().unwrap() as u16)
                    .collect();
                Response::empty()
            }
            "/publisher/release" => {
                self.release.add_permits(1);
                Response::empty()
            }
            "/publisher/drain" => {
                self.release.close();
                Response::empty()
            }
            "/dispatch" => {
                let entries = super::dispatch_entries(&request.json().await?)?;
                self.requests.set(self.requests.get() + 1);
                self.dispatches.borrow_mut().extend(
                    entries
                        .into_iter()
                        .map(|entry| serde_json::to_value(entry).unwrap()),
                );
                let status = self
                    .statuses
                    .borrow_mut()
                    .pop_front()
                    .unwrap_or(self.status.get());
                let release = self.release.clone();
                let drained = self.drained.clone();
                let stream = stream::unfold(
                    (0, release, drained),
                    move |(part, release, drained)| async move {
                        let bytes = match part {
                            // Send more than the diagnostic excerpt before withholding the tail.
                            0 if status == 200 => {
                                format!("{{\"ok\":true}}{}", " ".repeat(10_000)).into_bytes()
                            }
                            0 => format!("fixture-secret:{}", "a".repeat(10_000)).into_bytes(),
                            1 => {
                                if let Ok(permit) = release.acquire().await {
                                    permit.forget();
                                }
                                if status == 200 {
                                    b"\n".to_vec()
                                } else {
                                    "🦀:complete".as_bytes().to_vec()
                                }
                            }
                            _ => {
                                drained.fetch_add(1, Ordering::SeqCst);
                                return None;
                            }
                        };
                        Some((Ok::<_, Error>(bytes), (part + 1, release, drained)))
                    },
                );
                Ok(Response::from_stream(stream)?.with_status(status))
            }
            _ => Response::error("not-found", 404),
        }
    }
}
