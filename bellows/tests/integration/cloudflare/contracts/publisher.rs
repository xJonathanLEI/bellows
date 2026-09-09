//! Test-only publisher and controllable dispatch receiver, without a processor binding.

use std::{
    cell::{Cell, RefCell},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bellows::{
    PublishTrigger, TaskDefinition,
    backends::postgres_publishing::PostgresBackendOptions,
    cloudflare::{
        dispatch_task,
        sdk::{PostgresPublisher, PostgresPublisherConfig},
    },
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

pub async fn fetch(mut request: Request, env: &Env) -> Result<Response> {
    match request.path().as_str() {
        "/publisher/publish" => {
            let publisher = PostgresPublisher::<PublisherTask, _>::new(|env: &Env| {
                Ok(PostgresPublisherConfig::new(
                    env.hyperdrive("HYPERDRIVE")?.connection_string(),
                    PostgresBackendOptions {
                        schema: Some(env.var("BELLOWS_SCHEMA")?.to_string()),
                    },
                    env.durable_object("DISPATCHER")?,
                ))
            });
            match publisher
                .publish(env, ("hello \"🦀\"\n".into(), vec![1, 2, 3]))
                .await
            {
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
            dispatch_task(&env.durable_object("DISPATCHER")?, task_id)
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
    dispatches: RefCell<Vec<Value>>,
    drained: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
}

impl DurableObject for PublisherReceiver {
    fn new(_: State, _: Env) -> Self {
        Self {
            status: Cell::new(200),
            dispatches: RefCell::new(Vec::new()),
            drained: Arc::new(AtomicUsize::new(0)),
            release: Arc::new(Semaphore::new(0)),
        }
    }

    async fn fetch(&self, mut request: Request) -> Result<Response> {
        match request.path().as_str() {
            "/publisher/state" => Response::from_json(&json!({
                "dispatches": *self.dispatches.borrow(),
                "drained": self.drained.load(Ordering::SeqCst),
            })),
            "/publisher/response" => {
                let body: Value = request.json().await?;
                self.status.set(body["status"].as_u64().unwrap() as u16);
                Response::empty()
            }
            "/publisher/release" => {
                self.release.add_permits(1);
                Response::empty()
            }
            "/publisher/drain" => {
                self.release.add_permits(100);
                Response::empty()
            }
            "/dispatch" => {
                let body = request.json().await?;
                self.dispatches.borrow_mut().push(body);
                let release = self.release.clone();
                let drained = self.drained.clone();
                let stream = stream::unfold(
                    (0, release, drained),
                    |(part, release, drained)| async move {
                        let bytes = match part {
                            // Send more than the diagnostic excerpt before withholding the tail.
                            0 => format!("fixture-secret:{}", "a".repeat(10_000)).into_bytes(),
                            1 => {
                                release.acquire().await.unwrap().forget();
                                "🦀:complete".as_bytes().to_vec()
                            }
                            _ => {
                                drained.fetch_add(1, Ordering::SeqCst);
                                return None;
                            }
                        };
                        Some((Ok::<_, Error>(bytes), (part + 1, release, drained)))
                    },
                );
                Ok(Response::from_stream(stream)?.with_status(self.status.get()))
            }
            _ => Response::error("not-found", 404),
        }
    }
}
