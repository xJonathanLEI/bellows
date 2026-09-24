#![cfg(target_arch = "wasm32")]

#[path = "../http.rs"]
mod http;
#[path = "../task.rs"]
mod task;

use bellows::{
    backends::postgres_publishing::PostgresBackendOptions,
    cloudflare::{
        RetainedTaskDispatcher,
        sdk::{
            Dispatcher, PostgresPublisher, PostgresPublisherConfig, PostgresSweeper,
            PostgresSweeperConfig, PostgresSweeperSingleton,
        },
    },
};
use serde_json::json;
use task::{
    FullNamePayload, FullNameTask, GreetingPayload, GreetingTask, SchedulingPayload,
    SchedulingTask, SingletonTask,
};
use worker::*;

// ECMAScript String.trim whitespace, including BOM (Rust's str::trim differs).
fn blank(name: &str) -> bool {
    name.chars().all(|ch| {
        matches!(ch, '\u{0009}'..='\u{000d}' | ' ' | '\u{00a0}' | '\u{1680}'
            | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}'
            | '\u{205f}' | '\u{3000}' | '\u{feff}')
    })
}

fn name(value: Option<&serde_json::Value>) -> Option<&str> {
    value
        .and_then(serde_json::Value::as_str)
        .filter(|name| !blank(name) && name.encode_utf16().count() <= 200)
}

fn publisher_config(env: &Env) -> Result<PostgresPublisherConfig> {
    Ok(PostgresPublisherConfig::new(
        env.hyperdrive("HYPERDRIVE")?.connection_string(),
        PostgresBackendOptions {
            schema: Some(env.var("BELLOWS_SCHEMA")?.to_string()),
        },
        env.durable_object("DISPATCHER")?,
    ))
}

fn sweeper_config(env: &Env) -> Result<PostgresSweeperConfig> {
    // Keep published-only scenarios free of perpetual singleton chains.
    let singletons = if env
        .var("BELLOWS_SINGLETON_BOOTSTRAP")
        .is_ok_and(|value| value.to_string() == "true")
    {
        vec![PostgresSweeperSingleton::new::<SingletonTask>()]
    } else {
        vec![]
    };
    Ok(PostgresSweeperConfig::new(
        env.hyperdrive("HYPERDRIVE")?.connection_string(),
        PostgresBackendOptions {
            schema: Some(env.var("BELLOWS_SCHEMA")?.to_string()),
        },
        env.durable_object("DISPATCHER")?,
    )
    .with_singletons(singletons))
}

// worker 0.8.5's #[event(scheduled)] discards the handler's Result. Export a real
// rejecting promise instead, without exposing the typed error's driver/candidate causes.
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn scheduled(
    _event: worker_sys::ScheduledEvent,
    env: Env,
    _ctx: worker_sys::ScheduleContext,
) -> js_sys::Promise {
    js_sys::futures::future_to_promise(std::panic::AssertUnwindSafe(async move {
        PostgresSweeper::new(sweeper_config)
            .sweep(&env)
            .await
            .map_err(|error| js_sys::Error::new(&error.to_string()))?;
        Ok(wasm_bindgen::JsValue::UNDEFINED)
    }))
}

#[event(fetch)]
pub async fn fetch(mut request: Request, env: Env, _ctx: Context) -> Result<Response> {
    let path = request.path();
    if path != "/tasks" && path != "/full-names" && path != "/scheduled" {
        return http::error(404, "not-found");
    }
    let body = match http::body(&mut request, &path).await {
        Ok(body) => body,
        Err(response) => return Ok(response),
    };
    let available_from = match request
        .url()?
        .query_pairs()
        .find(|(key, _)| key == "availableFromMs")
    {
        None => None,
        Some((_, value)) => match value.parse::<u64>() {
            Ok(at) if at <= 8_640_000_000_000_000 && value.bytes().all(|b| b.is_ascii_digit()) => {
                Some(task::deadline(at))
            }
            _ => return http::error(400, "invalid availability"),
        },
    };
    let publication = if path == "/scheduled" {
        let Ok(payload) = serde_json::from_value::<SchedulingPayload>(body) else {
            return http::error(400, "invalid scheduling payload");
        };
        if blank(&payload.name)
            || payload.name.encode_utf16().count() > 200
            || payload.available_from_ms > 8_640_000_000_000_000
        {
            return http::error(400, "invalid scheduling payload");
        }
        PostgresPublisher::<SchedulingTask, _>::new(publisher_config)
            .publish(&env, payload)
            .await
    } else if path == "/tasks" {
        let Some(name) = name(body.get("name")) else {
            return http::error(
                400,
                "body must be an object with a non-blank name of at most 200 characters",
            );
        };
        let publisher = PostgresPublisher::<GreetingTask, _>::new(publisher_config);
        let payload = GreetingPayload { name: name.into() };
        match available_from {
            Some(at) => publisher.publish_future(&env, payload, at).await,
            None => publisher.publish(&env, payload).await,
        }
    } else {
        let (Some(first_name), Some(last_name)) =
            (name(body.get("firstName")), name(body.get("lastName")))
        else {
            return http::error(
                400,
                "body must be an object with non-blank firstName and lastName of at most 200 characters each",
            );
        };
        let publisher = PostgresPublisher::<FullNameTask, _>::new(publisher_config);
        let payload = FullNamePayload {
            first_name: first_name.into(),
            last_name: last_name.into(),
        };
        match available_from {
            Some(at) => publisher.publish_future(&env, payload, at).await,
            None => publisher.publish(&env, payload).await,
        }
    };
    let receipt = match publication {
        Ok(receipt) => receipt,
        Err(error) => {
            return Ok(Response::from_json(&match error.receipt {
                None => json!({ "error": "task publication failed" }),
                Some(receipt) => json!({
                    "error": "task published, but dispatch acceptance was not confirmed",
                    "taskId": receipt.task_id
                }),
            })?
            .with_status(503));
        }
    };
    // Acceptance is not completion; the processor may still be running.
    let mut response = Response::ok(receipt.task_id)?.with_status(202);
    response
        .headers_mut()
        .set("content-type", "text/plain; charset=utf-8")?;
    response.headers_mut().set("cache-control", "no-store")?;
    Ok(response)
}

#[durable_object]
pub struct TaskDispatcher {
    dispatcher: std::cell::RefCell<std::rc::Rc<Dispatcher>>,
    state: State,
    env: Env,
}

impl DurableObject for TaskDispatcher {
    fn new(state: State, env: Env) -> Self {
        Self {
            dispatcher: std::cell::RefCell::new(std::rc::Rc::new(
                RetainedTaskDispatcher::from_bindings(
                    state.storage(),
                    env.service("PROCESSOR")
                        .expect("PROCESSOR service binding is required"),
                ),
            )),
            state,
            env,
        }
    }

    async fn fetch(&self, request: Request) -> Result<Response> {
        // Fixture-only storage inspection/reconstruction, not producer HTTP routes.
        match request.path().as_str() {
            "/__test/clear" => {
                self.state.storage().delete_all().await?;
                self.state.storage().delete_alarm().await?;
                return Response::empty();
            }
            "/__test/reconstruct" => {
                *self.dispatcher.borrow_mut() =
                    std::rc::Rc::new(RetainedTaskDispatcher::from_bindings(
                        self.state.storage(),
                        self.env.service("PROCESSOR")?,
                    ));
                return Response::empty();
            }
            "/__test/state" => {
                let storage = self.state.storage();
                let records = storage
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
                return Response::from_json(&json!({
                    "metadata": storage.get::<serde_json::Value>("scheduler").await?,
                    "tasks": tasks,
                    "alarm": storage.get_alarm().await?,
                    "now": js_sys::Date::now(),
                }));
            }
            _ => {}
        }
        let dispatcher = self.dispatcher.borrow().clone();
        dispatcher.fetch_worker(request).await
    }

    async fn alarm(&self) -> Result<Response> {
        let dispatcher = self.dispatcher.borrow().clone();
        dispatcher.alarm_worker().await
    }
}
