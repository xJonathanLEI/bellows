#![cfg(target_arch = "wasm32")]

#[path = "../http.rs"]
mod http;
#[path = "../task.rs"]
mod task;

use bellows::{
    backends::postgres_publishing::PostgresBackendOptions,
    cloudflare::{
        RetainedTaskDispatcher,
        sdk::{Dispatcher, PostgresPublisher, PostgresPublisherConfig},
    },
};
use serde_json::json;
use task::{FullNamePayload, FullNameTask, GreetingPayload, GreetingTask};
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

#[event(fetch)]
pub async fn fetch(mut request: Request, env: Env, _ctx: Context) -> Result<Response> {
    let path = request.path();
    if path != "/tasks" && path != "/full-names" {
        return http::error(404, "not-found");
    }
    let body = match http::body(&mut request, &path).await {
        Ok(body) => body,
        Err(response) => return Ok(response),
    };
    let publication = if path == "/tasks" {
        let Some(name) = name(body.get("name")) else {
            return http::error(
                400,
                "body must be an object with a non-blank name of at most 200 characters",
            );
        };
        PostgresPublisher::<GreetingTask, _>::new(publisher_config)
            .publish(&env, GreetingPayload { name: name.into() })
            .await
    } else {
        let (Some(first_name), Some(last_name)) =
            (name(body.get("firstName")), name(body.get("lastName")))
        else {
            return http::error(
                400,
                "body must be an object with non-blank firstName and lastName of at most 200 characters each",
            );
        };
        PostgresPublisher::<FullNameTask, _>::new(publisher_config)
            .publish(
                &env,
                FullNamePayload {
                    first_name: first_name.into(),
                    last_name: last_name.into(),
                },
            )
            .await
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
    dispatcher: Dispatcher,
}

impl DurableObject for TaskDispatcher {
    fn new(state: State, env: Env) -> Self {
        Self {
            dispatcher: RetainedTaskDispatcher::from_bindings(
                state.storage(),
                env.service("PROCESSOR")
                    .expect("PROCESSOR service binding is required"),
            ),
        }
    }

    async fn fetch(&self, request: Request) -> Result<Response> {
        self.dispatcher.fetch_worker(request).await
    }

    async fn alarm(&self) -> Result<Response> {
        self.dispatcher.alarm_worker().await
    }
}
