use serde_json::{Value, json};
use worker::{Method, Request, Response};

pub fn error(status: u16, message: &str) -> worker::Result<Response> {
    Ok(Response::from_json(&json!({ "error": message }))?.with_status(status))
}

// The examples deliberately match the TypeScript route/media-type/JSON envelopes.
pub async fn body(request: &mut Request, path: &str) -> Result<Value, Response> {
    if request.path() != path {
        return Err(error(404, "not-found").unwrap());
    }
    if request.method() != Method::Post {
        let mut response = error(405, "method-not-allowed").unwrap();
        response.headers_mut().set("allow", "POST").unwrap();
        return Err(response);
    }
    if !request
        .headers()
        .get("content-type")
        .ok()
        .flatten()
        .is_some_and(|value| value.to_ascii_lowercase().contains("application/json"))
    {
        return Err(error(415, "content-type must be application/json").unwrap());
    }
    request
        .json()
        .await
        .map_err(|_| error(400, "invalid JSON").unwrap())
}
