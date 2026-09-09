//! Workers SDK adapters (wasm + `cloudflare` only).
//!
//! Store one [`Dispatcher`] per Durable Object, created with [`Dispatcher::from_bindings`],
//! and forward handlers to [`Dispatcher::fetch_worker`] and [`Dispatcher::alarm_worker`].

use http::{Request, Response};
use worker::send::{SendFuture, SendWrapper};

use super::{
    AlarmStorage, BoxDispatchError, DurableObjectNamespaceLike, ProcessorFetcher,
    RetainedTaskDispatcher, TextBody,
};

/// A Workers service binding adapted to [`ProcessorFetcher`].
pub struct Service(SendWrapper<worker::Fetcher>);

impl From<worker::Fetcher> for Service {
    fn from(value: worker::Fetcher) -> Self {
        Self(SendWrapper::new(value))
    }
}

/// Durable Object alarm storage using absolute timestamps.
pub struct Storage(SendWrapper<worker::Storage>);

impl From<worker::Storage> for Storage {
    fn from(value: worker::Storage) -> Self {
        Self(SendWrapper::new(value))
    }
}

/// The delegate stored by a Rust Durable Object.
pub type Dispatcher = RetainedTaskDispatcher<Storage, Service>;

impl Dispatcher {
    pub fn from_bindings(storage: worker::Storage, processor: worker::Fetcher) -> Self {
        Self::new(storage.into(), processor.into())
    }

    /// Forwards an SDK request, validating route and content type before reading the body.
    pub async fn fetch_worker(
        &self,
        mut request: worker::Request,
    ) -> worker::Result<worker::Response> {
        let mut builder = Request::builder()
            .method(request.method().as_ref())
            .uri(request.url()?.as_str());
        for (name, value) in request.headers().entries() {
            builder = builder.header(name, value);
        }
        let body: TextBody = Box::pin(SendFuture::new(async move {
            request.text().await.map_err(sdk_error)
        }));
        let request = builder
            .body(body)
            .map_err(|error| worker::Error::RustError(error.to_string()))?;
        let response = self.fetch(request).await;
        let headers = worker::Headers::new();
        for (name, value) in response.headers() {
            headers.set(
                name.as_str(),
                value.to_str().expect("dispatch headers are ASCII"),
            )?;
        }
        let status = response.status().as_u16();
        Ok(worker::Response::ok(response.into_body())?
            .with_status(status)
            .with_headers(headers))
    }

    /// Forwards an alarm and returns an empty response.
    pub async fn alarm_worker(&self) -> worker::Result<worker::Response> {
        self.alarm()
            .await
            .map_err(|error| worker::Error::RustError(error.to_string()))?;
        worker::Response::empty()
    }
}

impl DurableObjectNamespaceLike for worker::ObjectNamespace {
    type Stub = worker::Stub;

    fn get_by_name(&self, name: &str) -> Result<Self::Stub, BoxDispatchError> {
        self.get_by_name(name).map_err(sdk_error)
    }
}

impl ProcessorFetcher for worker::Stub {
    fn fetch(
        &self,
        request: Request<String>,
    ) -> impl Future<Output = Result<Response<TextBody>, BoxDispatchError>> + Send {
        SendFuture::new(async move {
            let response = self
                .fetch_with_request(outgoing_request(request)?)
                .await
                .map_err(sdk_error)?;
            incoming_response(response)
        })
    }
}

impl ProcessorFetcher for Service {
    fn fetch(
        &self,
        request: Request<String>,
    ) -> impl Future<Output = Result<Response<TextBody>, BoxDispatchError>> + Send {
        SendFuture::new(async move {
            let response = self
                .0
                .fetch_request(outgoing_request(request)?)
                .await
                .map_err(sdk_error)?;
            // Also works when an application's other dependencies enable the SDK's `http` feature.
            #[allow(clippy::useless_conversion)]
            let response = response.try_into().map_err(|error| -> BoxDispatchError {
                format!("Workers response conversion failed: {error}").into()
            })?;
            incoming_response(response)
        })
    }
}

impl AlarmStorage for Storage {
    fn get_alarm(&self) -> impl Future<Output = Result<Option<i64>, BoxDispatchError>> + Send {
        SendFuture::new(async move { self.0.get_alarm().await.map_err(sdk_error) })
    }

    fn set_alarm(
        &self,
        alarm_time: i64,
    ) -> impl Future<Output = Result<(), BoxDispatchError>> + Send {
        SendFuture::new(async move {
            // An i64 passed directly to set_alarm is an OFFSET in workers-rs, not a timestamp.
            let date = worker::js_sys::Date::new(&worker::wasm_bindgen::JsValue::from_f64(
                alarm_time as f64,
            ));
            self.0
                .set_alarm(worker::ScheduledTime::new(date))
                .await
                .map_err(sdk_error)
        })
    }
}

fn outgoing_request(request: Request<String>) -> Result<worker::Request, BoxDispatchError> {
    let headers = worker::Headers::new();
    for (name, value) in request.headers() {
        headers
            .set(name.as_str(), value.to_str()?)
            .map_err(sdk_error)?;
    }
    let mut init = worker::RequestInit::new();
    init.with_method(request.method().as_str().to_owned().into())
        .with_headers(headers);
    let url = request.uri().to_string();
    init.with_body(Some(request.into_body().into()));
    worker::Request::new_with_init(&url, &init).map_err(sdk_error)
}

fn incoming_response(response: worker::Response) -> Result<Response<TextBody>, BoxDispatchError> {
    let status = response.status_code();
    let body: TextBody = Box::pin(SendFuture::new(async move {
        // Use Fetch's text decoder (including UTF-8 replacement/BOM handling), rather than the
        // SDK Response::text's strict String::from_utf8. Read the entire stream before truncating.
        let response: worker::web_sys::Response = response.into();
        let text = response.text().map_err(|error| sdk_error(error.into()))?;
        let text = worker::wasm_bindgen_futures::JsFuture::from(text)
            .await
            .map_err(|error| sdk_error(error.into()))?;
        Ok(text
            .as_string()
            .expect("Response.text() resolves to a string"))
    }));
    Ok(Response::builder().status(status).body(body)?)
}

// Do not let JS-affine error values escape into the portable error/future contracts.
fn sdk_error(error: worker::Error) -> BoxDispatchError {
    error.to_string().into()
}
