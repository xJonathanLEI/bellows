use std::{error::Error, fmt, future::Future, pin::Pin, task::Poll};

use futures_util::{Stream, StreamExt, future::poll_fn, stream::FuturesUnordered};

use super::{BoxDispatchError, DurableObjectNamespaceLike, dispatch_task};

/// Settled candidate counts, partial on error. Accepted includes duplicate acknowledgements,
/// not completion or durable tracking.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PostgresSweepReport {
    pub discovered: u64,
    pub accepted: u64,
    pub failed: u64,
}

/// Exact stored identity retained for deliberate failure inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresSweepCandidate {
    pub task_id: String,
    pub task_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresSweeperStage {
    Configuration,
    Acquisition,
    Discovery,
    Candidate,
    Dispatch,
    BackendClose,
}

impl PostgresSweeperStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::Acquisition => "acquisition",
            Self::Discovery => "discovery",
            Self::Candidate => "candidate",
            Self::Dispatch => "dispatch",
            Self::BackendClose => "backend-close",
        }
    }
}

impl fmt::Display for PostgresSweeperStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// First failure with settled counts and any later close error.
///
/// Only `Display` is sanitized; do not automatically log causes, candidate identities, or `Debug`.
#[derive(Debug)]
pub struct PostgresSweeperError {
    pub stage: PostgresSweeperStage,
    pub cause: BoxDispatchError,
    pub report: PostgresSweepReport,
    pub candidate: Option<PostgresSweepCandidate>,
    /// A later close failure never replaces the first observed cause.
    pub backend_close_error: Option<BoxDispatchError>,
}

impl fmt::Display for PostgresSweeperError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL sweeper failed at {}", self.stage)
    }
}

impl Error for PostgresSweeperError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

pub(super) struct Scope<S, N> {
    pub settings: S,
    pub dispatcher: N,
}

// Associated window/backend keep native orchestration independent of PostgreSQL features.
pub(super) trait Sweeper {
    type Settings;
    type Backend;
    type Window;
    type Namespace: DurableObjectNamespaceLike;

    fn configure(&self) -> Result<Scope<Self::Settings, Self::Namespace>, BoxDispatchError>;
    async fn acquire(&self, settings: Self::Settings) -> Result<Self::Backend, BoxDispatchError>;
    async fn begin(&self, backend: &Self::Backend) -> Result<Self::Window, BoxDispatchError>;
    async fn page(
        &self,
        backend: &Self::Backend,
        window: &Self::Window,
        cursor: Option<i64>,
    ) -> Result<Vec<(i64, String)>, BoxDispatchError>;
    async fn close(&self, backend: Self::Backend) -> Result<(), BoxDispatchError>;
}

#[derive(Default)]
struct State {
    report: PostgresSweepReport,
    failure: Option<PostgresSweeperError>,
}

impl State {
    fn fail(
        &mut self,
        stage: PostgresSweeperStage,
        cause: BoxDispatchError,
        candidate: Option<PostgresSweepCandidate>,
    ) {
        self.failure.get_or_insert(PostgresSweeperError {
            stage,
            cause,
            report: PostgresSweepReport::default(),
            candidate,
            backend_close_error: None,
        });
    }

    fn settle(&mut self, (candidate, result): DispatchResult) {
        match result {
            Ok(()) => self.report.accepted += 1,
            Err(cause) => {
                self.report.failed += 1;
                self.fail(PostgresSweeperStage::Dispatch, cause, Some(candidate));
            }
        }
    }

    fn finish(self) -> Result<PostgresSweepReport, PostgresSweeperError> {
        match self.failure {
            Some(mut error) => {
                error.report = self.report;
                Err(error)
            }
            None => Ok(self.report),
        }
    }
}

type DispatchResult = (PostgresSweepCandidate, Result<(), BoxDispatchError>);
type Pending<'a> = FuturesUnordered<Pin<Box<dyn Future<Output = DispatchResult> + 'a>>>;

// Poll dispatches even while a query or shutdown is pending. Merely collecting futures is lazy.
async fn drive<F: Future>(operation: F, pending: &mut Pending<'_>, state: &mut State) -> F::Output {
    let mut operation = std::pin::pin!(operation);
    poll_fn(|cx| {
        while let Poll::Ready(Some(result)) = Pin::new(&mut *pending).poll_next(cx) {
            state.settle(result);
        }
        operation.as_mut().poll(cx)
    })
    .await
}

pub(super) async fn sweep<S: Sweeper>(
    sweeper: &S,
) -> Result<PostgresSweepReport, PostgresSweeperError> {
    use PostgresSweeperStage::*;
    let mut state = State::default();
    let scope = match sweeper.configure() {
        Ok(scope) => scope,
        Err(cause) => {
            state.fail(Configuration, cause, None);
            return state.finish();
        }
    };
    let backend = match sweeper.acquire(scope.settings).await {
        Ok(backend) => backend,
        Err(cause) => {
            state.fail(Acquisition, cause, None);
            return state.finish();
        }
    };
    let mut pending = Pending::new();
    match sweeper.begin(&backend).await {
        Err(cause) => state.fail(Discovery, cause, None),
        Ok(window) => {
            let mut cursor = None;
            loop {
                let page = drive(
                    sweeper.page(&backend, &window, cursor),
                    &mut pending,
                    &mut state,
                )
                .await;
                match page {
                    Err(cause) => {
                        state.fail(Discovery, cause, None);
                        break;
                    }
                    Ok(page) if page.is_empty() => break,
                    Ok(page) => {
                        state.report.discovered += page.len() as u64;
                        for (id, name) in page {
                            cursor = Some(id);
                            let candidate = PostgresSweepCandidate {
                                task_id: id.to_string(),
                                task_name: name,
                            };
                            if !(1..=9_007_199_254_740_991).contains(&id)
                                || candidate.task_name.is_empty()
                            {
                                state.report.failed += 1;
                                state.fail(
                                    Candidate,
                                    "unsupported task identity".into(),
                                    Some(candidate),
                                );
                                continue;
                            }
                            let dispatcher = &scope.dispatcher;
                            pending.push(Box::pin(async move {
                                let result = dispatch_task(
                                    dispatcher,
                                    &candidate.task_name,
                                    &candidate.task_id,
                                )
                                .await;
                                (candidate, result)
                            }));
                        }
                    }
                }
            }
        }
    }
    if let Err(cause) = drive(sweeper.close(backend), &mut pending, &mut state).await {
        if let Some(error) = &mut state.failure {
            error.backend_close_error = Some(cause);
        } else {
            state.fail(BackendClose, cause, None);
        }
    }
    while let Some(result) = pending.next().await {
        state.settle(result);
    }
    state.finish()
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
