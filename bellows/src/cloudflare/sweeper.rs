use std::{collections::HashSet, error::Error, fmt, future::Future, pin::Pin, task::Poll};

use futures_util::{Stream, StreamExt, future::poll_fn, stream::FuturesUnordered};

use super::{
    BoxDispatchError, DispatchIntent, DispatchTask, DurableObjectNamespaceLike, TaskIdentity,
    dispatch_tasks,
};
use crate::{SingletonTrigger, TaskDefinition};

/// Typed bootstrap registration. Names are validated per sweep before backend acquisition.
#[derive(Clone, Debug)]
pub struct PostgresSweeperSingleton {
    name: &'static str,
}

impl PostgresSweeperSingleton {
    pub fn new<T: TaskDefinition<Trigger = SingletonTrigger>>() -> Self {
        Self { name: T::NAME }
    }

    pub(super) fn name(&self) -> &str {
        self.name
    }
}

/// Settled candidate counts, partial on error. Accepted includes duplicate acknowledgements,
/// not completion or durable tracking. Discovered + bootstrap_candidates = accepted + failed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PostgresSweepReport {
    pub discovered: u64,
    /// Ensure entries submitted, not singleton executions or creations.
    pub bootstrap_candidates: u64,
    pub accepted: u64,
    pub failed: u64,
}

/// Exact database identity or name-only bootstrap entry retained for failure inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresSweepCandidate {
    Discovered {
        task_id: String,
        task_name: String,
        is_singleton: bool,
    },
    Bootstrap {
        task_name: String,
    },
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
    /// At most 100 entries from the failed batch; one for candidate validation failures.
    pub candidates: Vec<PostgresSweepCandidate>,
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
    pub singletons: Vec<String>,
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
    ) -> Result<Vec<(i64, String, bool)>, BoxDispatchError>;
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
        candidates: Vec<PostgresSweepCandidate>,
    ) {
        self.failure.get_or_insert(PostgresSweeperError {
            stage,
            cause,
            report: PostgresSweepReport::default(),
            candidates,
            backend_close_error: None,
        });
    }

    fn settle(&mut self, (count, candidates, result): DispatchResult) {
        match result {
            Ok(()) => self.report.accepted += count,
            Err(cause) => {
                self.report.failed += count;
                self.fail(PostgresSweeperStage::Dispatch, cause, candidates);
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

type DispatchResult = (
    u64,
    Vec<PostgresSweepCandidate>,
    Result<(), BoxDispatchError>,
);
type Pending<'a> = FuturesUnordered<Pin<Box<dyn Future<Output = DispatchResult> + 'a>>>;

fn submit<'a, N: DurableObjectNamespaceLike>(
    dispatcher: &'a N,
    tasks: Vec<DispatchTask>,
    mut candidates: Vec<PostgresSweepCandidate>,
    pending: &mut Pending<'a>,
) {
    if tasks.is_empty() {
        return;
    }
    candidates.truncate(100);
    pending.push(Box::pin(async move {
        let count = tasks.len() as u64;
        let result = dispatch_tasks(dispatcher, &tasks).await;
        (count, candidates, result)
    }));
}

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
            state.fail(Configuration, cause, vec![]);
            return state.finish();
        }
    };
    let mut names = HashSet::new();
    for name in &scope.singletons {
        let task = TaskIdentity::Singleton {
            task_name: name.clone(),
        };
        if let Err(cause) = task.validate_dispatch().and_then(|()| {
            if names.insert(name.clone()) {
                Ok(())
            } else {
                Err("duplicate singleton definition".into())
            }
        }) {
            state.fail(Configuration, cause, vec![]);
            return state.finish();
        }
    }
    let backend = match sweeper.acquire(scope.settings).await {
        Ok(backend) => backend,
        Err(cause) => {
            state.fail(Acquisition, cause, vec![]);
            return state.finish();
        }
    };
    let mut pending = Pending::new();
    let mut submitted = HashSet::new();
    match sweeper.begin(&backend).await {
        Err(cause) => state.fail(Discovery, cause, vec![]),
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
                        state.fail(Discovery, cause, vec![]);
                        break;
                    }
                    Ok(page) if page.is_empty() => {
                        let bootstrap: Vec<_> = scope
                            .singletons
                            .iter()
                            .filter(|name| !submitted.contains(*name))
                            .cloned()
                            .collect();
                        state.report.bootstrap_candidates += bootstrap.len() as u64;
                        let tasks = bootstrap
                            .iter()
                            .map(|name| DispatchTask {
                                task: TaskIdentity::Singleton {
                                    task_name: name.clone(),
                                },
                                intent: DispatchIntent::Ensure,
                            })
                            .collect();
                        let candidates = bootstrap
                            .into_iter()
                            .map(|task_name| PostgresSweepCandidate::Bootstrap { task_name })
                            .collect();
                        submit(&scope.dispatcher, tasks, candidates, &mut pending);
                        break;
                    }
                    Ok(page) => {
                        state.report.discovered += page.len() as u64;
                        let mut tasks = Vec::new();
                        let mut candidates = Vec::new();
                        for (id, name, is_singleton) in page {
                            cursor = Some(id);
                            let candidate = PostgresSweepCandidate::Discovered {
                                task_id: id.to_string(),
                                task_name: name.clone(),
                                is_singleton,
                            };
                            let task = if is_singleton {
                                TaskIdentity::Singleton { task_name: name }
                            } else {
                                TaskIdentity::Published {
                                    task_id: id.to_string(),
                                    task_name: name,
                                }
                            };
                            let valid = task.validate_dispatch().and_then(|()| {
                                if is_singleton || (1..=9_007_199_254_740_991).contains(&id) {
                                    Ok(())
                                } else {
                                    Err("unsupported task identity".into())
                                }
                            });
                            if let Err(cause) = valid {
                                state.report.failed += 1;
                                state.fail(Candidate, cause, vec![candidate]);
                                continue;
                            }
                            if is_singleton {
                                submitted.insert(task.name().to_owned());
                            }
                            tasks.push(DispatchTask {
                                task,
                                intent: DispatchIntent::Run,
                            });
                            candidates.push(candidate);
                        }
                        submit(&scope.dispatcher, tasks, candidates, &mut pending);
                    }
                }
            }
        }
    }
    if let Err(cause) = drive(sweeper.close(backend), &mut pending, &mut state).await {
        if let Some(error) = &mut state.failure {
            error.backend_close_error = Some(cause);
        } else {
            state.fail(BackendClose, cause, vec![]);
        }
    }
    while let Some(result) = pending.next().await {
        state.settle(result);
    }
    state.finish()
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
