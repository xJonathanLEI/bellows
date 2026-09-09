//! Full PostgreSQL backend with listener-backed signaling and callback delivery.
//!
//! Producers that do not need callback delivery or subscriptions can use
//! [`super::postgres_publishing::PostgresPublishingBackend`] without a listener.

use std::{
    collections::{HashMap, hash_map::Entry as HashMapEntry},
    fmt::Formatter,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant, SystemTime},
};

use rand::RngExt;
use sqlx::postgres::PgListener;
use tokio::sync::{
    broadcast::{self, Sender as BroadcastSender},
    oneshot::Sender as OneshotSender,
    watch,
};
use tracing::warn;

use crate::backends::{
    Backend, BackendSignal, BackendSignalSubscription, ClaimTaskError, ClaimedTask, FailTaskError,
    FailedTask, FinishTaskError, FinishedTask, NewTaskAvailableSignalPayload, PublishTaskError,
    PublishedTask, RenewTaskError, RenewedTaskLease, SubscribeError, TaskExecutionBackend,
    TaskPublishingBackend,
};
use crate::{AwaitableTask, PublishActivationStrategy, TaskDefinition};

pub use super::postgres_common::PostgresBackendOptions;
use super::postgres_common::{NOTIFY_CHANNEL, NotificationPayload, unix_ms_to_instant};
use super::postgres_operations::PostgresTaskOperations;
pub use super::postgres_operations::{PostgresBackendError, initialize_postgres_schema};

const SIGNAL_CHANNEL_SIZE: usize = 1024;
const LISTENER_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Postgres-backed task registry with native `LISTEN`/`NOTIFY` signaling.
///
/// This type can be cheaply cloned.
///
/// Unlike the SQLite backend, task availability signals are emitted directly by Postgres via a
/// trigger on the task table, making this backend suitable for multi-process and distributed
/// deployments as long as all participants can reach the same database.
///
/// This full backend supports both [`crate::dispatcher::WorkerDispatcher`] and
/// [`crate::run_task_once`]. For execution without a dedicated listener or publishing/subscription
/// capabilities, use [`super::postgres_execution::PostgresExecutionBackend`] instead.
/// For plain publication, including callback-bearing definitions without a callback handle, use
/// [`super::postgres_publishing::PostgresPublishingBackend`].
#[derive(Clone)]
pub struct PostgresBackend {
    operations: PostgresTaskOperations,
    shared: Arc<Shared>,
}

struct Shared {
    signals: Mutex<HashMap<&'static str, BroadcastSender<BackendSignal>>>,
    callbacks: Mutex<HashMap<u64, Box<dyn CallbackSink>>>,
    shutdown_signal: watch::Sender<bool>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = self.shutdown_signal.send(true);
    }
}

impl std::fmt::Debug for PostgresBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresBackend")
            .field("operations", &self.operations)
            .finish_non_exhaustive()
    }
}

impl PostgresBackend {
    /// Connects to a Postgres database URL.
    ///
    /// This only establishes the connection pool and starts the background notification listener.
    /// Call [`Self::initialize`] separately to create the required tables, indexes, and triggers.
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        Self::connect_with_options(database_url, PostgresBackendOptions::default()).await
    }

    /// Connects with an optional, existing PostgreSQL schema.
    ///
    /// Schema names are validated before connecting; invalid names return
    /// [`sqlx::Error::Configuration`]. This does not create schemas or initialize any tables.
    pub async fn connect_with_options(
        database_url: &str,
        options: PostgresBackendOptions,
    ) -> Result<Self, sqlx::Error> {
        let operations = PostgresTaskOperations::connect(database_url, options).await?;
        let listener = connect_listener(database_url).await?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let shared = Arc::new(Shared {
            signals: Default::default(),
            callbacks: Default::default(),
            shutdown_signal: shutdown_tx,
        });

        let daemon = Daemon {
            database_url: database_url.to_owned(),
            listener: Some(listener),
            shared: Arc::downgrade(&shared),
            shutdown_signal: shutdown_rx,
        };

        tokio::spawn(daemon.run());

        Ok(Self { operations, shared })
    }

    /// Initializes the Postgres schema required by the backend.
    ///
    /// This operation is transactional and idempotent. A configured schema must already exist.
    /// The connection's search path is not changed outside the initialization transaction.
    pub async fn initialize(&self) -> Result<(), sqlx::Error> {
        self.operations.initialize().await
    }

    fn signal_for_task(&self, task_name: &'static str) -> BroadcastSender<BackendSignal> {
        let mut signals = self
            .shared
            .signals
            .lock()
            .expect("postgres backend signal registry mutex should not be poisoned");

        signals
            .entry(task_name)
            .or_insert_with(|| broadcast::channel(SIGNAL_CHANNEL_SIZE).0)
            .clone()
    }

    fn reserve_callback<T>(&self) -> (i64, tokio::sync::oneshot::Receiver<T>)
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let (callback_tx, callback_rx) = tokio::sync::oneshot::channel();
        let mut callbacks = self
            .shared
            .callbacks
            .lock()
            .expect("postgres backend callback registry mutex should not be poisoned");
        let mut rng = rand::rng();

        let callback_id = loop {
            let callback_id = rng.random::<i64>();
            if callback_id >= 0
                && let HashMapEntry::Vacant(entry) = callbacks.entry(callback_id as u64)
            {
                entry.insert(Box::new(TypedCallbackSink { tx: callback_tx }));
                break callback_id;
            }
        };

        (callback_id, callback_rx)
    }

    fn drop_reserved_callback(&self, callback_id: i64) {
        if let Ok(callback_id) = u64::try_from(callback_id) {
            self.shared
                .callbacks
                .lock()
                .expect("postgres backend callback registry mutex should not be poisoned")
                .remove(&callback_id);
        }
    }

    async fn publish_impl<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        callback_id: Option<i64>,
        available_from: Option<Instant>,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let result = self
            .operations
            .publish::<T>(payload, callback_id, available_from)
            .await;
        if result.is_err()
            && let Some(callback_id) = callback_id
        {
            self.drop_reserved_callback(callback_id);
        }
        result
    }
}

impl Backend for PostgresBackend {
    async fn subscribe<T>(&self) -> Result<BackendSignalSubscription<T>, SubscribeError>
    where
        T: TaskDefinition,
    {
        Ok(BackendSignalSubscription::new(
            self.signal_for_task(T::NAME).subscribe(),
        ))
    }

    async fn publish_awaitable<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<AwaitableTask<T::Callback>, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        let (callback_id, callback_rx) = self.reserve_callback::<T::Callback>();
        let published = self
            .publish_impl::<T>(payload, Some(callback_id), None)
            .await?;
        Ok(AwaitableTask::new(published.task_id, callback_rx))
    }
}

impl TaskPublishingBackend for PostgresBackend {
    async fn publish<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.publish_impl::<T>(payload, None, None).await
    }

    async fn publish_future<T>(
        &self,
        payload: <<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload,
        available_from: Instant,
    ) -> Result<PublishedTask, PublishTaskError>
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.publish_impl::<T>(payload, None, Some(available_from))
            .await
    }
}

impl TaskExecutionBackend for PostgresBackend {
    async fn claim_published<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<
        ClaimedTask<<<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload>,
        ClaimTaskError,
    >
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.operations
            .claim_published::<T>(worker_id, task_id, lease_expiration)
            .await
    }

    async fn claim_earliest_published<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<
        ClaimedTask<<<T as TaskDefinition>::Trigger as PublishActivationStrategy>::Payload>,
        ClaimTaskError,
    >
    where
        T: TaskDefinition,
        T::Trigger: PublishActivationStrategy,
    {
        self.operations
            .claim_earliest_published::<T>(worker_id, lease_expiration)
            .await
    }

    async fn claim_singleton<T>(
        &self,
        worker_id: u64,
        lease_expiration: Instant,
    ) -> Result<ClaimedTask<()>, ClaimTaskError>
    where
        T: TaskDefinition,
    {
        self.operations
            .claim_singleton::<T>(worker_id, lease_expiration)
            .await
    }

    async fn renew(
        &self,
        worker_id: u64,
        task_id: u64,
        lease_expiration: Instant,
    ) -> Result<RenewedTaskLease, RenewTaskError> {
        self.operations
            .renew(worker_id, task_id, lease_expiration)
            .await
    }

    async fn fail(
        &self,
        worker_id: u64,
        task_id: u64,
        available_from: Option<Instant>,
    ) -> Result<FailedTask, FailTaskError> {
        self.operations
            .fail(worker_id, task_id, available_from)
            .await
    }

    async fn finish<T>(
        &self,
        worker_id: u64,
        task_id: u64,
        callback_payload: T::Callback,
        available_from: Option<Instant>,
    ) -> Result<FinishedTask, FinishTaskError>
    where
        T: TaskDefinition,
    {
        self.operations
            .finish::<T>(worker_id, task_id, callback_payload, available_from)
            .await
    }
}

struct TypedCallbackSink<T> {
    tx: OneshotSender<T>,
}

trait CallbackSink: Send {
    fn send(self: Box<Self>, callback_payload_json: String);
}

impl<T> CallbackSink for TypedCallbackSink<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    fn send(self: Box<Self>, callback_payload_json: String) {
        if let Ok(callback_payload) = serde_json::from_str(&callback_payload_json) {
            let _ = self.tx.send(callback_payload);
        }
    }
}

#[derive(Debug)]
struct Daemon {
    database_url: String,
    listener: Option<PgListener>,
    shared: Weak<Shared>,
    shutdown_signal: watch::Receiver<bool>,
}

impl Daemon {
    async fn run(mut self) {
        while let EventLoopResult::Continue = self.event_loop().await {}
    }

    async fn event_loop(&mut self) -> EventLoopResult {
        if self.shared.strong_count() == 0 {
            return EventLoopResult::Exit;
        }

        if self.listener.is_none() {
            match connect_listener(&self.database_url).await {
                Ok(listener) => self.listener = Some(listener),
                Err(error) => {
                    warn!(
                        "postgres notification listener failed to reconnect: {}",
                        error
                    );
                    return self.wait_for_retry().await;
                }
            }
        }

        let listener = self
            .listener
            .as_mut()
            .expect("postgres listener should exist before waiting for notifications");

        tokio::select! {
            changed = self.shutdown_signal.changed() => {
                let _ = changed;
                EventLoopResult::Exit
            }
            notification = listener.recv() => {
                match notification {
                    Ok(notification) => self.handle_notification(notification.payload()),
                    Err(error) => {
                        warn!("postgres notification listener failed and will restart: {}", error);
                        self.listener = None;
                        self.wait_for_retry().await
                    }
                }
            }
        }
    }

    fn handle_notification(&self, payload_json: &str) -> EventLoopResult {
        let payload = match serde_json::from_str::<NotificationPayload>(payload_json) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    "failed to deserialize postgres notification payload {:?}: {}",
                    payload_json, error
                );
                return EventLoopResult::Continue;
            }
        };

        let Some(shared) = self.shared.upgrade() else {
            return EventLoopResult::Exit;
        };

        match payload {
            NotificationPayload::NewTaskAvailable {
                task_name,
                task_id,
                available_from_unix_ms,
            } => {
                let Ok(task_id) = u64::try_from(task_id) else {
                    warn!(
                        "received postgres notification with out-of-range task ID: {}",
                        task_id
                    );
                    return EventLoopResult::Continue;
                };

                let sender = {
                    let signals = shared
                        .signals
                        .lock()
                        .expect("postgres backend signal registry mutex should not be poisoned");
                    signals.get(task_name.as_str()).cloned()
                };

                if let Some(sender) = sender {
                    let available_from = available_from_unix_ms
                        .map(|unix_ms| unix_ms_to_instant(unix_ms, SystemTime::now()))
                        .unwrap_or_else(Instant::now);
                    let _ = sender.send(BackendSignal::NewTaskAvailable(
                        NewTaskAvailableSignalPayload {
                            task_id: Some(task_id),
                            available_from,
                        },
                    ));
                }
            }
            NotificationPayload::TaskCallback {
                task_name: _,
                callback_id,
                callback_payload_json,
            } => {
                let Ok(callback_id) = u64::try_from(callback_id) else {
                    warn!(
                        "received postgres callback notification with out-of-range callback ID: {}",
                        callback_id
                    );
                    return EventLoopResult::Continue;
                };

                if let Some(callback_sink) = shared
                    .callbacks
                    .lock()
                    .expect("postgres backend callback registry mutex should not be poisoned")
                    .remove(&callback_id)
                {
                    callback_sink.send(callback_payload_json);
                }
            }
        }

        EventLoopResult::Continue
    }

    async fn wait_for_retry(&mut self) -> EventLoopResult {
        tokio::select! {
            changed = self.shutdown_signal.changed() => {
                let _ = changed;
                EventLoopResult::Exit
            }
            _ = tokio::time::sleep(LISTENER_RETRY_DELAY) => {
                if self.shared.strong_count() == 0 {
                    EventLoopResult::Exit
                } else {
                    EventLoopResult::Continue
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventLoopResult {
    Continue,
    Exit,
}

async fn connect_listener(database_url: &str) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect(database_url).await?;
    listener.listen(NOTIFY_CHANNEL).await?;
    Ok(listener)
}
