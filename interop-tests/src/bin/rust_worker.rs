use bellows::{
    TaskResult, TaskSuccess, Worker, WorkerFactory, backends::postgres::PostgresBackend,
    dispatcher::WorkerDispatcher,
};
use interop_tests::{InteropEchoPayload, InteropEchoTask, InteropProcessEvent, emit_event};
use tokio::sync::mpsc::{self, UnboundedSender};

struct EchoWorkerFactory {
    processed_tx: UnboundedSender<(u64, String)>,
}

impl WorkerFactory for EchoWorkerFactory {
    type Worker = EchoWorker;

    fn build(&self, _worker_id: u64) -> Self::Worker {
        EchoWorker {
            processed_tx: self.processed_tx.clone(),
        }
    }
}

struct EchoWorker {
    processed_tx: UnboundedSender<(u64, String)>,
}

impl Worker for EchoWorker {
    type Task = InteropEchoTask;

    async fn process(self, task_id: u64, task_payload: InteropEchoPayload) -> TaskResult<String> {
        self.processed_tx
            .send((task_id, task_payload.name.clone()))
            .expect(
                "interop worker receiver should remain available until the first task finishes",
            );

        Ok(TaskSuccess::done(task_payload.name))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let database_url = parse_args();

    let backend = PostgresBackend::connect(&database_url)
        .await
        .expect("failed to connect worker to postgres backend");
    backend
        .initialize()
        .await
        .expect("failed to initialize postgres backend for worker");

    let (processed_tx, mut processed_rx) = mpsc::unbounded_channel();
    let dispatcher = WorkerDispatcher::new(backend, EchoWorkerFactory { processed_tx });
    let dispatcher_handle = dispatcher
        .launch()
        .await
        .expect("failed to launch interop worker dispatcher");

    emit_event(&InteropProcessEvent::Ready);

    let (task_id, name) = processed_rx
        .recv()
        .await
        .expect("interop worker should receive the first processed task");

    emit_event(&InteropProcessEvent::Processed { task_id, name });

    dispatcher_handle.drain().await;
}

fn parse_args() -> String {
    let mut args = std::env::args().skip(1);
    let database_url = args
        .next()
        .expect("expected database URL as the first argument");

    assert!(
        args.next().is_none(),
        "unexpected extra arguments passed to rust_worker"
    );

    database_url
}
