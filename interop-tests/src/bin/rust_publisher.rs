use bellows::{Backend, backends::postgres::PostgresBackend};
use interop_tests::{InteropEchoPayload, InteropEchoTask, InteropProcessEvent, emit_event};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let [database_url, name] = parse_args();

    let backend = PostgresBackend::connect(&database_url)
        .await
        .expect("failed to connect publisher to postgres backend");
    backend
        .initialize()
        .await
        .expect("failed to initialize postgres backend for publisher");

    let awaitable = backend
        .publish_awaitable::<InteropEchoTask>(InteropEchoPayload { name: name.clone() })
        .await
        .expect("failed to publish awaitable interop echo task");

    let task_id = awaitable.task_id();

    emit_event(&InteropProcessEvent::Published {
        task_id,
        name: name.clone(),
    });

    let awaited_name = awaitable
        .wait()
        .await
        .expect("failed while waiting for interop echo task callback");

    emit_event(&InteropProcessEvent::Awaited {
        task_id,
        name: awaited_name,
    });
}

fn parse_args() -> [String; 2] {
    let mut args = std::env::args().skip(1);
    let database_url = args
        .next()
        .expect("expected database URL as the first argument");
    let name = args
        .next()
        .expect("expected task name as the second argument");

    assert!(
        args.next().is_none(),
        "unexpected extra arguments passed to rust_publisher"
    );

    [database_url, name]
}
