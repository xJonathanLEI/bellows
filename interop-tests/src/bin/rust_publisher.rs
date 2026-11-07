use bellows::{
    Backend,
    backends::{
        postgres::PostgresBackend,
        postgres_publishing::{
            PostgresBackendOptions, PostgresEncodedRowsPublicationOptions,
            PostgresEncodedRowsSource, prepare_postgres_publication_from_encoded_rows,
        },
    },
};
use interop_tests::{InteropEchoPayload, InteropEchoTask, InteropProcessEvent, emit_event};
use sqlx::{Connection, PgConnection};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (database_url, name, fragment_mode) = parse_args();

    let backend = PostgresBackend::connect(&database_url)
        .await
        .expect("failed to connect publisher to postgres backend");
    backend
        .initialize()
        .await
        .expect("failed to initialize postgres backend for publisher");

    if fragment_mode {
        let fragment = prepare_postgres_publication_from_encoded_rows::<InteropEchoTask>(
            PostgresEncodedRowsPublicationOptions {
                backend: PostgresBackendOptions::default(),
                source: PostgresEncodedRowsSource {
                    relation: "inputs".into(),
                    payload_column: "payload".into(),
                },
                parameter_offset: 1,
            },
        )
        .expect("failed to prepare publication fragment");
        let sql = format!(
            "WITH inputs AS (SELECT json_build_object('name', $1::text)::text AS payload),
             published AS ({}) SELECT task_id FROM published",
            fragment.sql(),
        );
        let mut connection = PgConnection::connect(&database_url).await.unwrap();
        let mut transaction = connection.begin().await.unwrap();
        let ids: Vec<String> = sqlx::query_scalar(&sql)
            .bind(&name)
            .bind(&fragment.parameters()[0])
            .fetch_all(&mut *transaction)
            .await
            .unwrap();
        let receipts = fragment.read_receipts(&ids).unwrap();
        assert_eq!(receipts.len(), 1);
        transaction.commit().await.unwrap();
        connection.close().await.unwrap();
        emit_event(&InteropProcessEvent::Published {
            task_id: receipts[0].task_id,
            name,
        });
        // Fragment publication deliberately registers no callback.
        return;
    }

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

fn parse_args() -> (String, String, bool) {
    let mut args = std::env::args().skip(1);
    let database_url = args
        .next()
        .expect("expected database URL as the first argument");
    let name = args
        .next()
        .expect("expected task name as the second argument");

    let fragment_mode = match args.next().as_deref() {
        None => false,
        Some("fragment") => true,
        Some(_) => panic!("expected optional fragment publication mode"),
    };
    assert!(
        args.next().is_none(),
        "unexpected extra arguments passed to rust_publisher"
    );

    (database_url, name, fragment_mode)
}
