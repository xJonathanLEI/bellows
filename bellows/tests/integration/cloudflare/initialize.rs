//! Native administrative bridge for the workerd fixture.

use std::{env, process::ExitCode, time::Duration};

use bellows::backends::postgres::initialize_postgres_schema;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match initialize().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            // Never print connection URLs or database errors that may contain credentials.
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

async fn initialize() -> Result<(), &'static str> {
    let arguments: Vec<_> = env::args().skip(1).collect();
    let [schema] = arguments.as_slice() else {
        return Err("Usage: cloudflare_initialize_postgres <existing-schema>");
    };
    let database_url = env::var("BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL")
        .map_err(|_| "Set BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL in the child environment")?;

    // The public initializer awaits both transactional initialization and connection close.
    tokio::time::timeout(
        Duration::from_secs(2),
        initialize_postgres_schema(&database_url, schema),
    )
    .await
    .map_err(|_| "Rust Cloudflare PostgreSQL initialization timed out")?
    .map_err(|_| {
        "Rust Cloudflare PostgreSQL initialization failed; check connectivity and schema privileges"
    })
}
