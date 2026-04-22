use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use interop_tests::InteropProcessEvent;
use sqlx::{Connection, Executor, PgConnection};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const PROCESS_TIMEOUT: Duration = Duration::from_secs(10);
const ADMIN_DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432/postgres";

#[tokio::test]
async fn rust_publisher_can_drive_rust_worker() {
    run_publish_to_worker_scenario(PublisherKind::Rust, WorkerKind::Rust).await;
}

#[tokio::test]
async fn rust_publisher_can_drive_typescript_worker() {
    run_publish_to_worker_scenario(PublisherKind::Rust, WorkerKind::TypeScript).await;
}

#[tokio::test]
async fn typescript_publisher_can_drive_rust_worker() {
    run_publish_to_worker_scenario(PublisherKind::TypeScript, WorkerKind::Rust).await;
}

#[tokio::test]
async fn typescript_publisher_can_drive_typescript_worker() {
    run_publish_to_worker_scenario(PublisherKind::TypeScript, WorkerKind::TypeScript).await;
}

async fn run_publish_to_worker_scenario(publisher: PublisherKind, worker: WorkerKind) {
    let database = TestDatabase::new(&format!(
        "{}_publisher_to_{}_worker",
        publisher.label(),
        worker.label()
    ))
    .await;
    let mut worker_process = None;
    let mut publisher_process = None;

    let result = async {
        let task_name = format!(
            "{}-{}-{}",
            publisher.label(),
            worker.label(),
            unique_suffix()
        );

        worker_process = Some(spawn_worker(worker, database.url())?);
        worker_process
            .as_mut()
            .expect("worker process should be available")
            .expect_ready()
            .await?;

        publisher_process = Some(spawn_publisher(publisher, database.url(), &task_name)?);

        let published = publisher_process
            .as_mut()
            .expect("publisher process should be available")
            .expect_published()
            .await?;
        let processed = worker_process
            .as_mut()
            .expect("worker process should be available")
            .expect_processed()
            .await?;
        let awaited = publisher_process
            .as_mut()
            .expect("publisher process should be available")
            .expect_awaited()
            .await?;

        if published.task_id != processed.task_id {
            return Err(format!(
                "publisher task id {} did not match worker task id {}",
                published.task_id, processed.task_id
            ));
        }

        if published.task_id != awaited.task_id {
            return Err(format!(
                "publisher task id {} did not match awaited task id {}",
                published.task_id, awaited.task_id
            ));
        }

        if published.name != task_name {
            return Err(format!(
                "publisher reported unexpected task name: expected {task_name}, got {}",
                published.name
            ));
        }

        if processed.name != task_name {
            return Err(format!(
                "worker reported unexpected task name: expected {task_name}, got {}",
                processed.name
            ));
        }

        if awaited.name != task_name {
            return Err(format!(
                "awaitable callback reported unexpected task name: expected {task_name}, got {}",
                awaited.name
            ));
        }

        publisher_process
            .take()
            .expect("publisher process should be available")
            .wait_for_success()
            .await?;
        worker_process
            .take()
            .expect("worker process should be available")
            .wait_for_success()
            .await?;
        Ok(())
    }
    .await;

    if let Some(process) = publisher_process {
        process.terminate().await;
    }

    if let Some(process) = worker_process {
        process.terminate().await;
    }

    database.cleanup().await;

    if let Err(message) = result {
        panic!("{message}");
    }
}

#[derive(Clone, Copy)]
enum PublisherKind {
    Rust,
    TypeScript,
}

impl PublisherKind {
    fn label(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
        }
    }
}

#[derive(Clone, Copy)]
enum WorkerKind {
    Rust,
    TypeScript,
}

impl WorkerKind {
    fn label(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
        }
    }
}

struct PublishedEvent {
    task_id: u64,
    name: String,
}

struct ProcessedEvent {
    task_id: u64,
    name: String,
}

struct AwaitedEvent {
    task_id: u64,
    name: String,
}

struct InteropProcess {
    label: String,
    child: Child,
    stdout: tokio::io::Lines<BufReader<ChildStdout>>,
    stderr_task: Option<JoinHandle<String>>,
    stdout_lines: Vec<String>,
}

impl InteropProcess {
    async fn expect_ready(&mut self) -> Result<(), String> {
        match self.next_event().await? {
            InteropProcessEvent::Ready => Ok(()),
            other => Err(format!(
                "{} emitted {:?} instead of a ready event\n{}",
                self.label,
                other,
                self.format_output()
            )),
        }
    }

    async fn expect_published(&mut self) -> Result<PublishedEvent, String> {
        match self.next_event().await? {
            InteropProcessEvent::Published { task_id, name } => {
                Ok(PublishedEvent { task_id, name })
            }
            other => Err(format!(
                "{} emitted {:?} instead of a published event\n{}",
                self.label,
                other,
                self.format_output()
            )),
        }
    }

    async fn expect_processed(&mut self) -> Result<ProcessedEvent, String> {
        match self.next_event().await? {
            InteropProcessEvent::Processed { task_id, name } => {
                Ok(ProcessedEvent { task_id, name })
            }
            other => Err(format!(
                "{} emitted {:?} instead of a processed event\n{}",
                self.label,
                other,
                self.format_output()
            )),
        }
    }

    async fn expect_awaited(&mut self) -> Result<AwaitedEvent, String> {
        match self.next_event().await? {
            InteropProcessEvent::Awaited { task_id, name } => Ok(AwaitedEvent { task_id, name }),
            other => Err(format!(
                "{} emitted {:?} instead of an awaited event\n{}",
                self.label,
                other,
                self.format_output()
            )),
        }
    }

    async fn wait_for_success(mut self) -> Result<(), String> {
        let exit_status = timeout(PROCESS_TIMEOUT, self.child.wait())
            .await
            .map_err(|_| {
                format!(
                    "timed out waiting for {} to exit\n{}",
                    self.label,
                    self.format_output()
                )
            })?
            .map_err(|error| {
                format!(
                    "failed to wait for {} exit: {error}\n{}",
                    self.label,
                    self.format_output()
                )
            })?;
        let stderr = self.join_stderr().await?;

        if exit_status.success() {
            Ok(())
        } else {
            Err(format!(
                "{} exited unsuccessfully with status {exit_status}\nstdout:\n{}\nstderr:\n{}",
                self.label,
                self.stdout_lines.join("\n"),
                stderr
            ))
        }
    }

    async fn terminate(mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.start_kill();
            let _ = timeout(PROCESS_TIMEOUT, self.child.wait()).await;
        }

        let _ = self.join_stderr().await;
    }

    async fn next_event(&mut self) -> Result<InteropProcessEvent, String> {
        let next_line = timeout(PROCESS_TIMEOUT, self.stdout.next_line())
            .await
            .map_err(|_| {
                format!(
                    "timed out waiting for {} output\n{}",
                    self.label,
                    self.format_output()
                )
            })?
            .map_err(|error| {
                format!(
                    "failed to read {} stdout: {error}\n{}",
                    self.label,
                    self.format_output()
                )
            })?;

        let Some(line) = next_line else {
            let status = self.child.wait().await.map_err(|error| {
                format!(
                    "failed waiting for {} after stdout closed: {error}",
                    self.label
                )
            })?;
            let stderr = self.join_stderr().await?;
            return Err(format!(
                "{} exited before producing the expected event with status {status}\nstdout:\n{}\nstderr:\n{}",
                self.label,
                self.stdout_lines.join("\n"),
                stderr
            ));
        };

        self.stdout_lines.push(line.clone());

        serde_json::from_str(&line).map_err(|error| {
            format!(
                "failed to parse {} event from line {line:?}: {error}\n{}",
                self.label,
                self.format_output()
            )
        })
    }

    async fn join_stderr(&mut self) -> Result<String, String> {
        let stderr_task = self
            .stderr_task
            .take()
            .ok_or_else(|| format!("{} stderr task was already joined", self.label))?;

        stderr_task
            .await
            .map_err(|error| format!("failed to join {} stderr task: {error}", self.label))
    }

    fn format_output(&self) -> String {
        format!("captured stdout:\n{}", self.stdout_lines.join("\n"))
    }
}

fn spawn_worker(worker: WorkerKind, database_url: &str) -> Result<InteropProcess, String> {
    let (label, command) = match worker {
        WorkerKind::Rust => {
            let mut command = Command::new(env!("CARGO_BIN_EXE_rust_worker"));
            command.arg(database_url);
            ("rust worker", command)
        }
        WorkerKind::TypeScript => {
            let mut command = Command::new("node");
            command.arg(typescript_worker_path()).arg(database_url);
            ("typescript worker", command)
        }
    };

    spawn_process(label, command)
}

fn spawn_publisher(
    publisher: PublisherKind,
    database_url: &str,
    task_name: &str,
) -> Result<InteropProcess, String> {
    let (label, command) = match publisher {
        PublisherKind::Rust => {
            let mut command = Command::new(env!("CARGO_BIN_EXE_rust_publisher"));
            command.args([database_url, task_name]);
            ("rust publisher", command)
        }
        PublisherKind::TypeScript => {
            let mut command = Command::new("node");
            command
                .arg(typescript_publisher_path())
                .args([database_url, task_name]);
            ("typescript publisher", command)
        }
    };

    spawn_process(label, command)
}

fn spawn_process(label: &str, mut command: Command) -> Result<InteropProcess, String> {
    command
        .current_dir(workspace_root())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|error| format!("failed to spawn {label}: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("{label} stdout was not piped"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("{label} stderr was not piped"))?;

    Ok(InteropProcess {
        label: label.to_owned(),
        child,
        stdout: BufReader::new(stdout).lines(),
        stderr_task: Some(tokio::spawn(read_stream(stderr))),
        stdout_lines: Vec::new(),
    })
}

async fn read_stream<T>(stream: T) -> String
where
    T: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    let mut output = Vec::new();

    while let Ok(Some(line)) = lines.next_line().await {
        output.push(line);
    }

    output.join("\n")
}

fn typescript_publisher_path() -> PathBuf {
    workspace_root().join("bellows-ts/dist/interop/publisher.js")
}

fn typescript_worker_path() -> PathBuf {
    workspace_root().join("bellows-ts/dist/interop/worker.js")
}

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("interop-tests crate should be nested inside the bellows workspace")
}

struct TestDatabase {
    database_name: String,
    url: String,
}

impl TestDatabase {
    async fn new(test_name: &str) -> Self {
        let database_name = format!("bellows_{}_{}", test_name, unique_suffix());

        let mut admin = PgConnection::connect(ADMIN_DATABASE_URL)
            .await
            .expect("failed to connect to the postgres admin database");

        admin
            .execute(format!(r#"CREATE DATABASE "{}""#, database_name).as_str())
            .await
            .expect("failed to create temporary postgres interop test database");

        Self {
            database_name: database_name.clone(),
            url: format!("postgres://postgres:postgres@localhost:5432/{database_name}"),
        }
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn cleanup(&self) {
        let mut admin = PgConnection::connect(ADMIN_DATABASE_URL)
            .await
            .expect("failed to connect to the postgres admin database for cleanup");

        admin
            .execute(
                format!(
                    r#"
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '{database_name}'
  AND pid <> pg_backend_pid()
"#,
                    database_name = self.database_name
                )
                .as_str(),
            )
            .await
            .expect("failed to terminate temporary postgres interop test database connections");

        admin
            .execute(format!(r#"DROP DATABASE "{}""#, self.database_name).as_str())
            .await
            .expect("failed to drop temporary postgres interop test database");
    }
}

fn unique_suffix() -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the unix epoch")
        .as_nanos();

    format!("{}_{}", std::process::id(), unix_nanos)
}
