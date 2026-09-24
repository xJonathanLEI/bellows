import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { Client } from "pg";
import { createTestHarness, type TestHarness } from "wrangler";

const HYPERDRIVE_LOCAL_URL =
  "CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE";
const POLL_TIMEOUT_MS = 3_000;

interface TaskRow {
  task_id: string;
  task_name: string;
  task_unique_key: string | null;
  payload_json: string;
  callback_id: string | null;
  lease_worker_id: string | null;
  available_from_unix_ms: string | null;
}

interface ProcessedRow {
  task_id: string;
  name: string;
  execution_count: number;
}

interface DatabaseState {
  tasks: TaskRow[];
  processed: ProcessedRow[];
}

interface ResponseLike {
  status: number;
  text(): Promise<string>;
}

interface DispatcherNamespace {
  getByName(name: string): {
    fetch(input: string, init?: RequestInit): Promise<ResponseLike>;
  };
}

export interface ConsumedResponse {
  status: number;
  body: string;
}

interface WorkerProject {
  configPath: URL;
  prebuiltWorkerDir?: URL;
}

export interface CloudflareProjects {
  producer: WorkerProject;
  processor?: WorkerProject;
  singleton?: {
    bootstrap: boolean;
    mode?: "park" | "success" | "failure" | "immediate" | "done";
  };
}

export interface CloudflarePostgresFixtureOptions {
  databaseUrl: string;
  // Initialize the existing schema with bounded operations and close any owned connections.
  initializeSchema: (databaseUrl: string, schema: string) => Promise<void>;
}

export async function deadline<T>(
  promise: Promise<T>,
  description: string,
  timeoutMs: number,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`Timed out after ${timeoutMs}ms: ${description}`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export async function poll<T>(
  description: string,
  read: () => T | Promise<T>,
  ready: (value: T) => boolean,
): Promise<T> {
  const expires = performance.now() + POLL_TIMEOUT_MS;
  let latest: T;
  do {
    latest = await read();
    if (ready(latest)) {
      return latest;
    }
    // Back off only after inspecting real state; no fixed processing sleeps.
    await delay(20);
  } while (performance.now() < expires);
  throw new Error(
    `Timed out waiting for ${description}; last observation: ${JSON.stringify(latest)}`,
  );
}

export class CloudflarePostgresFixture {
  readonly schema = `bellows_cf_${randomUUID().replaceAll("-", "")}`;
  readonly table = `"${this.schema}".bellows_tasks`;
  readonly processedTable = `"${this.schema}".processed_tasks`;
  private readonly previousHyperdriveUrl = process.env[HYPERDRIVE_LOCAL_URL];
  private readonly clientErrors: string[] = [];
  readonly admin: Client;
  readonly server: TestHarness;
  private schemaCreated = false;
  private started = false;
  private closed = false;
  private lastState: DatabaseState | undefined;
  private readonly releaseGates: Array<() => Promise<void>> = [];
  private readonly responses = new Set<Promise<ConsumedResponse>>();
  private readonly scheduledEvents = new Set<Promise<unknown>>();
  private readonly gatePids = new Set<number>();
  private readonly requestPids = new Set<number>();

  constructor(
    project: CloudflareProjects,
    private readonly options: CloudflarePostgresFixtureOptions,
  ) {
    // Only fixture-generated identifiers may be interpolated into administrative SQL.
    if (!/^bellows_cf_[0-9a-f]{32}$/.test(this.schema)) {
      throw new Error("Invalid generated Cloudflare test schema name");
    }
    this.admin = this.createClient();
    // Wrangler reads this while loading the real on-disk Hyperdrive bindings.
    // Do not replace HYPERDRIVE with a vars override or a fabricated binding.
    process.env[HYPERDRIVE_LOCAL_URL] = this.options.databaseUrl;
    const vars = {
      BELLOWS_SCHEMA: this.schema,
      BELLOWS_SINGLETON_BOOTSTRAP: project.singleton?.bootstrap
        ? "true"
        : "false",
      BELLOWS_SINGLETON_MODE: project.singleton?.mode ?? "park",
    };
    try {
      this.server = createTestHarness({
        workers: [
          {
            ...project.producer,
            vars,
          },
          ...(project.processor
            ? [
                {
                  ...project.processor,
                  vars,
                },
              ]
            : []),
        ],
      });
    } catch (error) {
      this.restoreEnvironment();
      throw error;
    }
  }

  private createClient(): Client {
    const client = new Client({
      connectionString: this.options.databaseUrl,
      connectionTimeoutMillis: 1_500,
      statement_timeout: 1_000,
      query_timeout: 1_500,
      idle_in_transaction_session_timeout: 15_000,
    });
    client.on("error", (error: Error) => {
      this.clientErrors.push(this.redact(error.message));
    });
    return client;
  }

  async start(): Promise<void> {
    try {
      await this.admin.connect();
      await this.admin.query("SELECT 1");
    } catch {
      throw new Error(
        "Cloudflare integration requires reachable PostgreSQL 17. Set " +
          "BELLOWS_CLOUDFLARE_TEST_POSTGRES_URL to a database with CREATE schema privileges " +
          "(default: local PostgreSQL on port 5432, database/user/password postgres). " +
          "The connectivity preflight failed; this suite does not skip unavailable databases.",
      );
    }

    await this.admin.query(`CREATE SCHEMA "${this.schema}"`);
    this.schemaCreated = true;
    // The language-owned initializer finishes before side-effect tables or Workers start.
    await this.options.initializeSchema(this.options.databaseUrl, this.schema);
    await this.admin.query(`
CREATE TABLE ${this.processedTable} (
    task_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    execution_count INTEGER NOT NULL CHECK (execution_count > 0)
)
    `);
    // Statement start precedes the SQL gate's lock wait, so releasing a gate late cannot
    // hide early business execution behind a later commit/trigger timestamp.
    await this.admin.query(`
CREATE TABLE "${this.schema}".task_executions (
    task_id BIGINT NOT NULL,
    execution_count INTEGER NOT NULL,
    executed_at_ms BIGINT NOT NULL DEFAULT floor(extract(epoch FROM statement_timestamp()) * 1000)
);
CREATE FUNCTION "${this.schema}".record_execution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO "${this.schema}".task_executions (task_id, execution_count)
    VALUES (NEW.task_id, NEW.execution_count);
    RETURN NEW;
END $$;
CREATE TRIGGER record_execution AFTER INSERT OR UPDATE ON ${this.processedTable}
FOR EACH ROW EXECUTE FUNCTION "${this.schema}".record_execution()
    `);
    await deadline(
      this.server.listen(),
      "starting both Wrangler projects",
      8_000,
    );
    this.started = true;
  }

  async state(): Promise<DatabaseState> {
    await this.activeRequestClients();
    const tasks = await this.admin.query<TaskRow>(
      `SELECT * FROM ${this.table} ORDER BY task_id`,
    );
    const processed = await this.admin.query<ProcessedRow>(
      `SELECT * FROM ${this.processedTable} ORDER BY task_id`,
    );
    return { tasks: tasks.rows, processed: processed.rows };
  }

  async dispatcher() {
    // A minimal structural type keeps cloudflare:workers globals out of Node's type scope.
    const env = await deadline(
      this.server
        .getWorker<{ DISPATCHER: DispatcherNamespace }>(
          "bellows-cloudflare-producer",
        )
        .getEnv(),
      "retrieving the real Durable Object binding",
      2_000,
    );
    return env.DISPATCHER.getByName("global");
  }

  async schedule() {
    const dispatcher = await this.dispatcher();
    const response = await this.consume(
      dispatcher.fetch("https://dispatcher/__test/state"),
      "inspect durable schedule",
    );
    if (response.status !== 200) throw new Error(response.body);
    return JSON.parse(response.body) as {
      metadata: { nextHeartbeatAtMs: number; nextAttemptId: number };
      tasks: Record<
        string,
        {
          task:
            | { kind: "published"; taskId: string; taskName: string }
            | { kind: "singleton"; taskName: string };
          state: { type: "pending" | "running"; attemptId?: number };
          nextAttemptAtMs: number;
          infrastructureFailures: number;
        }
      >;
      alarm: number;
      now: number;
    };
  }

  async executions(taskId: string) {
    return (
      await this.admin.query<{
        execution_count: number;
        executed_at_ms: string;
      }>(
        `SELECT execution_count, executed_at_ms FROM "${this.schema}".task_executions
       WHERE task_id = $1 ORDER BY execution_count`,
        [taskId],
      )
    ).rows;
  }

  get processor() {
    return this.server.getWorker("bellows-cloudflare-processor");
  }

  async runScheduled(
    outcome: "ok" | "exception" = "ok",
    scheduledTime = new Date(),
  ): Promise<void> {
    const event = this.server
      .getWorker()
      .scheduled({ cron: "* * * * *", scheduledTime });
    this.scheduledEvents.add(event);
    void event.then(
      () => this.scheduledEvents.delete(event),
      () => this.scheduledEvents.delete(event),
    );
    const result = await deadline(event, "complete scheduled sweep", 3_000);
    if (result.outcome !== outcome) {
      throw new Error(
        `Scheduled sweep outcome: expected ${outcome}, received ${result.outcome}`,
      );
    }
  }

  async consume(
    response: Promise<ResponseLike>,
    description: string,
  ): Promise<ConsumedResponse> {
    const consumed = response.then(async (value) => ({
      status: value.status,
      body: await value.text(),
    }));
    this.responses.add(consumed);
    void consumed.then(
      () => this.responses.delete(consumed),
      () => this.responses.delete(consumed),
    );
    // The deadline includes consuming the body. On timeout, cleanup releases gates
    // before draining this promise, rather than abandoning a live response.
    return await deadline(consumed, description, 2_000);
  }

  async gate(table: "processed_tasks" | "bellows_tasks" = "processed_tasks") {
    const client = this.createClient();
    let releasing: Promise<void> | undefined;
    const release = (statement: "ROLLBACK" | "COMMIT" = "ROLLBACK") => {
      releasing ??= (async () => {
        try {
          await client.query(statement);
        } finally {
          await client.end();
        }
      })();
      return releasing;
    };
    this.releaseGates.push(release);
    await client.connect();
    this.gatePids.add(
      (await client.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))
        .rows[0].pid,
    );
    await client.query("BEGIN");
    const relation = `"${this.schema}".${table}`;
    await client.query(`LOCK TABLE ${relation} IN SHARE MODE`);
    return {
      release,
      // The lock owner can change eligibility before releasing blocked claims.
      client,
      blocked: async (count: number) =>
        await poll(
          `${count} processor connection(s) blocked on ${table}`,
          async () => {
            const result = await this.admin.query<{ pid: number }>(
              `SELECT pid FROM pg_locks
               WHERE relation = $1::regclass AND NOT granted
                 AND mode = 'RowExclusiveLock'
               ORDER BY pid`,
              [relation],
            );
            return result.rows.map(({ pid }) => pid);
          },
          (pids) => pids.length === count,
        ),
    };
  }

  async waitForClientExit(pids: number[]): Promise<void> {
    await poll(
      "processor database clients to close",
      async () =>
        (
          await this.admin.query(
            "SELECT pid FROM pg_stat_activity WHERE pid = ANY($1::int[])",
            [pids],
          )
        ).rows,
      (rows) => rows.length === 0,
    );
  }

  async activeRequestClients() {
    const { rows } = await this.admin.query<{
      pid: number;
      state: string;
      wait_event_type: string | null;
      wait_event: string | null;
    }>(
      `SELECT pid, state, wait_event_type, wait_event
       FROM pg_stat_activity
       WHERE datname = current_database() AND pid <> pg_backend_pid()
         AND pid <> ALL($2::int[])
         AND (query LIKE $1 OR pid = ANY($3::int[]))`,
      [`%${this.schema}%`, [...this.gatePids], [...this.requestPids]],
    );
    // Remember observed owners even when their last statement later becomes COMMIT/ROLLBACK.
    // Other tests can use the same database concurrently; never claim their unrelated clients.
    for (const { pid } of rows) this.requestPids.add(pid);
    return rows;
  }

  async waitForIdle(): Promise<void> {
    await poll(
      "all request-scoped database clients and drivers to close",
      () => this.activeRequestClients(),
      (rows) => rows.length === 0,
    );
  }

  private restoreEnvironment(): void {
    if (this.previousHyperdriveUrl === undefined) {
      delete process.env[HYPERDRIVE_LOCAL_URL];
    } else {
      process.env[HYPERDRIVE_LOCAL_URL] = this.previousHyperdriveUrl;
    }
  }

  private redact(message: string): string {
    return message
      .replaceAll(this.options.databaseUrl, "[database URL redacted]")
      .replace(/postgres(?:ql)?:\/\/[^\s"']+/gi, "[database URL redacted]");
  }

  async debug(): Promise<void> {
    // The harness timeline contains request routes and sanitized Worker errors,
    // not env/getEnv(), configuration, or pg.Client objects containing credentials.
    this.server.debug();
    let state: DatabaseState | string | undefined = this.lastState;
    if (!this.closed && this.schemaCreated) {
      try {
        state = await this.state();
      } catch (error) {
        state = String(error);
      }
    }
    console.log(
      this.redact(
        JSON.stringify({
          schema: this.schema,
          state,
          runtimeLogs: this.server.getLogs(),
          clientErrors: this.clientErrors,
        }),
      ),
    );
  }

  async close(): Promise<void> {
    const errors: string[] = [];
    const clean = async (
      description: string,
      action: () => Promise<unknown>,
    ) => {
      try {
        await action();
      } catch (error) {
        errors.push(`${description}: ${this.redact(String(error))}`);
      }
    };
    try {
      // Roll back locks first, including on test failure or a response deadline.
      for (const release of this.releaseGates) {
        await clean("release database gate", release);
      }
      await clean("consume outstanding responses", () =>
        deadline(
          Promise.allSettled([...this.responses]),
          "draining caller responses",
          3_000,
        ),
      );
      await clean("settle outstanding scheduled events", () =>
        deadline(
          Promise.allSettled([...this.scheduledEvents]),
          "draining scheduled events",
          3_000,
        ),
      );
      if (this.started) {
        await clean("drain processor database activity", () =>
          this.waitForIdle(),
        );
      }
      await clean("close Wrangler harness", () =>
        deadline(this.server.close(), "closing Wrangler harness", 5_000),
      );
      if (this.schemaCreated) {
        await clean("capture final database state", async () => {
          this.lastState = await this.state();
        });
        await clean("drop isolated schema", () =>
          this.admin.query(`DROP SCHEMA "${this.schema}" CASCADE`),
        );
      }
    } finally {
      await clean("close administrative connection", () => this.admin.end());
      this.restoreEnvironment();
      this.closed = true;
    }
    errors.push(...this.clientErrors);
    if (errors.length > 0) {
      throw new Error(
        `Cloudflare fixture cleanup failed:\n${errors.join("\n")}`,
      );
    }
  }
}
