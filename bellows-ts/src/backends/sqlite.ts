import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import {
  type CallbackSink,
  createCallbackChannel,
} from "../internal/awaitable-task.js";
import { SignalHub } from "../internal/signal-hub.js";
import {
  AwaitableTask,
  type Backend,
  type BackendSignal,
  type ClaimedTask,
  type FinishedTask,
  LeaseLostError,
  type PublishedTask,
  type PublishTaskDefinition,
  type RenewedTaskLease,
  type SingletonTaskDefinition,
  type SweptTask,
  type TaskCallback,
  type TaskDefinition,
  TaskLeasedError,
  TaskNotFoundError,
} from "../types.js";

const INITIALIZE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS bellows_tasks (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT NOT NULL,
    task_unique_key TEXT,
    payload_json TEXT NOT NULL,
    callback_id TEXT,
    lease_worker_id INTEGER,
    lease_expiration_unix_ms INTEGER,
    CHECK ((lease_worker_id IS NULL) = (lease_expiration_unix_ms IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS bellows_tasks_unique_key_idx
    ON bellows_tasks (task_unique_key);

CREATE INDEX IF NOT EXISTS bellows_tasks_sweep_idx
    ON bellows_tasks (task_name, lease_expiration_unix_ms, task_id);
`;

export class SqliteBackend implements Backend {
  private readonly signals = new Map<string, SignalHub>();
  private readonly callbacks = new Map<string, CallbackSink>();

  private constructor(private readonly database: DatabaseSync) {}

  static async connect(databaseUrl: string): Promise<SqliteBackend> {
    return new SqliteBackend(new DatabaseSync(sqliteUrlToPath(databaseUrl)));
  }

  async initialize(): Promise<void> {
    this.database.exec(INITIALIZE_SCHEMA_SQL);
  }

  async close(): Promise<void> {
    this.database.close();
    for (const callback of this.callbacks.values()) {
      callback.drop();
    }
    this.callbacks.clear();
    for (const signal of this.signals.values()) {
      signal.close();
    }
  }

  async subscribe(task: TaskDefinition) {
    return this.signalForTask(task.name).subscribe();
  }

  async sweep(task: TaskDefinition): Promise<SweptTask[]> {
    const rows = this.database
      .prepare(
        `
SELECT task_id
FROM bellows_tasks
WHERE task_name = ?
  AND (
        lease_worker_id IS NULL
        OR lease_expiration_unix_ms IS NULL
        OR lease_expiration_unix_ms <= ?
      )
ORDER BY task_id
        `,
      )
      .all(task.name, Date.now()) as Array<{ task_id: number }>;

    return rows.map((row) => ({ taskId: row.task_id }));
  }

  async publish<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
  ): Promise<PublishedTask> {
    return await this.publishInternal(task, payload, null);
  }

  async publishAwaitable<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
  ): Promise<AwaitableTask<TCallback>> {
    const callbackId = this.reserveCallbackId();
    const { callbackPromise, callbackSink } = createCallbackChannel(
      task.callbackCodec,
    );
    this.callbacks.set(callbackId, callbackSink);

    try {
      const published = await this.publishInternal(task, payload, callbackId);
      return new AwaitableTask(published.taskId, callbackPromise);
    } catch (error) {
      this.callbacks.get(callbackId)?.drop();
      this.callbacks.delete(callbackId);
      throw error;
    }
  }

  async claimPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    const claimedRow = this.database
      .prepare(
        `
UPDATE bellows_tasks
SET lease_worker_id = ?, lease_expiration_unix_ms = ?
WHERE task_id = ?
  AND task_name = ?
  AND (
        lease_worker_id IS NULL
        OR lease_expiration_unix_ms IS NULL
        OR lease_expiration_unix_ms <= ?
      )
RETURNING payload_json
        `,
      )
      .get(workerId, leaseExpirationMs, taskId, task.name, Date.now()) as
      | { payload_json: string }
      | undefined;

    if (!claimedRow) {
      const currentRow = this.database
        .prepare(
          `
SELECT lease_expiration_unix_ms
FROM bellows_tasks
WHERE task_id = ?
  AND task_name = ?
          `,
        )
        .get(taskId, task.name) as
        | { lease_expiration_unix_ms: number | null }
        | undefined;

      if (!currentRow) {
        throw new TaskNotFoundError();
      }

      if (
        currentRow.lease_expiration_unix_ms !== null &&
        currentRow.lease_expiration_unix_ms > Date.now()
      ) {
        throw new TaskLeasedError(currentRow.lease_expiration_unix_ms);
      }

      throw new TaskNotFoundError();
    }

    return {
      taskId,
      taskPayload: task.codec.decode(claimedRow.payload_json),
      leaseExpirationMs,
    };
  }

  async claimSingleton<TCallback>(
    task: SingletonTaskDefinition<TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<undefined>> {
    const claimedRow = this.database
      .prepare(
        `
INSERT INTO bellows_tasks (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    lease_expiration_unix_ms
)
VALUES (?, ?, 'null', NULL, ?, ?)
ON CONFLICT(task_unique_key) DO UPDATE
SET lease_worker_id = excluded.lease_worker_id,
    lease_expiration_unix_ms = excluded.lease_expiration_unix_ms
WHERE bellows_tasks.task_name = excluded.task_name
  AND (
        bellows_tasks.lease_worker_id IS NULL
        OR bellows_tasks.lease_expiration_unix_ms IS NULL
        OR bellows_tasks.lease_expiration_unix_ms <= ?
      )
RETURNING task_id
        `,
      )
      .get(task.name, task.name, workerId, leaseExpirationMs, Date.now()) as
      | { task_id: number }
      | undefined;

    if (!claimedRow) {
      const currentRow = this.database
        .prepare(
          `
SELECT lease_expiration_unix_ms
FROM bellows_tasks
WHERE task_name = ?
  AND task_unique_key = ?
          `,
        )
        .get(task.name, task.name) as
        | { lease_expiration_unix_ms: number | null }
        | undefined;

      if (!currentRow) {
        throw new TaskNotFoundError();
      }

      if (
        currentRow.lease_expiration_unix_ms !== null &&
        currentRow.lease_expiration_unix_ms > Date.now()
      ) {
        throw new TaskLeasedError(currentRow.lease_expiration_unix_ms);
      }

      throw new TaskNotFoundError();
    }

    return {
      taskId: claimedRow.task_id,
      taskPayload: undefined,
      leaseExpirationMs,
    };
  }

  async renew(
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<RenewedTaskLease> {
    const result = this.database
      .prepare(
        `
UPDATE bellows_tasks
SET lease_expiration_unix_ms = ?
WHERE task_id = ?
  AND lease_worker_id = ?
        `,
      )
      .run(leaseExpirationMs, taskId, workerId);

    if (result.changes === 0) {
      throw new LeaseLostError();
    }

    return { newExpirationMs: leaseExpirationMs };
  }

  async finish<TTask extends TaskDefinition>(
    task: TTask,
    workerId: number,
    taskId: number,
    callbackPayload: TaskCallback<TTask>,
  ): Promise<FinishedTask> {
    const callbackPayloadJson = task.callbackCodec.encode(callbackPayload);

    const singletonRow = this.database
      .prepare(
        `
UPDATE bellows_tasks
SET lease_worker_id = NULL,
    lease_expiration_unix_ms = NULL
WHERE task_id = ?
  AND lease_worker_id = ?
  AND task_unique_key IS NOT NULL
RETURNING task_name, callback_id
        `,
      )
      .get(taskId, workerId) as
      | { task_name: string; callback_id: string | null }
      | undefined;

    if (singletonRow) {
      this.deliverCallback(singletonRow.callback_id, callbackPayloadJson);
      this.emitSignal(singletonRow.task_name, newTaskAvailable(taskId));
      return { taskId };
    }

    const publishedRow = this.database
      .prepare(
        `
DELETE FROM bellows_tasks
WHERE task_id = ?
  AND lease_worker_id = ?
  AND task_unique_key IS NULL
RETURNING callback_id
        `,
      )
      .get(taskId, workerId) as { callback_id: string | null } | undefined;

    if (!publishedRow) {
      throw new LeaseLostError();
    }

    this.deliverCallback(publishedRow.callback_id, callbackPayloadJson);
    return { taskId };
  }

  private async publishInternal<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
    callbackId: string | null,
  ): Promise<PublishedTask> {
    const result = this.database
      .prepare(
        `
INSERT INTO bellows_tasks (task_name, task_unique_key, payload_json, callback_id)
VALUES (?, NULL, ?, ?)
        `,
      )
      .run(task.name, task.codec.encode(payload), callbackId);

    const taskId = Number(result.lastInsertRowid);
    this.emitSignal(task.name, newTaskAvailable(taskId));
    return { taskId };
  }

  private deliverCallback(
    callbackId: string | null,
    callbackPayloadJson: string,
  ): void {
    if (!callbackId) {
      return;
    }

    const callback = this.callbacks.get(callbackId);
    if (!callback) {
      return;
    }

    this.callbacks.delete(callbackId);
    callback.deliver(callbackPayloadJson);
  }

  private reserveCallbackId(): string {
    while (true) {
      const callbackId = randomUUID();
      if (!this.callbacks.has(callbackId)) {
        return callbackId;
      }
    }
  }

  private emitSignal(taskName: string, signal: BackendSignal): void {
    this.signals.get(taskName)?.send(signal);
  }

  private signalForTask(taskName: string): SignalHub {
    let signal = this.signals.get(taskName);
    if (!signal) {
      signal = new SignalHub();
      this.signals.set(taskName, signal);
    }

    return signal;
  }
}

function sqliteUrlToPath(databaseUrl: string): string {
  if (!databaseUrl.startsWith("sqlite://")) {
    throw new Error(`unsupported SQLite URL: ${databaseUrl}`);
  }

  return decodeURIComponent(databaseUrl.slice("sqlite://".length));
}

function newTaskAvailable(taskId: number): BackendSignal {
  return {
    type: "new-task-available",
    taskId,
  };
}
