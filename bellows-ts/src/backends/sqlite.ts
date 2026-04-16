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
  type FailedTask,
  type FinishedTask,
  LeaseLostError,
  type PublishedTask,
  type PublishTaskDefinition,
  type RenewedTaskLease,
  type SingletonTaskDefinition,
  type TaskCallback,
  type TaskDefinition,
  TaskLeasedError,
  TaskNotFoundError,
  TaskUnavailableError,
} from "../types.js";

const INITIALIZE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS bellows_tasks (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT NOT NULL,
    task_unique_key TEXT,
    payload_json TEXT NOT NULL,
    callback_id TEXT,
    lease_worker_id INTEGER,
    available_from_unix_ms INTEGER,
    CHECK (lease_worker_id IS NULL OR available_from_unix_ms IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS bellows_tasks_unique_key_idx
    ON bellows_tasks (task_unique_key);

CREATE INDEX IF NOT EXISTS bellows_tasks_available_idx
    ON bellows_tasks (task_name, task_unique_key, available_from_unix_ms, task_id);
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
SET lease_worker_id = ?,
    available_from_unix_ms = ?
WHERE task_id = ?
  AND task_name = ?
  AND task_unique_key IS NULL
  AND (
        available_from_unix_ms IS NULL
        OR available_from_unix_ms <= ?
      )
RETURNING payload_json
        `,
      )
      .get(workerId, leaseExpirationMs, taskId, task.name, Date.now()) as
      | { payload_json: string }
      | undefined;

    if (!claimedRow) {
      throw this.loadClaimFailure(taskId, task.name);
    }

    this.emitSignal(task.name, taskId, leaseExpirationMs);

    return {
      taskId,
      taskPayload: task.codec.decode(claimedRow.payload_json),
      leaseExpirationMs,
    };
  }

  async claimEarliestPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    this.database.exec("BEGIN IMMEDIATE");

    try {
      const candidate = this.database
        .prepare(
          `
SELECT task_id, payload_json
FROM bellows_tasks
WHERE task_name = ?
  AND task_unique_key IS NULL
  AND (
        available_from_unix_ms IS NULL
        OR available_from_unix_ms <= ?
      )
ORDER BY available_from_unix_ms IS NOT NULL, available_from_unix_ms, task_id
LIMIT 1
          `,
        )
        .get(task.name, Date.now()) as
        | { task_id: number; payload_json: string }
        | undefined;

      if (!candidate) {
        const nextRow = this.database
          .prepare(
            `
SELECT MIN(available_from_unix_ms) AS available_from_unix_ms
FROM bellows_tasks
WHERE task_name = ?
  AND task_unique_key IS NULL
  AND available_from_unix_ms > ?
            `,
          )
          .get(task.name, Date.now()) as {
          available_from_unix_ms: number | null;
        };

        this.database.exec("COMMIT");
        throw new TaskUnavailableError(nextRow.available_from_unix_ms);
      }

      const updated = this.database
        .prepare(
          `
UPDATE bellows_tasks
SET lease_worker_id = ?,
    available_from_unix_ms = ?
WHERE task_id = ?
  AND (
        available_from_unix_ms IS NULL
        OR available_from_unix_ms <= ?
      )
        `,
        )
        .run(workerId, leaseExpirationMs, candidate.task_id, Date.now());

      if (updated.changes === 0) {
        this.database.exec("ROLLBACK");
        throw new TaskUnavailableError(Date.now());
      }

      this.database.exec("COMMIT");
      this.emitSignal(task.name, candidate.task_id, leaseExpirationMs);

      return {
        taskId: candidate.task_id,
        taskPayload: task.codec.decode(candidate.payload_json),
        leaseExpirationMs,
      };
    } catch (error) {
      if (this.database.isTransaction) {
        this.database.exec("ROLLBACK");
      }
      throw error;
    }
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
    available_from_unix_ms
)
VALUES (?, ?, 'null', NULL, ?, ?)
ON CONFLICT(task_unique_key) DO UPDATE
SET lease_worker_id = excluded.lease_worker_id,
    available_from_unix_ms = excluded.available_from_unix_ms
WHERE bellows_tasks.task_name = excluded.task_name
  AND (
        bellows_tasks.available_from_unix_ms IS NULL
        OR bellows_tasks.available_from_unix_ms <= ?
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
SELECT lease_worker_id, available_from_unix_ms
FROM bellows_tasks
WHERE task_name = ?
  AND task_unique_key = ?
          `,
        )
        .get(task.name, task.name) as
        | {
            lease_worker_id: number | null;
            available_from_unix_ms: number | null;
          }
        | undefined;

      if (!currentRow) {
        throw new TaskNotFoundError();
      }

      if (
        currentRow.available_from_unix_ms !== null &&
        currentRow.available_from_unix_ms > Date.now()
      ) {
        if (currentRow.lease_worker_id !== null) {
          throw new TaskLeasedError(currentRow.available_from_unix_ms);
        }

        throw new TaskUnavailableError(currentRow.available_from_unix_ms);
      }

      throw new TaskNotFoundError();
    }

    this.emitSignal(task.name, claimedRow.task_id, leaseExpirationMs);

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
SET available_from_unix_ms = ?
WHERE task_id = ?
  AND lease_worker_id = ?
        `,
      )
      .run(leaseExpirationMs, taskId, workerId);

    if (result.changes === 0) {
      throw new LeaseLostError();
    }

    const row = this.database
      .prepare("SELECT task_name FROM bellows_tasks WHERE task_id = ?")
      .get(taskId) as { task_name: string };
    this.emitSignalIfRegistered(row.task_name, taskId, leaseExpirationMs);

    return { newExpirationMs: leaseExpirationMs };
  }

  async fail(
    workerId: number,
    taskId: number,
    availableFromMs: number | null,
  ): Promise<FailedTask> {
    const updated = this.database
      .prepare(
        `
UPDATE bellows_tasks
SET lease_worker_id = NULL,
    available_from_unix_ms = ?
WHERE task_id = ?
  AND lease_worker_id = ?
RETURNING task_name
        `,
      )
      .get(availableFromMs, taskId, workerId) as
      | { task_name: string }
      | undefined;

    if (!updated) {
      throw new LeaseLostError();
    }

    this.emitSignalIfRegistered(updated.task_name, taskId, availableFromMs);
    return { taskId };
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
    available_from_unix_ms = NULL
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
      if (singletonRow.callback_id !== null) {
        this.deliverCallback(singletonRow.callback_id, callbackPayloadJson);
      }

      this.emitSignalIfRegistered(singletonRow.task_name, taskId, null);
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

    if (publishedRow.callback_id !== null) {
      this.deliverCallback(publishedRow.callback_id, callbackPayloadJson);
    }

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
INSERT INTO bellows_tasks (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    available_from_unix_ms
)
VALUES (?, NULL, ?, ?, NULL, NULL)
        `,
      )
      .run(task.name, task.codec.encode(payload), callbackId);

    const taskId = Number(result.lastInsertRowid);
    this.emitSignal(task.name, taskId, null);
    return { taskId };
  }

  private loadClaimFailure(taskId: number, taskName: string): Error {
    const currentRow = this.database
      .prepare(
        `
SELECT lease_worker_id, available_from_unix_ms
FROM bellows_tasks
WHERE task_id = ?
  AND task_name = ?
  AND task_unique_key IS NULL
        `,
      )
      .get(taskId, taskName) as
      | {
          lease_worker_id: number | null;
          available_from_unix_ms: number | null;
        }
      | undefined;

    if (!currentRow) {
      return new TaskNotFoundError();
    }

    if (
      currentRow.available_from_unix_ms !== null &&
      currentRow.available_from_unix_ms > Date.now()
    ) {
      if (currentRow.lease_worker_id !== null) {
        return new TaskLeasedError(currentRow.available_from_unix_ms);
      }

      return new TaskUnavailableError(currentRow.available_from_unix_ms);
    }

    return new TaskNotFoundError();
  }

  private emitSignal(
    taskName: string,
    taskId: number,
    availableFromMs: number | null,
  ): void {
    this.signals
      .get(taskName)
      ?.send(newTaskAvailable(taskId, availableFromMs ?? Date.now()));
  }

  private emitSignalIfRegistered(
    taskName: string,
    taskId: number,
    availableFromMs: number | null,
  ): void {
    if (this.signals.has(taskName)) {
      this.emitSignal(taskName, taskId, availableFromMs);
    }
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

function newTaskAvailable(
  taskId: number | null,
  availableFromMs: number,
): BackendSignal {
  return {
    type: "new-task-available",
    taskId,
    availableFromMs,
  };
}
