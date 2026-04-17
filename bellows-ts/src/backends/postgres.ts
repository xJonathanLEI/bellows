import { randomUUID } from "node:crypto";
import { Client, Pool, type PoolClient } from "pg";
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

const NOTIFY_CHANNEL = "bellows_tasks";
const INITIALIZE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS bellows_tasks (
    task_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_name TEXT NOT NULL,
    task_unique_key TEXT,
    payload_json TEXT NOT NULL,
    callback_id TEXT,
    lease_worker_id BIGINT,
    available_from_unix_ms BIGINT,
    CHECK (lease_worker_id IS NULL OR available_from_unix_ms IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS bellows_tasks_unique_key_idx
    ON bellows_tasks (task_unique_key);

CREATE INDEX IF NOT EXISTS bellows_tasks_available_idx
    ON bellows_tasks (task_name, task_unique_key, available_from_unix_ms, task_id);

CREATE OR REPLACE FUNCTION bellows_notify_task_available()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify(
        'bellows_tasks',
        json_build_object(
            'kind', 'new_task_available',
            'task_name', NEW.task_name,
            'task_id', NEW.task_id,
            'available_from_unix_ms', NEW.available_from_unix_ms
        )::text
    );

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS bellows_tasks_notify_available ON bellows_tasks;

CREATE TRIGGER bellows_tasks_notify_available
AFTER INSERT OR UPDATE OF lease_worker_id, available_from_unix_ms ON bellows_tasks
FOR EACH ROW
EXECUTE FUNCTION bellows_notify_task_available();
`;

type NotificationPayload =
  | {
      readonly kind: "new_task_available";
      readonly task_name: string;
      readonly task_id: number | string;
      readonly available_from_unix_ms: number | string | null;
    }
  | {
      readonly kind: "task_callback";
      readonly task_name: string;
      readonly callback_id: string;
      readonly callback_payload_json: string;
    };

export class PostgresBackend implements Backend {
  private readonly signals = new Map<string, SignalHub>();
  private readonly callbacks = new Map<string, CallbackSink>();

  private constructor(
    private readonly pool: Pool,
    private readonly listener: Client,
  ) {}

  static async connect(databaseUrl: string): Promise<PostgresBackend> {
    const pool = new Pool({ connectionString: databaseUrl });
    const listener = new Client({ connectionString: databaseUrl });
    await listener.connect();
    await listener.query(`LISTEN ${NOTIFY_CHANNEL}`);

    const backend = new PostgresBackend(pool, listener);
    listener.on("notification", (message: { payload?: string | null }) => {
      if (!message.payload) {
        return;
      }

      backend.handleNotification(message.payload);
    });

    return backend;
  }

  async initialize(): Promise<void> {
    await this.pool.query(INITIALIZE_SCHEMA_SQL);
  }

  async close(): Promise<void> {
    await this.listener.end().catch(() => undefined);
    await this.pool.end();
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
    return await this.publishInternal(task, payload, null, null);
  }

  async publishFuture<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
    availableFromMs: number,
  ): Promise<PublishedTask> {
    return await this.publishInternal(task, payload, null, availableFromMs);
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
      const published = await this.publishInternal(
        task,
        payload,
        callbackId,
        null,
      );
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
    const claimedResult = await this.pool.query<{ payload_json: string }>(
      `
UPDATE bellows_tasks
SET lease_worker_id = $1,
    available_from_unix_ms = $2
WHERE task_id = $3
  AND task_name = $4
  AND task_unique_key IS NULL
  AND (
        available_from_unix_ms IS NULL
        OR available_from_unix_ms <= $5
      )
RETURNING payload_json
      `,
      [workerId, leaseExpirationMs, taskId, task.name, Date.now()],
    );

    if (claimedResult.rowCount === 0) {
      throw await this.loadClaimFailure(this.pool, taskId, task.name);
    }

    return {
      taskId,
      taskPayload: task.codec.decode(claimedResult.rows[0].payload_json),
      leaseExpirationMs,
    };
  }

  async claimEarliestPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    const claimedResult = await this.pool.query<{
      task_id: string;
      payload_json: string;
    }>(
      `
WITH next_task AS (
    SELECT task_id
    FROM bellows_tasks
    WHERE task_name = $1
      AND task_unique_key IS NULL
      AND (
            available_from_unix_ms IS NULL
            OR available_from_unix_ms <= $2
          )
    ORDER BY available_from_unix_ms NULLS FIRST, task_id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE bellows_tasks
SET lease_worker_id = $3,
    available_from_unix_ms = $4
FROM next_task
WHERE bellows_tasks.task_id = next_task.task_id
RETURNING bellows_tasks.task_id::text AS task_id, bellows_tasks.payload_json
      `,
      [task.name, Date.now(), workerId, leaseExpirationMs],
    );

    if (claimedResult.rowCount === 0) {
      const availableFromResult = await this.pool.query<{
        available_from_unix_ms: string | null;
      }>(
        `
SELECT MIN(available_from_unix_ms)::text AS available_from_unix_ms
FROM bellows_tasks
WHERE task_name = $1
  AND task_unique_key IS NULL
  AND available_from_unix_ms > $2
        `,
        [task.name, Date.now()],
      );

      throw new TaskUnavailableError(
        availableFromResult.rows[0]?.available_from_unix_ms === null
          ? null
          : Number(availableFromResult.rows[0]?.available_from_unix_ms ?? null),
      );
    }

    return {
      taskId: Number(claimedResult.rows[0].task_id),
      taskPayload: task.codec.decode(claimedResult.rows[0].payload_json),
      leaseExpirationMs,
    };
  }

  async claimSingleton<TCallback>(
    task: SingletonTaskDefinition<TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<undefined>> {
    const claimedResult = await this.pool.query<{ task_id: string }>(
      `
INSERT INTO bellows_tasks (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    available_from_unix_ms
)
VALUES ($1, $2, 'null', NULL, $3, $4)
ON CONFLICT (task_unique_key) DO UPDATE
SET lease_worker_id = EXCLUDED.lease_worker_id,
    available_from_unix_ms = EXCLUDED.available_from_unix_ms
WHERE bellows_tasks.task_name = EXCLUDED.task_name
  AND (
        bellows_tasks.available_from_unix_ms IS NULL
        OR bellows_tasks.available_from_unix_ms <= $5
      )
RETURNING task_id::text AS task_id
      `,
      [task.name, task.name, workerId, leaseExpirationMs, Date.now()],
    );

    if (claimedResult.rowCount === 0) {
      const currentResult = await this.pool.query<{
        lease_worker_id: string | null;
        available_from_unix_ms: string | null;
      }>(
        `
SELECT lease_worker_id::text AS lease_worker_id,
       available_from_unix_ms::text AS available_from_unix_ms
FROM bellows_tasks
WHERE task_name = $1
  AND task_unique_key = $2
        `,
        [task.name, task.name],
      );

      if (currentResult.rowCount === 0) {
        throw new TaskNotFoundError();
      }

      const current = currentResult.rows[0];
      if (
        current.available_from_unix_ms !== null &&
        Number(current.available_from_unix_ms) > Date.now()
      ) {
        if (current.lease_worker_id !== null) {
          throw new TaskLeasedError(Number(current.available_from_unix_ms));
        }

        throw new TaskUnavailableError(Number(current.available_from_unix_ms));
      }

      throw new TaskNotFoundError();
    }

    return {
      taskId: Number(claimedResult.rows[0].task_id),
      taskPayload: undefined,
      leaseExpirationMs,
    };
  }

  async renew(
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<RenewedTaskLease> {
    const result = await this.pool.query(
      `
UPDATE bellows_tasks
SET available_from_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
      `,
      [leaseExpirationMs, taskId, workerId],
    );

    if (result.rowCount === 0) {
      throw new LeaseLostError();
    }

    return { newExpirationMs: leaseExpirationMs };
  }

  async fail(
    workerId: number,
    taskId: number,
    availableFromMs: number | null,
  ): Promise<FailedTask> {
    const result = await this.pool.query(
      `
UPDATE bellows_tasks
SET lease_worker_id = NULL,
    available_from_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
      `,
      [availableFromMs, taskId, workerId],
    );

    if (result.rowCount === 0) {
      throw new LeaseLostError();
    }

    return { taskId };
  }

  async finish<TTask extends TaskDefinition>(
    task: TTask,
    workerId: number,
    taskId: number,
    callbackPayload: TaskCallback<TTask>,
  ): Promise<FinishedTask> {
    const callbackPayloadJson = task.callbackCodec.encode(callbackPayload);
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");

      const finishedRow =
        task.kind === "singleton"
          ? (
              await client.query<{
                callback_id: string | null;
              }>(
                `
UPDATE bellows_tasks
SET lease_worker_id = NULL,
    available_from_unix_ms = NULL
WHERE task_id = $1
  AND lease_worker_id = $2
  AND task_unique_key IS NOT NULL
RETURNING callback_id
                `,
                [taskId, workerId],
              )
            ).rows[0]
          : (
              await client.query<{
                callback_id: string | null;
              }>(
                `
DELETE FROM bellows_tasks
WHERE task_id = $1
  AND lease_worker_id = $2
  AND task_unique_key IS NULL
RETURNING callback_id
                `,
                [taskId, workerId],
              )
            ).rows[0];

      if (!finishedRow) {
        await client.query("ROLLBACK");
        throw new LeaseLostError();
      }

      if (finishedRow.callback_id !== null) {
        await client.query("SELECT pg_notify($1, $2)", [
          NOTIFY_CHANNEL,
          JSON.stringify({
            kind: "task_callback",
            task_name: task.name,
            callback_id: finishedRow.callback_id,
            callback_payload_json: callbackPayloadJson,
          } satisfies NotificationPayload),
        ]);
      }

      await client.query("COMMIT");
      return { taskId };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async publishInternal<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
    callbackId: string | null,
    availableFromMs: number | null,
  ): Promise<PublishedTask> {
    const result = await this.pool.query<{ task_id: string }>(
      `
INSERT INTO bellows_tasks (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    available_from_unix_ms
)
VALUES ($1, NULL, $2, $3, NULL, $4)
RETURNING task_id::text AS task_id
      `,
      [task.name, task.codec.encode(payload), callbackId, availableFromMs],
    );

    return { taskId: Number(result.rows[0].task_id) };
  }

  private async loadClaimFailure(
    client: Pool | PoolClient,
    taskId: number,
    taskName: string,
  ): Promise<TaskLeasedError | TaskUnavailableError | TaskNotFoundError> {
    const currentResult = await client.query<{
      lease_worker_id: string | null;
      available_from_unix_ms: string | null;
    }>(
      `
SELECT lease_worker_id::text AS lease_worker_id,
       available_from_unix_ms::text AS available_from_unix_ms
FROM bellows_tasks
WHERE task_id = $1
  AND task_name = $2
  AND task_unique_key IS NULL
      `,
      [taskId, taskName],
    );

    if (currentResult.rowCount === 0) {
      return new TaskNotFoundError();
    }

    const current = currentResult.rows[0];
    if (
      current.available_from_unix_ms !== null &&
      Number(current.available_from_unix_ms) > Date.now()
    ) {
      if (current.lease_worker_id !== null) {
        return new TaskLeasedError(Number(current.available_from_unix_ms));
      }

      return new TaskUnavailableError(Number(current.available_from_unix_ms));
    }

    return new TaskNotFoundError();
  }

  private handleNotification(payload: string): void {
    let notification: NotificationPayload;
    try {
      notification = JSON.parse(payload) as NotificationPayload;
    } catch {
      return;
    }

    if (notification.kind === "new_task_available") {
      this.emitSignal(
        notification.task_name,
        newTaskAvailable(
          Number(notification.task_id),
          notification.available_from_unix_ms === null
            ? Date.now()
            : Number(notification.available_from_unix_ms),
        ),
      );
      return;
    }

    this.deliverCallback(
      notification.callback_id,
      notification.callback_payload_json,
    );
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
