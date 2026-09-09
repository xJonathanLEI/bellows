import { randomBytes } from "node:crypto";
import { Client, Pool } from "pg";
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
  type PublishedTask,
  type PublishTaskDefinition,
  type RenewedTaskLease,
  type SingletonTaskDefinition,
  type TaskCallback,
  type TaskDefinition,
} from "../types.js";
import {
  initializePostgresPool,
  POSTGRES_NOTIFY_CHANNEL,
  type PostgresBackendOptions,
  PostgresTaskOperations,
} from "./postgres-operations.js";

export type { PostgresBackendOptions } from "./postgres-operations.js";
export {
  initializePostgresSchema,
  PostgresPublishedTaskIdError,
} from "./postgres-operations.js";

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
      readonly callback_id: number;
      readonly callback_payload_json: string;
    };

const MAX_CALLBACK_ID = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * Full PostgreSQL backend with listener-backed signaling and callback delivery.
 * For plain publication without a listener, use `PostgresPublishingBackend` from
 * `@xjonathanlei/bellows/backends/postgres-publishing`, including for callback-bearing definitions.
 * Publication returns safe numeric IDs or throws `PostgresPublishedTaskIdError` with the exact
 * committed ID as a string. Other publication errors do not establish whether the insert committed.
 */
export class PostgresBackend implements Backend {
  private readonly signals = new Map<string, SignalHub>();
  private readonly callbacks = new Map<number, CallbackSink>();

  private constructor(
    private readonly pool: Pool,
    private readonly listener: Client,
    private readonly operations: PostgresTaskOperations,
    private readonly options: PostgresBackendOptions,
  ) {}

  static async connect(
    databaseUrl: string,
    options: PostgresBackendOptions = {},
  ): Promise<PostgresBackend> {
    const pool = new Pool({ connectionString: databaseUrl });
    let listener: Client | undefined;

    try {
      const operations = new PostgresTaskOperations(pool, options);
      listener = new Client({ connectionString: databaseUrl });
      await listener.connect();
      await listener.query(`LISTEN ${POSTGRES_NOTIFY_CHANNEL}`);

      const backend = new PostgresBackend(pool, listener, operations, options);
      listener.on(
        "notification",
        (message: { payload?: string | null | undefined }) => {
          if (!message.payload) {
            return;
          }

          backend.handleNotification(message.payload);
        },
      );

      return backend;
    } catch (error) {
      await listener?.end().catch(() => undefined);
      await pool.end().catch(() => undefined);
      throw error;
    }
  }

  async initialize(): Promise<void> {
    await initializePostgresPool(this.pool, this.options);
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
    return await this.operations.claimPublished(
      task,
      workerId,
      taskId,
      leaseExpirationMs,
    );
  }

  async claimEarliestPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    return await this.operations.claimEarliestPublished(
      task,
      workerId,
      leaseExpirationMs,
    );
  }

  async claimSingleton<TCallback>(
    task: SingletonTaskDefinition<TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<undefined>> {
    return await this.operations.claimSingleton(
      task,
      workerId,
      leaseExpirationMs,
    );
  }

  async renew(
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<RenewedTaskLease> {
    return await this.operations.renew(workerId, taskId, leaseExpirationMs);
  }

  async fail(
    workerId: number,
    taskId: number,
    availableFromMs: number | null,
  ): Promise<FailedTask> {
    return await this.operations.fail(workerId, taskId, availableFromMs);
  }

  async finish<TTask extends TaskDefinition>(
    task: TTask,
    workerId: number,
    taskId: number,
    callbackPayload: TaskCallback<TTask>,
    availableFromMs: number | null,
  ): Promise<FinishedTask> {
    return await this.operations.finish(
      task,
      workerId,
      taskId,
      callbackPayload,
      availableFromMs,
    );
  }

  private async publishInternal<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
    callbackId: number | null,
    availableFromMs: number | null,
  ): Promise<PublishedTask> {
    return await this.operations.publish(
      task,
      payload,
      callbackId,
      availableFromMs,
    );
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
    callbackId: number | null,
    callbackPayloadJson: string,
  ): void {
    if (callbackId === null) {
      return;
    }

    const callback = this.callbacks.get(callbackId);
    if (!callback) {
      return;
    }

    this.callbacks.delete(callbackId);
    callback.deliver(callbackPayloadJson);
  }

  private reserveCallbackId(): number {
    while (true) {
      const callbackId = Number(
        randomBytes(8).readBigUInt64BE(0) % (MAX_CALLBACK_ID + 1n),
      );
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
