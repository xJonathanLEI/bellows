import { randomUUID } from "node:crypto";
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

class TaskEntry {
  constructor(
    readonly taskName: string,
    readonly payloadJson: string,
    readonly callbackId: string | null,
    readonly kind: "publish" | "singleton",
    readonly workerId: number | null = null,
    readonly availableFromMs: number | null = null,
  ) {}

  withState(
    workerId: number | null,
    availableFromMs: number | null,
  ): TaskEntry {
    return new TaskEntry(
      this.taskName,
      this.payloadJson,
      this.callbackId,
      this.kind,
      workerId,
      availableFromMs,
    );
  }

  signal(taskId: number): BackendSignal {
    return newTaskAvailable(taskId, this.availableFromMs ?? Date.now());
  }
}

export class InMemoryBackend implements Backend {
  private nextTaskId = 0;
  private readonly signals = new Map<string, SignalHub>();
  private readonly callbacks = new Map<string, CallbackSink>();
  private readonly tasks = new Map<number, TaskEntry>();

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
    const claimed = this.claimTask(
      task.name,
      "publish",
      workerId,
      leaseExpirationMs,
      taskId,
    );

    return {
      taskId: claimed.taskId,
      taskPayload: task.codec.decode(claimed.payloadJson),
      leaseExpirationMs: claimed.leaseExpirationMs,
    };
  }

  async claimEarliestPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    const claimed = this.claimTask(
      task.name,
      "publish",
      workerId,
      leaseExpirationMs,
      null,
    );

    return {
      taskId: claimed.taskId,
      taskPayload: task.codec.decode(claimed.payloadJson),
      leaseExpirationMs: claimed.leaseExpirationMs,
    };
  }

  async claimSingleton<TCallback>(
    task: SingletonTaskDefinition<TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<undefined>> {
    let taskId = [...this.tasks.entries()].find(
      ([, entry]) => entry.taskName === task.name && entry.kind === "singleton",
    )?.[0];

    if (taskId === undefined) {
      taskId = this.nextTaskId;
      this.nextTaskId += 1;
      this.tasks.set(
        taskId,
        new TaskEntry(task.name, "null", null, "singleton"),
      );
    }

    const claimed = this.claimTask(
      task.name,
      "singleton",
      workerId,
      leaseExpirationMs,
      taskId,
    );

    return {
      taskId: claimed.taskId,
      taskPayload: undefined,
      leaseExpirationMs: claimed.leaseExpirationMs,
    };
  }

  async renew(
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<RenewedTaskLease> {
    const entry = this.tasks.get(taskId);
    if (!entry || entry.workerId !== workerId) {
      throw new LeaseLostError();
    }

    const nextEntry = entry.withState(workerId, leaseExpirationMs);
    this.tasks.set(taskId, nextEntry);
    this.emitSignal(nextEntry.taskName, nextEntry.signal(taskId));
    return { newExpirationMs: leaseExpirationMs };
  }

  async fail(
    workerId: number,
    taskId: number,
    availableFromMs: number | null,
  ): Promise<FailedTask> {
    const entry = this.tasks.get(taskId);
    if (!entry || entry.workerId !== workerId) {
      throw new LeaseLostError();
    }

    const nextEntry = entry.withState(null, availableFromMs);
    this.tasks.set(taskId, nextEntry);
    this.emitSignal(nextEntry.taskName, nextEntry.signal(taskId));
    return { taskId };
  }

  async finish<TTask extends TaskDefinition>(
    task: TTask,
    workerId: number,
    taskId: number,
    callbackPayload: TaskCallback<TTask>,
  ): Promise<FinishedTask> {
    const entry = this.tasks.get(taskId);
    if (!entry || entry.workerId !== workerId) {
      throw new LeaseLostError();
    }

    const callbackPayloadJson = task.callbackCodec.encode(callbackPayload);

    if (entry.kind === "singleton") {
      const nextEntry = entry.withState(null, null);
      this.tasks.set(taskId, nextEntry);
      this.emitSignal(nextEntry.taskName, nextEntry.signal(taskId));
      this.deliverCallback(nextEntry.callbackId, callbackPayloadJson);
      return { taskId };
    }

    this.tasks.delete(taskId);
    this.deliverCallback(entry.callbackId, callbackPayloadJson);
    return { taskId };
  }

  private async publishInternal<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
    callbackId: string | null,
  ): Promise<PublishedTask> {
    const taskId = this.nextTaskId;
    this.nextTaskId += 1;
    this.tasks.set(
      taskId,
      new TaskEntry(
        task.name,
        task.codec.encode(payload),
        callbackId,
        "publish",
      ),
    );

    this.emitSignal(task.name, newTaskAvailable(taskId, Date.now()));

    return { taskId };
  }

  private claimTask(
    taskName: string,
    kind: "publish" | "singleton",
    workerId: number,
    leaseExpirationMs: number,
    taskId: number | null,
  ): {
    readonly taskId: number;
    readonly payloadJson: string;
    readonly leaseExpirationMs: number;
  } {
    const now = Date.now();
    const selectedTaskId =
      taskId ?? this.findEarliestClaimableTask(taskName, kind, now);

    if (selectedTaskId === undefined) {
      throw new TaskUnavailableError(
        this.findEarliestFutureTask(taskName, kind, now),
      );
    }

    const entry = this.tasks.get(selectedTaskId);
    if (!entry || entry.taskName !== taskName || entry.kind !== kind) {
      throw new TaskNotFoundError();
    }

    if (
      entry.workerId !== null &&
      entry.availableFromMs !== null &&
      entry.availableFromMs > now
    ) {
      throw new TaskLeasedError(entry.availableFromMs);
    }

    if (entry.availableFromMs !== null && entry.availableFromMs > now) {
      throw new TaskUnavailableError(entry.availableFromMs);
    }

    const nextEntry = entry.withState(workerId, leaseExpirationMs);
    this.tasks.set(selectedTaskId, nextEntry);
    this.emitSignal(nextEntry.taskName, nextEntry.signal(selectedTaskId));

    return {
      taskId: selectedTaskId,
      payloadJson: nextEntry.payloadJson,
      leaseExpirationMs,
    };
  }

  private findEarliestClaimableTask(
    taskName: string,
    kind: "publish" | "singleton",
    now: number,
  ): number | undefined {
    let earliestTaskId: number | undefined;
    let earliestAvailableFromMs = Number.POSITIVE_INFINITY;

    for (const [taskId, entry] of this.tasks) {
      if (entry.taskName !== taskName || entry.kind !== kind) {
        continue;
      }

      if (entry.availableFromMs !== null && entry.availableFromMs > now) {
        continue;
      }

      const availableFromMs = entry.availableFromMs ?? now;
      if (
        earliestTaskId === undefined ||
        availableFromMs < earliestAvailableFromMs ||
        (availableFromMs === earliestAvailableFromMs && taskId < earliestTaskId)
      ) {
        earliestTaskId = taskId;
        earliestAvailableFromMs = availableFromMs;
      }
    }

    return earliestTaskId;
  }

  private findEarliestFutureTask(
    taskName: string,
    kind: "publish" | "singleton",
    now: number,
  ): number | null {
    let earliest: number | null = null;

    for (const entry of this.tasks.values()) {
      if (entry.taskName !== taskName || entry.kind !== kind) {
        continue;
      }

      if (entry.availableFromMs === null || entry.availableFromMs <= now) {
        continue;
      }

      if (earliest === null || entry.availableFromMs < earliest) {
        earliest = entry.availableFromMs;
      }
    }

    return earliest;
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
