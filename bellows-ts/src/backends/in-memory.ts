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

interface ClaimEntry {
  readonly workerId: number;
  readonly leaseExpirationMs: number;
}

interface TaskEntry {
  readonly taskName: string;
  readonly payloadJson: string;
  readonly callbackId: string | null;
  claim: ClaimEntry | null;
  readonly kind: "publish" | "singleton";
}

export class InMemoryBackend implements Backend {
  private nextTaskId = 0;
  private readonly signals = new Map<string, SignalHub>();
  private readonly callbacks = new Map<string, CallbackSink>();
  private readonly tasks = new Map<number, TaskEntry>();

  async subscribe(task: TaskDefinition) {
    return this.signalForTask(task.name).subscribe();
  }

  async sweep(task: TaskDefinition): Promise<SweptTask[]> {
    const now = Date.now();
    const tasks: SweptTask[] = [];

    for (const [taskId, entry] of this.tasks) {
      if (entry.taskName !== task.name) {
        continue;
      }

      if (entry.claim && entry.claim.leaseExpirationMs > now) {
        continue;
      }

      tasks.push({ taskId });
    }

    return tasks;
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
    const entry = this.tasks.get(taskId);
    if (!entry || entry.taskName !== task.name || entry.kind !== "publish") {
      throw new TaskNotFoundError();
    }

    if (entry.claim && entry.claim.leaseExpirationMs > Date.now()) {
      throw new TaskLeasedError(entry.claim.leaseExpirationMs);
    }

    entry.claim = { workerId, leaseExpirationMs };

    return {
      taskId,
      taskPayload: task.codec.decode(entry.payloadJson),
      leaseExpirationMs,
    };
  }

  async claimSingleton<TCallback>(
    task: SingletonTaskDefinition<TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<undefined>> {
    const existingTask = [...this.tasks.entries()].find(
      ([, entry]) => entry.taskName === task.name && entry.kind === "singleton",
    );

    let taskId = existingTask?.[0];
    let entry = existingTask?.[1];

    if (taskId === undefined || entry === undefined) {
      taskId = this.nextTaskId;
      this.nextTaskId += 1;
      entry = {
        taskName: task.name,
        payloadJson: "null",
        callbackId: null,
        claim: null,
        kind: "singleton",
      };
      this.tasks.set(taskId, entry);
    }

    if (entry.claim && entry.claim.leaseExpirationMs > Date.now()) {
      throw new TaskLeasedError(entry.claim.leaseExpirationMs);
    }

    entry.claim = { workerId, leaseExpirationMs };

    return {
      taskId,
      taskPayload: undefined,
      leaseExpirationMs,
    };
  }

  async renew(
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<RenewedTaskLease> {
    const entry = this.tasks.get(taskId);
    if (!entry || !entry.claim || entry.claim.workerId !== workerId) {
      throw new LeaseLostError();
    }

    entry.claim = { workerId, leaseExpirationMs };
    return { newExpirationMs: leaseExpirationMs };
  }

  async finish<TTask extends TaskDefinition>(
    task: TTask,
    workerId: number,
    taskId: number,
    callbackPayload: TaskCallback<TTask>,
  ): Promise<FinishedTask> {
    const entry = this.tasks.get(taskId);
    if (!entry || !entry.claim || entry.claim.workerId !== workerId) {
      throw new LeaseLostError();
    }

    const callbackPayloadJson = task.callbackCodec.encode(callbackPayload);

    if (entry.kind === "singleton") {
      entry.claim = null;
      this.emitSignal(entry.taskName, newTaskAvailable(taskId));
      this.deliverCallback(entry.callbackId, callbackPayloadJson);
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
    this.tasks.set(taskId, {
      taskName: task.name,
      payloadJson: task.codec.encode(payload),
      callbackId,
      claim: null,
      kind: "publish",
    });

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

function newTaskAvailable(taskId: number): BackendSignal {
  return {
    type: "new-task-available",
    taskId,
  };
}
