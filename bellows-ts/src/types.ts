import type { BackendSignalSubscription } from "./internal/signal-hub.js";

export interface TaskCodec<TValue> {
  encode(value: TValue): string;
  decode(value: string): TValue;
}

function jsonTaskCodec<TValue>(): TaskCodec<TValue> {
  return {
    encode(value) {
      if (value === undefined) {
        return "null";
      }

      const encoded = JSON.stringify(value);
      if (encoded === undefined) {
        throw new Error("value is not JSON serializable");
      }

      return encoded;
    },
    decode(value) {
      if (value === "null") {
        return undefined as TValue;
      }

      return JSON.parse(value) as TValue;
    },
  };
}

export interface PublishTaskDefinition<TPayload, TCallback = undefined> {
  readonly kind: "publish";
  readonly name: string;
  readonly codec: TaskCodec<TPayload>;
  readonly callbackCodec: TaskCodec<TCallback>;
}

export interface SingletonTaskDefinition<TCallback = undefined> {
  readonly kind: "singleton";
  readonly name: string;
  readonly callbackCodec: TaskCodec<TCallback>;
}

export type TaskDefinition =
  | PublishTaskDefinition<unknown, unknown>
  | SingletonTaskDefinition<unknown>;

export type TaskPayload<TTask extends TaskDefinition> =
  TTask extends PublishTaskDefinition<infer TPayload, unknown>
    ? TPayload
    : undefined;

export type TaskCallback<TTask extends TaskDefinition> =
  TTask extends PublishTaskDefinition<unknown, infer TCallback>
    ? TCallback
    : TTask extends SingletonTaskDefinition<infer TCallback>
      ? TCallback
      : never;

export function definePublishTask<TPayload, TCallback = undefined>(
  name: string,
  codec: TaskCodec<TPayload> = jsonTaskCodec<TPayload>(),
  callbackCodec: TaskCodec<TCallback> = jsonTaskCodec<TCallback>(),
): PublishTaskDefinition<TPayload, TCallback> {
  return {
    kind: "publish",
    name,
    codec,
    callbackCodec,
  };
}

export function defineSingletonTask<TCallback = undefined>(
  name: string,
  callbackCodec: TaskCodec<TCallback> = jsonTaskCodec<TCallback>(),
): SingletonTaskDefinition<TCallback> {
  return {
    kind: "singleton",
    name,
    callbackCodec,
  };
}

export interface BackendSignal {
  readonly type: "new-task-available";
  readonly taskId: number | null;
  readonly availableFromMs: number;
}

export interface PublishedTask {
  readonly taskId: number;
}

export interface ClaimedTask<TPayload> {
  readonly taskId: number;
  readonly taskPayload: TPayload;
  readonly leaseExpirationMs: number;
}

export interface RenewedTaskLease {
  readonly newExpirationMs: number;
}

export interface FailedTask {
  readonly taskId: number;
}

export interface FinishedTask {
  readonly taskId: number;
}

export class TaskLeasedError extends Error {
  constructor(readonly expirationMs: number) {
    super(
      `task is currently leased until ${new Date(expirationMs).toISOString()}`,
    );
    this.name = "TaskLeasedError";
  }
}

export class TaskUnavailableError extends Error {
  constructor(readonly availableFromMs: number | null) {
    super(
      availableFromMs === null
        ? "no task is currently available"
        : `no task is currently available; next availability is ${new Date(availableFromMs).toISOString()}`,
    );
    this.name = "TaskUnavailableError";
  }
}

export class TaskNotFoundError extends Error {
  constructor() {
    super("task was not found");
    this.name = "TaskNotFoundError";
  }
}

export class LeaseLostError extends Error {
  constructor() {
    super("task lease was lost");
    this.name = "LeaseLostError";
  }
}

export class AwaitTaskError extends Error {
  constructor() {
    super("task finished without an observable callback notification");
    this.name = "AwaitTaskError";
  }
}

export class AwaitableTask<TCallback> {
  constructor(
    readonly taskId: number,
    private callbackPromise: Promise<TCallback> | null,
  ) {}

  wait(): Promise<TCallback> {
    if (this.callbackPromise === null) {
      return Promise.reject(new AwaitTaskError());
    }

    const callbackPromise = this.callbackPromise;
    this.callbackPromise = null;
    return callbackPromise;
  }
}

export class TaskFailure {
  constructor(readonly availableFromMs: number | null) {}

  static retryImmediately(): TaskFailure {
    return new TaskFailure(null);
  }

  static retryAt(availableFromMs: number): TaskFailure {
    return new TaskFailure(availableFromMs);
  }
}

export type TaskResult<TCallback> = TCallback | TaskFailure;

export interface Backend {
  subscribe(task: TaskDefinition): Promise<BackendSignalSubscription>;
  publish<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
  ): Promise<PublishedTask>;
  publishAwaitable<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
  ): Promise<AwaitableTask<TCallback>>;
  claimPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>>;
  claimEarliestPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>>;
  claimSingleton<TCallback>(
    task: SingletonTaskDefinition<TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<undefined>>;
  renew(
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<RenewedTaskLease>;
  fail(
    workerId: number,
    taskId: number,
    availableFromMs: number | null,
  ): Promise<FailedTask>;
  finish<TTask extends TaskDefinition>(
    task: TTask,
    workerId: number,
    taskId: number,
    callbackPayload: TaskCallback<TTask>,
  ): Promise<FinishedTask>;
}

export interface Worker<TTask extends TaskDefinition> {
  process(
    taskId: number,
    taskPayload: TaskPayload<TTask>,
  ): Promise<TaskResult<TaskCallback<TTask>>>;
}

export interface WorkerFactory<TTask extends TaskDefinition> {
  readonly task: TTask;
  build(workerId: number): Worker<TTask>;
}
