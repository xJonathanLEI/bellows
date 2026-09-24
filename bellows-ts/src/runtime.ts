import {
  LeaseLostError,
  type TaskCallback,
  type TaskDefinition,
  type TaskExecutionBackend,
  TaskFailure,
  TaskLeasedError,
  TaskNotFoundError,
  type TaskPayload,
  type TaskSuccess,
  TaskUnavailableError,
  type WorkerFactory,
} from "./types.js";

const LEASE_DURATION_MS = 20_000;
const LEASE_RENEWAL_THRESHOLD_MS = 10_000;

/** A host scheduling instruction, not a business success status. */
export type TaskAttemptOutcome =
  | { readonly type: "done" }
  | { readonly type: "retryAt"; readonly availableFromMs: number }
  | { readonly type: "retry" };

export interface RuntimeUpdate {
  readonly nextAvailableFromUpdate: {
    readonly availableFromMs: number | null;
  } | null;
  readonly claimedTask: boolean;
}

export type PublishDispatchToken =
  | { readonly type: "task"; readonly taskId: number }
  | { readonly type: "earliest-available" };

export class WorkerRuntime<TTask extends TaskDefinition> {
  constructor(
    private readonly backend: TaskExecutionBackend,
    private readonly factory: WorkerFactory<TTask>,
    private readonly workerId: number,
    private readonly onUpdate: (update: RuntimeUpdate) => void,
    private readonly onExit: () => void,
  ) {}

  run(dispatchToken: PublishDispatchToken | undefined): void {
    void this.runAndWait(dispatchToken);
  }

  async runAndWait(
    dispatchToken: PublishDispatchToken | undefined,
  ): Promise<TaskAttemptOutcome> {
    return this.runInternal(dispatchToken).finally(() => {
      this.onExit();
    });
  }

  private async runInternal(
    dispatchToken: PublishDispatchToken | undefined,
  ): Promise<TaskAttemptOutcome> {
    let taskId: number;
    let taskPayload: TaskPayload<TTask>;
    let leaseExpirationMs = Date.now() + LEASE_DURATION_MS;

    try {
      if (this.factory.task.kind === "publish") {
        if (dispatchToken === undefined) {
          return { type: "retry" };
        }

        const claimed =
          dispatchToken.type === "task"
            ? await this.backend.claimPublished(
                this.factory.task,
                this.workerId,
                dispatchToken.taskId,
                leaseExpirationMs,
              )
            : await this.backend.claimEarliestPublished(
                this.factory.task,
                this.workerId,
                leaseExpirationMs,
              );

        taskId = claimed.taskId;
        taskPayload = claimed.taskPayload as TaskPayload<TTask>;
        leaseExpirationMs = claimed.leaseExpirationMs;
      } else {
        const claimed = await this.backend.claimSingleton(
          this.factory.task,
          this.workerId,
          leaseExpirationMs,
        );
        taskId = claimed.taskId;
        taskPayload = undefined as TaskPayload<TTask>;
        leaseExpirationMs = claimed.leaseExpirationMs;
      }
    } catch (error) {
      if (error instanceof TaskLeasedError) {
        this.onUpdate({
          nextAvailableFromUpdate: { availableFromMs: error.expirationMs },
          claimedTask: false,
        });
        return { type: "retryAt", availableFromMs: error.expirationMs };
      }

      if (error instanceof TaskUnavailableError) {
        this.onUpdate({
          nextAvailableFromUpdate: { availableFromMs: error.availableFromMs },
          claimedTask: false,
        });
        return error.availableFromMs === null
          ? { type: "retry" }
          : { type: "retryAt", availableFromMs: error.availableFromMs };
      }

      if (error instanceof TaskNotFoundError) {
        return {
          type: this.factory.task.kind === "publish" ? "done" : "retry",
        };
      }

      return { type: "retry" };
    }

    this.onUpdate({ nextAvailableFromUpdate: null, claimedTask: true });

    const workerPromise = this.factory
      .build(this.workerId)
      .process(taskId, taskPayload);

    const workerResult = await this.waitForWorker(taskId, workerPromise, {
      getLeaseExpirationMs: () => leaseExpirationMs,
      setLeaseExpirationMs: (nextLeaseExpirationMs) => {
        leaseExpirationMs = nextLeaseExpirationMs;
      },
    });

    if (workerResult === null) {
      return { type: "retry" };
    }

    if (workerResult instanceof TaskFailure) {
      let outcome: TaskAttemptOutcome;
      try {
        await this.backend.fail(
          this.workerId,
          taskId,
          workerResult.availableFromMs,
        );
        outcome = {
          type: "retryAt",
          availableFromMs: workerResult.availableFromMs ?? Date.now(),
        };
      } catch {
        outcome = { type: "retry" };
      }

      this.onUpdate({ nextAvailableFromUpdate: null, claimedTask: false });
      return outcome;
    }

    let outcome: TaskAttemptOutcome;
    try {
      await this.backend.finish(
        this.factory.task,
        this.workerId,
        taskId,
        workerResult.callbackPayload,
        workerResult.availableFromMs,
      );
      outcome =
        workerResult.availableFromMs !== null ||
        this.factory.task.kind === "singleton"
          ? {
              type: "retryAt",
              availableFromMs: workerResult.availableFromMs ?? Date.now(),
            }
          : { type: "done" };
    } catch {
      outcome = { type: "retry" };
    }

    this.onUpdate({ nextAvailableFromUpdate: null, claimedTask: false });
    return outcome;
  }

  private async waitForWorker(
    taskId: number,
    workerPromise: Promise<TaskFailure | TaskSuccess<TaskCallback<TTask>>>,
    lease: {
      getLeaseExpirationMs: () => number;
      setLeaseExpirationMs: (leaseExpirationMs: number) => void;
    },
  ): Promise<TaskFailure | TaskSuccess<TaskCallback<TTask>> | null> {
    while (true) {
      const renewalDelayMs = Math.max(
        lease.getLeaseExpirationMs() - LEASE_RENEWAL_THRESHOLD_MS - Date.now(),
        0,
      );
      const renewalTimer = delay(renewalDelayMs);
      const result = await Promise.race([
        workerPromise
          .then((callbackPayload) => ({
            type: "worker-finished" as const,
            callbackPayload,
          }))
          .catch(() => ({
            type: "worker-finished" as const,
            callbackPayload: TaskFailure.retryImmediately(),
          })),
        renewalTimer.then(() => ({ type: "renew-lease" as const })),
      ]);

      if (result.type === "worker-finished") {
        return result.callbackPayload;
      }

      try {
        const renewed = await this.backend.renew(
          this.workerId,
          taskId,
          Date.now() + LEASE_DURATION_MS,
        );
        lease.setLeaseExpirationMs(renewed.newExpirationMs);
      } catch (error) {
        if (error instanceof LeaseLostError) {
          return null;
        }

        return null;
      }
    }
  }
}

/**
 * Claims and executes once, awaiting finalization before returning a scheduling instruction.
 * Backend/ownership uncertainty returns `retry`; no retries happen internally.
 * Lease loss ends the runtime, but does not cancel arbitrary business promises.
 */
export async function runTaskOnce<TTask extends TaskDefinition>(
  backend: TaskExecutionBackend,
  factory: WorkerFactory<TTask>,
  workerId: number,
  dispatchToken: TTask extends { readonly kind: "singleton" }
    ? undefined
    : PublishDispatchToken,
): Promise<TaskAttemptOutcome> {
  const runtime = new WorkerRuntime(
    backend,
    factory,
    workerId,
    () => {},
    () => {},
  );

  return runtime.runAndWait(dispatchToken);
}

async function delay(durationMs: number): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });
}
