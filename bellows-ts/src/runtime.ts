import {
  type Backend,
  LeaseLostError,
  type TaskCallback,
  type TaskDefinition,
  TaskFailure,
  TaskLeasedError,
  TaskNotFoundError,
  type TaskPayload,
  TaskUnavailableError,
  type WorkerFactory,
} from "./types.js";

const LEASE_DURATION_MS = 20_000;
const LEASE_RENEWAL_THRESHOLD_MS = 10_000;

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
    private readonly backend: Backend,
    private readonly factory: WorkerFactory<TTask>,
    private readonly workerId: number,
    private readonly onUpdate: (update: RuntimeUpdate) => void,
    private readonly onExit: () => void,
  ) {}

  run(dispatchToken: PublishDispatchToken | undefined): void {
    void this.runInternal(dispatchToken).finally(() => {
      this.onExit();
    });
  }

  private async runInternal(
    dispatchToken: PublishDispatchToken | undefined,
  ): Promise<void> {
    let taskId: number;
    let taskPayload: TaskPayload<TTask>;
    let leaseExpirationMs = Date.now() + LEASE_DURATION_MS;

    try {
      if (this.factory.task.kind === "publish") {
        if (dispatchToken === undefined) {
          return;
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
        return;
      }

      if (error instanceof TaskUnavailableError) {
        this.onUpdate({
          nextAvailableFromUpdate: { availableFromMs: error.availableFromMs },
          claimedTask: false,
        });
        return;
      }

      if (error instanceof TaskNotFoundError) {
        return;
      }

      return;
    }

    this.onUpdate({ nextAvailableFromUpdate: null, claimedTask: true });

    const workerPromise = this.factory
      .build(this.workerId)
      .process(taskId, taskPayload) as Promise<
      TaskFailure | TaskCallback<TTask>
    >;

    const workerResult = await this.waitForWorker(taskId, workerPromise, {
      getLeaseExpirationMs: () => leaseExpirationMs,
      setLeaseExpirationMs: (nextLeaseExpirationMs) => {
        leaseExpirationMs = nextLeaseExpirationMs;
      },
    });

    if (workerResult === null) {
      return;
    }

    if (workerResult instanceof TaskFailure) {
      try {
        await this.backend.fail(
          this.workerId,
          taskId,
          workerResult.availableFromMs,
        );
      } catch (error) {
        if (!(error instanceof LeaseLostError)) {
          // Ignore backend fail errors here to match the Rust runtime's exit behavior.
        }
      }

      this.onUpdate({ nextAvailableFromUpdate: null, claimedTask: false });
      return;
    }

    try {
      await this.backend.finish(
        this.factory.task,
        this.workerId,
        taskId,
        workerResult,
      );
    } catch (error) {
      if (!(error instanceof LeaseLostError)) {
        // Ignore backend finish errors here to match the Rust runtime's exit behavior.
      }
    }

    this.onUpdate({ nextAvailableFromUpdate: null, claimedTask: false });
  }

  private async waitForWorker(
    taskId: number,
    workerPromise: Promise<TaskFailure | TaskCallback<TTask>>,
    lease: {
      getLeaseExpirationMs: () => number;
      setLeaseExpirationMs: (leaseExpirationMs: number) => void;
    },
  ): Promise<TaskFailure | TaskCallback<TTask> | null> {
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

async function delay(durationMs: number): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });
}
