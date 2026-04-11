import {
  type Backend,
  LeaseLostError,
  type TaskCallback,
  type TaskDefinition,
  TaskLeasedError,
  TaskNotFoundError,
  type TaskPayload,
  type WorkerFactory,
} from "./types.js";

const LEASE_DURATION_MS = 20_000;
const LEASE_RENEWAL_THRESHOLD_MS = 10_000;

export class WorkerRuntime<TTask extends TaskDefinition> {
  constructor(
    private readonly backend: Backend,
    private readonly factory: WorkerFactory<TTask>,
    private readonly workerId: number,
    private readonly onFinished: () => void,
  ) {}

  run(dispatchToken: number | undefined): void {
    void this.runInternal(dispatchToken).finally(() => {
      this.onFinished();
    });
  }

  private async runInternal(dispatchToken: number | undefined): Promise<void> {
    let taskId: number;
    let taskPayload: TaskPayload<TTask>;
    let leaseExpirationMs = Date.now() + LEASE_DURATION_MS;

    try {
      if (this.factory.task.kind === "publish") {
        if (dispatchToken === undefined) {
          return;
        }

        const claimed = await this.backend.claimPublished(
          this.factory.task,
          this.workerId,
          dispatchToken,
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
      if (
        error instanceof TaskLeasedError ||
        error instanceof TaskNotFoundError
      ) {
        return;
      }

      throw error;
    }

    const workerPromise = this.factory
      .build(this.workerId)
      .process(taskId, taskPayload) as Promise<TaskCallback<TTask>>;

    const callbackPayload = await this.waitForWorker(taskId, workerPromise, {
      getLeaseExpirationMs: () => leaseExpirationMs,
      setLeaseExpirationMs: (nextLeaseExpirationMs) => {
        leaseExpirationMs = nextLeaseExpirationMs;
      },
    });

    if (callbackPayload === null) {
      return;
    }

    try {
      await this.backend.finish(
        this.factory.task,
        this.workerId,
        taskId,
        callbackPayload,
      );
    } catch (error) {
      if (error instanceof LeaseLostError) {
        return;
      }

      throw error;
    }
  }

  private async waitForWorker(
    taskId: number,
    workerPromise: Promise<TaskCallback<TTask>>,
    lease: {
      getLeaseExpirationMs: () => number;
      setLeaseExpirationMs: (leaseExpirationMs: number) => void;
    },
  ): Promise<TaskCallback<TTask> | null> {
    while (true) {
      const renewalDelayMs = Math.max(
        lease.getLeaseExpirationMs() - LEASE_RENEWAL_THRESHOLD_MS - Date.now(),
        0,
      );
      const renewalTimer = delay(renewalDelayMs);
      const result = await Promise.race([
        workerPromise.then((callbackPayload) => ({
          type: "worker-finished" as const,
          callbackPayload,
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

        throw error;
      }
    }
  }
}

async function delay(durationMs: number): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });
}
