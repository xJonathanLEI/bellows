import { AsyncQueue } from "./internal/async-queue.js";
import { WorkerRuntime } from "./runtime.js";
import type { Backend, TaskDefinition, WorkerFactory } from "./types.js";

class Deferred {
  readonly promise: Promise<void>;
  private resolvePromise!: () => void;

  constructor() {
    this.promise = new Promise<void>((resolve) => {
      this.resolvePromise = resolve;
    });
  }

  resolve(): void {
    this.resolvePromise();
  }
}

type DispatcherEvent =
  | { readonly type: "drain" }
  | { readonly type: "finished" }
  | { readonly type: "signal"; readonly taskId: number }
  | { readonly type: "subscription-closed" };

export class WorkerDispatcherHandle {
  private drainingStarted = false;
  private readonly drainRequested = new Deferred();
  private readonly drained = new Deferred();

  requestDrain(): void {
    if (this.drainingStarted) {
      return;
    }

    this.drainingStarted = true;
    this.drainRequested.resolve();
  }

  async waitForDrainRequest(): Promise<void> {
    await this.drainRequested.promise;
  }

  markDrained(): void {
    this.drained.resolve();
  }

  async drain(): Promise<void> {
    this.requestDrain();
    await this.drained.promise;
  }
}

export class WorkerDispatcher<TTask extends TaskDefinition> {
  constructor(
    private readonly backend: Backend,
    private readonly factory: WorkerFactory<TTask>,
  ) {}

  async launch(): Promise<WorkerDispatcherHandle> {
    const handle = new WorkerDispatcherHandle();
    const subscription = await this.backend.subscribe(this.factory.task);
    const startupTasks = await this.backend.sweep(this.factory.task);

    void this.run(
      handle,
      subscription,
      startupTasks.map(({ taskId }) => taskId),
    );

    return handle;
  }

  private async run(
    handle: WorkerDispatcherHandle,
    subscription: Awaited<ReturnType<Backend["subscribe"]>>,
    startupTaskIds: number[],
  ): Promise<void> {
    const startupQueue =
      this.factory.task.kind === "publish" ? [...startupTaskIds] : [undefined];
    const events = new AsyncQueue<DispatcherEvent>();

    let drainRequested = false;

    void handle.waitForDrainRequest().then(() => {
      drainRequested = true;
      events.push({ type: "drain" });
    });

    void (async () => {
      while (true) {
        const signal = await subscription.recv();
        if (signal === null) {
          events.push({ type: "subscription-closed" });
          return;
        }

        events.push({ type: "signal", taskId: signal.taskId });
      }
    })();

    let draining = false;
    let pendingWorkers = 0;
    let nextWorkerId = 0;
    let singletonRedispatchQueued = false;

    const dispatchTask = (dispatchToken: number | undefined): void => {
      if (draining) {
        return;
      }

      if (this.factory.task.kind === "singleton" && pendingWorkers > 0) {
        // Singleton tasks should only have one runtime in flight at a time. Unlike
        // published tasks, repeated availability signals all point at the same
        // backend-managed task instance. If a signal arrives before the current runtime
        // reports its completion back to the dispatcher, remember that signal so the
        // singleton is re-dispatched after the in-flight runtime finishes.
        singletonRedispatchQueued = true;
        return;
      }

      pendingWorkers += 1;
      const runtime = new WorkerRuntime(
        this.backend,
        this.factory,
        nextWorkerId,
        () => {
          events.push({ type: "finished" });
        },
      );
      nextWorkerId += 1;
      runtime.run(dispatchToken);
    };

    while (true) {
      if (!draining && startupQueue.length > 0) {
        dispatchTask(startupQueue.shift());
        continue;
      }

      if (!draining && drainRequested) {
        if (pendingWorkers === 0) {
          subscription.close();
          handle.markDrained();
          return;
        }

        draining = true;
        continue;
      }

      const event = await events.shift();
      if (event === null) {
        handle.markDrained();
        return;
      }

      if (event.type === "drain") {
        if (pendingWorkers === 0) {
          subscription.close();
          handle.markDrained();
          return;
        }

        draining = true;
        continue;
      }

      if (event.type === "finished") {
        pendingWorkers -= 1;

        if (
          !draining &&
          this.factory.task.kind === "singleton" &&
          pendingWorkers === 0 &&
          singletonRedispatchQueued
        ) {
          singletonRedispatchQueued = false;
          dispatchTask(undefined);
        }

        if (draining && pendingWorkers === 0) {
          subscription.close();
          handle.markDrained();
          return;
        }

        continue;
      }

      if (event.type === "subscription-closed") {
        handle.markDrained();
        return;
      }

      if (this.factory.task.kind === "publish") {
        dispatchTask(event.taskId);
      } else {
        dispatchTask(undefined);
      }
    }
  }
}
