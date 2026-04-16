import {
  type PublishDispatchToken,
  type RuntimeUpdate,
  WorkerRuntime,
} from "./runtime.js";
import type {
  Backend,
  BackendSignal,
  TaskDefinition,
  WorkerFactory,
} from "./types.js";

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

type RuntimeReport =
  | {
      readonly type: "update";
      readonly update: RuntimeUpdate;
      readonly clearsEarliestClaim: boolean;
    }
  | {
      readonly type: "exited";
      readonly clearsEarliestClaim: boolean;
    };

type DispatcherEvent =
  | { readonly type: "drain" }
  | { readonly type: "report"; readonly report: RuntimeReport }
  | { readonly type: "signal"; readonly signal: BackendSignal }
  | { readonly type: "subscription-closed" }
  | { readonly type: "timer" };

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

    void this.run(handle, subscription);

    return handle;
  }

  private async run(
    handle: WorkerDispatcherHandle,
    subscription: Awaited<ReturnType<Backend["subscribe"]>>,
  ): Promise<void> {
    const events = new EventQueue();

    void handle.waitForDrainRequest().then(() => {
      events.push({ type: "drain" });
    });

    void (async () => {
      while (true) {
        const signal = await subscription.recv();
        if (signal === null) {
          events.push({ type: "subscription-closed" });
          return;
        }

        events.push({ type: "signal", signal });
      }
    })();

    const state = new DaemonState();

    while (true) {
      if (
        !state.draining &&
        state.shouldDispatchEarliestNow() &&
        !state.earliestClaimInFlight
      ) {
        this.dispatchEarliestClaim(events, state);
        continue;
      }

      const event = await events.shift(state.earliestAvailableFromMs);
      if (event === null || event.type === "subscription-closed") {
        handle.markDrained();
        return;
      }

      if (event.type === "drain") {
        if (state.pendingWorkers === 0) {
          subscription.close();
          handle.markDrained();
          return;
        }

        state.draining = true;
        continue;
      }

      if (event.type === "timer") {
        continue;
      }

      if (event.type === "signal") {
        this.handleSignal(event.signal, events, state);
        continue;
      }

      this.handleReport(event.report, subscription, handle, state);
      if (state.finished) {
        return;
      }
    }
  }

  private handleSignal(
    signal: BackendSignal,
    events: EventQueue,
    state: DaemonState,
  ): void {
    const now = Date.now();
    const dispatchToken =
      this.factory.task.kind === "publish"
        ? tryDispatchFromSignal(this.factory.task.kind, signal, now)
        : tryDispatchFromSignal(this.factory.task.kind, signal, now);

    if (dispatchToken !== null) {
      this.dispatchTask(dispatchToken, events, state, false);
      return;
    }

    state.noteEarliestAvailableFrom(signal.availableFromMs);
  }

  private dispatchEarliestClaim(events: EventQueue, state: DaemonState): void {
    state.earliestClaimInFlight = true;
    state.earliestAvailableFromMs = null;
    this.dispatchTask(
      nextAvailableDispatchToken(this.factory.task.kind),
      events,
      state,
      true,
    );
  }

  private dispatchTask(
    dispatchToken: PublishDispatchToken | undefined,
    events: EventQueue,
    state: DaemonState,
    clearsEarliestClaim: boolean,
  ): void {
    if (state.draining) {
      return;
    }

    const runtime = new WorkerRuntime(
      this.backend,
      this.factory,
      state.nextWorkerId,
      (update) => {
        events.push({
          type: "report",
          report: {
            type: "update",
            update,
            clearsEarliestClaim,
          },
        });
        clearsEarliestClaim = false;
      },
      () => {
        events.push({
          type: "report",
          report: {
            type: "exited",
            clearsEarliestClaim,
          },
        });
      },
    );

    state.pendingWorkers += 1;
    state.nextWorkerId += 1;
    runtime.run(dispatchToken);
  }

  private handleReport(
    report: RuntimeReport,
    subscription: Awaited<ReturnType<Backend["subscribe"]>>,
    handle: WorkerDispatcherHandle,
    state: DaemonState,
  ): void {
    if (report.type === "update") {
      if (report.clearsEarliestClaim) {
        state.earliestClaimInFlight = false;
      }

      if (report.clearsEarliestClaim && report.update.claimedTask) {
        state.noteEarliestAvailableFrom(Date.now());
      }

      if (report.update.nextAvailableFromUpdate !== null) {
        const { availableFromMs } = report.update.nextAvailableFromUpdate;
        if (
          availableFromMs !== null ||
          state.earliestAvailableFromMs === null
        ) {
          state.noteEarliestAvailableFrom(availableFromMs);
        }
      }

      return;
    }

    if (report.clearsEarliestClaim) {
      state.earliestClaimInFlight = false;
    }

    if (state.pendingWorkers > 0) {
      state.pendingWorkers -= 1;
    }

    if (state.draining && state.pendingWorkers === 0) {
      subscription.close();
      handle.markDrained();
      state.finished = true;
    }
  }
}

class DaemonState {
  draining = false;
  pendingWorkers = 0;
  nextWorkerId = 0;
  earliestAvailableFromMs: number | null = Date.now();
  earliestClaimInFlight = false;
  finished = false;

  shouldDispatchEarliestNow(): boolean {
    return (
      this.earliestAvailableFromMs !== null &&
      this.earliestAvailableFromMs <= Date.now()
    );
  }

  noteEarliestAvailableFrom(availableFromMs: number | null): void {
    if (availableFromMs === null) {
      this.earliestAvailableFromMs = null;
      return;
    }

    if (
      this.earliestAvailableFromMs === null ||
      availableFromMs < this.earliestAvailableFromMs
    ) {
      this.earliestAvailableFromMs = availableFromMs;
    }
  }
}

class EventQueue {
  private readonly items: DispatcherEvent[] = [];
  private readonly waiters: Array<(value: DispatcherEvent | null) => void> = [];

  push(event: DispatcherEvent): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(event);
      return;
    }

    this.items.push(event);
  }

  async shift(availableFromMs: number | null): Promise<DispatcherEvent | null> {
    const item = this.items.shift();
    if (item !== undefined) {
      return item;
    }

    let resolveEvent: ((value: DispatcherEvent | null) => void) | null = null;
    const eventPromise = new Promise<DispatcherEvent | null>((resolve) => {
      resolveEvent = resolve;
      this.waiters.push(resolve);
    });

    if (availableFromMs === null) {
      return await eventPromise;
    }

    const delayMs = Math.max(availableFromMs - Date.now(), 0);
    const result = await Promise.race([
      eventPromise,
      delay(delayMs).then(() => ({ type: "timer" as const })),
    ]);

    if (result !== null && result.type === "timer" && resolveEvent !== null) {
      const waiterIndex = this.waiters.indexOf(resolveEvent);
      if (waiterIndex !== -1) {
        this.waiters.splice(waiterIndex, 1);
      }
    }

    return result;
  }
}

function tryDispatchFromSignal(
  taskKind: TaskDefinition["kind"],
  signal: BackendSignal,
  now: number,
): PublishDispatchToken | undefined | null {
  if (taskKind === "singleton") {
    return null;
  }

  if (signal.availableFromMs > now || signal.taskId === null) {
    return null;
  }

  return { type: "task", taskId: signal.taskId };
}

function nextAvailableDispatchToken(
  taskKind: TaskDefinition["kind"],
): PublishDispatchToken | undefined {
  if (taskKind === "singleton") {
    return undefined;
  }

  return { type: "earliest-available" };
}

async function delay(durationMs: number): Promise<void> {
  await new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });
}
