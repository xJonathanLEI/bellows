import { expect, expectTypeOf, test, vi } from "vitest";
import { InMemoryBackend } from "../src/backends/in-memory.js";
import {
  definePublishTask,
  defineSingletonTask,
  LeaseLostError,
  runTaskOnce,
  type TaskAttemptOutcome,
  type TaskExecutionBackend,
  TaskFailure,
  TaskLeasedError,
  TaskNotFoundError,
  type TaskPayload,
  type TaskResult,
  TaskSuccess,
  TaskUnavailableError,
  type WorkerFactory,
} from "../src/index.js";
import { type PublishDispatchToken, WorkerRuntime } from "../src/runtime.js";
import { Gate } from "./helpers.js";

const blockingTask = definePublishTask<void>("runtime_once_blocking");

test("runtime exit callback runs once on an outcome and on an uncaught factory error", async () => {
  for (const throws of [false, true]) {
    const backend = new InMemoryBackend();
    const task = await backend.publish(blockingTask, undefined);
    const onExit = vi.fn();
    const error = new Error("injected factory error");
    const runtime = new WorkerRuntime(
      backend,
      {
        task: blockingTask,
        build: () => {
          if (throws) throw error;
          return { process: async () => TaskSuccess.done(undefined) };
        },
      },
      17,
      () => {},
      onExit,
    );
    const execution = runtime.runAndWait({ type: "task", taskId: task.taskId });
    if (throws) await expect(execution).rejects.toBe(error);
    else await expect(execution).resolves.toEqual({ type: "done" });
    expect(onExit).toHaveBeenCalledTimes(1);
  }
});

// Deliberately exposes only execution operations, not publishing or subscriptions.
function executionOnly(inner: InMemoryBackend): TaskExecutionBackend {
  return {
    claimPublished: inner.claimPublished.bind(inner),
    claimEarliestPublished: inner.claimEarliestPublished.bind(inner),
    claimSingleton: inner.claimSingleton.bind(inner),
    renew: inner.renew.bind(inner),
    fail: inner.fail.bind(inner),
    finish: inner.finish.bind(inner),
  };
}

function gatedFactory(
  result: TaskResult<undefined> = TaskSuccess.done(undefined),
) {
  const started = new Gate();
  const gate = new Gate();
  const process = vi.fn(
    async (_taskId: number, _payload: TaskPayload<typeof blockingTask>) => {
      started.release();
      await gate.wait();
      return result;
    },
  );
  const factory: WorkerFactory<typeof blockingTask> = {
    task: blockingTask,
    build: vi.fn(() => ({ process })),
  };
  return { factory, started, gate, process };
}

test("runTaskOnce waits for worker completion and finalizes the task", async () => {
  const backend = new InMemoryBackend();
  const { factory, started, gate, process } = gatedFactory();

  const published = await backend.publish(blockingTask, undefined);
  const execution = runTaskOnce(backend, factory, 17, {
    type: "task",
    taskId: published.taskId,
  });

  await started.wait();
  expect(factory.build).toHaveBeenCalledWith(17);
  expect(process).toHaveBeenCalledWith(published.taskId, undefined);

  let completed = false;
  void execution.then(() => {
    completed = true;
  });
  await Promise.resolve();
  expect(completed).toBe(false);

  gate.release();
  await expect(execution).resolves.toEqual({ type: "done" });

  await expect(
    backend.claimPublished(
      blockingTask,
      18,
      published.taskId,
      Date.now() + 20_000,
    ),
  ).rejects.toThrow("task was not found");
});

test.each([
  false,
  true,
])("execution-only backend awaits finalization (failed=%s)", async (failed) => {
  const inner = new InMemoryBackend();
  const published = await inner.publish(blockingTask, undefined);
  const recording = new Gate();
  const recordingGate = new Gate();
  const backend: TaskExecutionBackend = {
    ...executionOnly(inner),
    async finish(task, workerId, taskId, callback, availableFrom) {
      recording.release();
      await recordingGate.wait();
      return inner.finish(task, workerId, taskId, callback, availableFrom);
    },
    async fail(workerId, taskId, availableFrom) {
      recording.release();
      await recordingGate.wait();
      return inner.fail(workerId, taskId, availableFrom);
    },
  };
  const { factory, gate } = gatedFactory(
    failed ? TaskFailure.retryImmediately() : TaskSuccess.done(undefined),
  );
  gate.release();
  const execution = runTaskOnce(backend, factory, 17, {
    type: "task",
    taskId: published.taskId,
  });
  let completed = false;
  void execution.then(() => {
    completed = true;
  });
  await recording.wait();
  expect(completed).toBe(false);
  await expect(
    inner.claimPublished(
      blockingTask,
      18,
      published.taskId,
      Date.now() + 60_000,
    ),
  ).rejects.toBeInstanceOf(TaskLeasedError);
  recordingGate.release();
  const outcome = await execution;
  if (failed) assertImmediate(outcome);
  else expect(outcome).toEqual({ type: "done" });
  const claim = inner.claimPublished(
    blockingTask,
    18,
    published.taskId,
    Date.now() + 60_000,
  );
  if (failed) {
    expect((await claim).taskId).toBe(published.taskId);
  } else {
    await expect(claim).rejects.toBeInstanceOf(TaskNotFoundError);
  }
});

test("earliest-available executes only one task", async () => {
  const backend = new InMemoryBackend();
  const first = await backend.publish(blockingTask, undefined);
  const second = await backend.publish(blockingTask, undefined);
  const { factory, gate, process } = gatedFactory();
  gate.release();
  await runTaskOnce(backend, factory, 17, { type: "earliest-available" });
  expect(factory.build).toHaveBeenCalledTimes(1);
  expect(process).toHaveBeenCalledWith(first.taskId, undefined);
  expect(
    (
      await backend.claimEarliestPublished(
        blockingTask,
        18,
        Date.now() + 60_000,
      )
    ).taskId,
  ).toBe(second.taskId);
});

test("singleton dispatch executes one backend-managed task", async () => {
  const backend = new InMemoryBackend();
  const task = defineSingletonTask("runtime_once_singleton");
  const process = vi.fn(async () => TaskSuccess.done(undefined));
  const factory = { task, build: vi.fn(() => ({ process })) };
  expectTypeOf<
    Parameters<typeof runTaskOnce<typeof task>>[3]
  >().toEqualTypeOf<undefined>();
  expectTypeOf<
    Parameters<typeof runTaskOnce<typeof blockingTask>>[3]
  >().toEqualTypeOf<PublishDispatchToken>();
  assertImmediate(
    await runTaskOnce(executionOnly(backend), factory, 17, undefined),
  );
  expect(factory.build).toHaveBeenCalledExactlyOnceWith(17);
  const claimed = await backend.claimSingleton(task, 18, Date.now() + 60_000);
  expect(process).toHaveBeenCalledExactlyOnceWith(claimed.taskId, undefined);
});

test("missing, leased, and unavailable claims never construct workers", async () => {
  const backend = new InMemoryBackend();
  const deadline = Date.now() + 60_000;
  const leased = await backend.publish(blockingTask, undefined);
  await backend.claimPublished(blockingTask, 18, leased.taskId, deadline);
  const future = await backend.publishFuture(blockingTask, undefined, deadline);
  const { factory } = gatedFactory();
  for (const [taskId, expected] of [
    [Number.MAX_SAFE_INTEGER, { type: "done" }],
    [leased.taskId, { type: "retryAt", availableFromMs: deadline }],
    [future.taskId, { type: "retryAt", availableFromMs: deadline }],
  ] as const) {
    await expect(
      runTaskOnce(backend, factory, 17, { type: "task", taskId }),
    ).resolves.toEqual(expected);
  }
  await expect(
    runTaskOnce(backend, factory, 17, { type: "earliest-available" }),
  ).resolves.toEqual({ type: "retryAt", availableFromMs: deadline });
  await expect(
    runTaskOnce(new InMemoryBackend(), factory, 17, {
      type: "earliest-available",
    }),
  ).resolves.toEqual({ type: "retry" });
  expect(factory.build).not.toHaveBeenCalled();
});

test.each([
  "immediate",
  "scheduled-failure",
  "rescheduled-success",
])("%s requires another external attempt", async (scenario) => {
  const backend = new InMemoryBackend();
  const published = await backend.publish(blockingTask, undefined);
  const token = { type: "task" as const, taskId: published.taskId };
  const availableFrom = Date.now() + 60_000;
  const result =
    scenario === "immediate"
      ? TaskFailure.retryImmediately()
      : scenario === "scheduled-failure"
        ? TaskFailure.retryAt(availableFrom)
        : TaskSuccess.scheduleNextRun(undefined, availableFrom);
  const first = gatedFactory(result);
  first.gate.release();
  const outcome = await runTaskOnce(backend, first.factory, 17, token);
  if (scenario === "immediate") assertImmediate(outcome);
  else
    expect(outcome).toEqual({
      type: "retryAt",
      availableFromMs: availableFrom,
    });
  expect(first.factory.build).toHaveBeenCalledTimes(1);
  const second = gatedFactory();
  second.gate.release();
  await runTaskOnce(backend, second.factory, 18, token);
  const claim = backend.claimPublished(
    blockingTask,
    19,
    published.taskId,
    Date.now() + 60_000,
  );
  if (scenario === "immediate") {
    expect(second.factory.build).toHaveBeenCalledTimes(1);
    await expect(claim).rejects.toBeInstanceOf(TaskNotFoundError);
  } else {
    expect(second.factory.build).not.toHaveBeenCalled();
    await expect(claim).rejects.toEqual(
      new TaskUnavailableError(availableFrom),
    );
  }
});

test.each([
  "claim",
  "finish",
  "fail",
  "finish-lost",
  "fail-lost",
])("backend %s errors remain retryable", async (operation) => {
  const inner = new InMemoryBackend();
  const published = await inner.publish(blockingTask, undefined);
  const backend = executionOnly(inner);
  const error = operation.endsWith("-lost")
    ? new LeaseLostError()
    : new Error(`injected ${operation} error`);
  if (operation === "claim") {
    backend.claimPublished = vi.fn().mockRejectedValue(error);
  } else if (operation.startsWith("finish")) {
    backend.finish = vi.fn().mockRejectedValue(error);
  } else {
    backend.fail = vi.fn().mockRejectedValue(error);
  }
  const { factory, gate } = gatedFactory(
    operation.startsWith("fail")
      ? TaskFailure.retryImmediately()
      : TaskSuccess.done(undefined),
  );
  gate.release();
  await expect(
    runTaskOnce(backend, factory, 17, {
      type: "task",
      taskId: published.taskId,
    }),
  ).resolves.toEqual({ type: "retry" });
  expect(factory.build).toHaveBeenCalledTimes(operation === "claim" ? 0 : 1);
  const claim = inner.claimPublished(
    blockingTask,
    18,
    published.taskId,
    Date.now() + 60_000,
  );
  if (operation === "claim") {
    expect((await claim).taskId).toBe(published.taskId);
  } else {
    await expect(claim).rejects.toBeInstanceOf(TaskLeasedError);
  }
});

test.each([
  "renewed",
  "lost",
  "error",
])("immediately due renewal: %s", async (renewal) => {
  const inner = new InMemoryBackend();
  const published = await inner.publish(blockingTask, undefined);
  const renewing = new Gate();
  const renewalGate = new Gate();
  const backend: TaskExecutionBackend = {
    ...executionOnly(inner),
    async claimPublished(task, workerId, taskId, expiration) {
      const claimed = await inner.claimPublished(
        task,
        workerId,
        taskId,
        expiration,
      );
      return { ...claimed, leaseExpirationMs: Date.now() };
    },
    async renew(workerId, taskId, expiration) {
      renewing.release();
      await renewalGate.wait();
      if (renewal === "lost") {
        throw new LeaseLostError();
      }
      if (renewal === "error") {
        throw new Error("injected renewal error");
      }
      return inner.renew(workerId, taskId, expiration);
    },
  };
  const finish = vi.spyOn(backend, "finish");
  const fail = vi.spyOn(backend, "fail");
  const { factory, gate, started, process } = gatedFactory();
  const execution = runTaskOnce(backend, factory, 17, {
    type: "task",
    taskId: published.taskId,
  });
  let completed = false;
  void execution.then(() => {
    completed = true;
  });
  await started.wait();
  await renewing.wait();
  expect(completed).toBe(false);
  renewalGate.release();
  if (renewal === "renewed") {
    gate.release();
  }
  await expect(execution).resolves.toEqual({
    type: renewal === "renewed" ? "done" : "retry",
  });
  if (renewal !== "renewed") {
    // JavaScript promises are not aborted: let the worker finish after the runtime exits.
    gate.release();
    await process.mock.results[0]?.value;
  }
  expect(fail).not.toHaveBeenCalled();
  expect(finish).toHaveBeenCalledTimes(renewal === "renewed" ? 1 : 0);
  const claim = inner.claimPublished(
    blockingTask,
    18,
    published.taskId,
    Date.now() + 60_000,
  );
  await expect(claim).rejects.toBeInstanceOf(
    renewal === "renewed" ? TaskNotFoundError : TaskLeasedError,
  );
});

test.each([
  "renewed",
  "lost",
  "error",
])("worker progresses during pending renewal: %s", async (renewal) => {
  const inner = new InMemoryBackend();
  const published = await inner.publish(blockingTask, undefined);
  const renewing = new Gate();
  const renewalGate = new Gate();
  const backend: TaskExecutionBackend = {
    ...executionOnly(inner),
    async claimPublished(task, workerId, taskId, expiration) {
      const claimed = await inner.claimPublished(
        task,
        workerId,
        taskId,
        expiration,
      );
      return { ...claimed, leaseExpirationMs: Date.now() };
    },
    async renew(workerId, taskId, expiration) {
      renewing.release();
      await renewalGate.wait();
      if (renewal === "lost") {
        throw new LeaseLostError();
      }
      if (renewal === "error") {
        throw new Error("injected renewal error");
      }
      return inner.renew(workerId, taskId, expiration);
    },
  };
  const finish = vi.spyOn(backend, "finish");
  const fail = vi.spyOn(backend, "fail");
  const { factory, gate, started, process } = gatedFactory();
  const execution = runTaskOnce(backend, factory, 17, {
    type: "task",
    taskId: published.taskId,
  });
  let completed = false;
  void execution.then(() => {
    completed = true;
  });
  await started.wait();
  await renewing.wait();
  gate.release();
  await process.mock.results[0]?.value;
  expect(completed).toBe(false);
  expect(finish).not.toHaveBeenCalled();
  expect(fail).not.toHaveBeenCalled();

  renewalGate.release();
  await expect(execution).resolves.toEqual({
    type: renewal === "renewed" ? "done" : "retry",
  });
  expect(fail).not.toHaveBeenCalled();
  expect(finish).toHaveBeenCalledTimes(renewal === "renewed" ? 1 : 0);
  await expect(
    inner.claimPublished(
      blockingTask,
      18,
      published.taskId,
      Date.now() + 60_000,
    ),
  ).rejects.toBeInstanceOf(
    renewal === "renewed" ? TaskNotFoundError : TaskLeasedError,
  );
});

function assertImmediate(outcome: TaskAttemptOutcome) {
  expect(outcome.type).toBe("retryAt");
  if (outcome.type !== "retryAt") throw new Error("expected immediate retry");
  expect(outcome.availableFromMs).toBeLessThanOrEqual(Date.now());
  expect(outcome.availableFromMs).toBeGreaterThan(Date.now() - 1_000);
}

test.each([
  [false, "committed"],
  [true, "committed"],
  [false, "error"],
  [true, "error"],
  [false, "lost"],
  [true, "lost"],
] as const)("scheduling waits for finalization and ownership: failed=%s, %s", async (failed, disposition) => {
  const inner = new InMemoryBackend();
  const published = await inner.publish(blockingTask, undefined);
  const recording = new Gate();
  const gate = new Gate();
  async function record() {
    recording.release();
    await gate.wait();
    if (disposition === "lost") throw new LeaseLostError();
    if (disposition === "error") throw new Error("injected finalization error");
  }
  const backend: TaskExecutionBackend = {
    ...executionOnly(inner),
    async finish(task, workerId, taskId, callback, available) {
      await record();
      return inner.finish(task, workerId, taskId, callback, available);
    },
    async fail(workerId, taskId, available) {
      await record();
      return inner.fail(workerId, taskId, available);
    },
  };
  const deadline = Date.now() + 60_000;
  const worker = gatedFactory(
    failed
      ? TaskFailure.retryAt(deadline)
      : TaskSuccess.scheduleNextRun(undefined, deadline),
  );
  worker.gate.release();
  const execution = runTaskOnce(backend, worker.factory, 17, {
    type: "task",
    taskId: published.taskId,
  });
  let completed = false;
  void execution.then(() => {
    completed = true;
  });
  await recording.wait();
  expect(completed).toBe(false);
  gate.release();
  await expect(execution).resolves.toEqual(
    disposition === "committed"
      ? { type: "retryAt", availableFromMs: deadline }
      : { type: "retry" },
  );
});

test("worker rejection awaits failure recording and requests immediate retry", async () => {
  const inner = new InMemoryBackend();
  const published = await inner.publish(blockingTask, undefined);
  const recording = new Gate();
  const gate = new Gate();
  const backend: TaskExecutionBackend = {
    ...executionOnly(inner),
    async fail(workerId, taskId, available) {
      recording.release();
      await gate.wait();
      return inner.fail(workerId, taskId, available);
    },
  };
  const factory = {
    task: blockingTask,
    build: vi.fn(() => ({
      process: async () => {
        throw new Error("injected worker rejection");
      },
    })),
  };
  const execution = runTaskOnce(backend, factory, 17, {
    type: "task",
    taskId: published.taskId,
  });
  let completed = false;
  void execution.then(() => {
    completed = true;
  });
  await recording.wait();
  expect(completed).toBe(false);
  gate.release();
  assertImmediate(await execution);
  expect(factory.build).toHaveBeenCalledTimes(1);
  expect(
    (
      await inner.claimPublished(
        blockingTask,
        18,
        published.taskId,
        Date.now() + 60_000,
      )
    ).taskId,
  ).toBe(published.taskId);
});
