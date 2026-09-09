import { expect, test, vi } from "vitest";
import { InMemoryBackend } from "../src/backends/in-memory.js";
import {
  definePublishTask,
  defineSingletonTask,
  LeaseLostError,
  runTaskOnce,
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
import { Gate } from "./helpers.js";

const blockingTask = definePublishTask<void>("runtime_once_blocking");

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
  await execution;

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
  await expect(execution).resolves.toBeUndefined();
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
  // TypeScript accepts a published token but ignores it for singleton activation.
  await runTaskOnce(executionOnly(backend), factory, 17, {
    type: "earliest-available",
  });
  expect(factory.build).toHaveBeenCalledExactlyOnceWith(17);
  const claimed = await backend.claimSingleton(task, 18, Date.now() + 60_000);
  expect(process).toHaveBeenCalledExactlyOnceWith(claimed.taskId, undefined);
});

test("missing, leased, and unavailable claims never construct workers", async () => {
  const backend = new InMemoryBackend();
  const leased = await backend.publish(blockingTask, undefined);
  await backend.claimPublished(
    blockingTask,
    18,
    leased.taskId,
    Date.now() + 60_000,
  );
  const future = await backend.publishFuture(
    blockingTask,
    undefined,
    Date.now() + 60_000,
  );
  const { factory } = gatedFactory();
  for (const taskId of [
    Number.MAX_SAFE_INTEGER,
    leased.taskId,
    future.taskId,
  ]) {
    await expect(
      runTaskOnce(backend, factory, 17, { type: "task", taskId }),
    ).resolves.toBeUndefined();
  }
  await runTaskOnce(backend, factory, 17, { type: "earliest-available" });
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
  await runTaskOnce(backend, first.factory, 17, token);
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
])("backend %s errors return void", async (operation) => {
  const inner = new InMemoryBackend();
  const published = await inner.publish(blockingTask, undefined);
  const backend = executionOnly(inner);
  const error = new Error(`injected ${operation} error`);
  if (operation === "claim") {
    backend.claimPublished = vi.fn().mockRejectedValue(error);
  } else if (operation === "finish") {
    backend.finish = vi.fn().mockRejectedValue(error);
  } else {
    backend.fail = vi.fn().mockRejectedValue(error);
  }
  const { factory, gate } = gatedFactory(
    operation === "fail"
      ? TaskFailure.retryImmediately()
      : TaskSuccess.done(undefined),
  );
  gate.release();
  await expect(
    runTaskOnce(backend, factory, 17, {
      type: "task",
      taskId: published.taskId,
    }),
  ).resolves.toBeUndefined();
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
  await expect(execution).resolves.toBeUndefined();
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
  await expect(execution).resolves.toBeUndefined();
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
