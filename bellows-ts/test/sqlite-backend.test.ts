import { afterEach, expect, test } from "vitest";
import {
  definePublishTask,
  defineSingletonTask,
  SqliteBackend,
  WorkerDispatcher,
  type WorkerFactory,
} from "../src/index.js";
import {
  AsyncChannel,
  assertNamesEchoed,
  Gate,
  type ProcessedTask,
  TestSqliteDatabase,
} from "./helpers.js";

const echoTask = definePublishTask<{ name: string }, string>("echo");
const ackTask = definePublishTask<void>("ack");
const singletonTask = defineSingletonTask("singleton_echo");

const resources: Array<{ close: () => Promise<void> | void }> = [];

afterEach(async () => {
  for (const resource of resources.splice(0)) {
    await resource.close();
  }
});

test("sqlite backend", async () => {
  const database = track(new TestSqliteDatabase());
  const backend = track(await SqliteBackend.connect(database.url));
  await backend.initialize();
  const processed = track(new AsyncChannel<ProcessedTask>());

  const dispatcher = new WorkerDispatcher(
    backend,
    createEchoWorkerFactory(processed),
  );
  const dispatcherHandle = await dispatcher.launch();

  await backend.publish(echoTask, { name: "Alice" });
  await backend.publish(echoTask, { name: "Bob" });
  await backend.publish(echoTask, { name: "Charlie" });

  await assertNamesEchoed(processed, ["Alice", "Bob", "Charlie"]);

  await dispatcherHandle.drain();
  processed.close();

  expect(await processed.recv()).toBeNull();
});

test("sqlite publish awaitable returns typed callback", async () => {
  const database = track(new TestSqliteDatabase());
  const backend = track(await SqliteBackend.connect(database.url));
  await backend.initialize();
  const processed = track(new AsyncChannel<ProcessedTask>());

  const dispatcher = new WorkerDispatcher(
    backend,
    createEchoWorkerFactory(processed),
  );
  const dispatcherHandle = await dispatcher.launch();

  const awaitableTask = await backend.publishAwaitable(echoTask, {
    name: "Alice",
  });

  expect(await awaitableTask.wait()).toBe("Alice");
  expect((await processed.recv())?.name).toBe("Alice");

  await dispatcherHandle.drain();
});

test("sqlite publish awaitable supports unit callback", async () => {
  const database = track(new TestSqliteDatabase());
  const backend = track(await SqliteBackend.connect(database.url));
  await backend.initialize();
  const processed = track(new AsyncChannel<number>());

  const dispatcher = new WorkerDispatcher(
    backend,
    createAckWorkerFactory(processed),
  );
  const dispatcherHandle = await dispatcher.launch();

  const awaitableTask = await backend.publishAwaitable(ackTask, undefined);

  expect(await awaitableTask.wait()).toBeUndefined();
  expect(await processed.recv()).toBe(awaitableTask.taskId);

  await dispatcherHandle.drain();
});

test("sqlite singleton task dispatch", async () => {
  const database = track(new TestSqliteDatabase());
  const backend = track(await SqliteBackend.connect(database.url));
  await backend.initialize();
  const processed = track(new AsyncChannel<number>());
  const gate = new Gate();

  const dispatcher = new WorkerDispatcher(
    backend,
    createSingletonWorkerFactory(processed, gate),
  );
  const dispatcherHandle = await dispatcher.launch();

  const firstTaskId = await processed.recv();
  expect(firstTaskId).toBeTypeOf("number");
  expect(firstTaskId).toBeGreaterThan(0);

  gate.release();

  const secondTaskId = await processed.recv();
  expect(secondTaskId).toBe(firstTaskId);

  const drainPromise = dispatcherHandle.drain();
  gate.release();
  await drainPromise;

  expect(processed.tryRecv()).toBeNull();
});

test("sqlite sweeping", async () => {
  const database = track(new TestSqliteDatabase());
  const backend = track(await SqliteBackend.connect(database.url));
  await backend.initialize();
  const processed = track(new AsyncChannel<ProcessedTask>());

  await backend.publish(echoTask, { name: "Alice" });

  const dispatcher = new WorkerDispatcher(
    backend,
    createEchoWorkerFactory(processed),
  );
  const dispatcherHandle = await dispatcher.launch();

  await backend.publish(echoTask, { name: "Bob" });
  await backend.publish(echoTask, { name: "Charlie" });

  await assertNamesEchoed(processed, ["Alice", "Bob", "Charlie"]);

  await dispatcherHandle.drain();
  processed.close();

  expect(await processed.recv()).toBeNull();
});

function createEchoWorkerFactory(
  processed: AsyncChannel<ProcessedTask>,
): WorkerFactory<typeof echoTask> {
  return {
    task: echoTask,
    build() {
      return {
        async process(taskId, taskPayload) {
          processed.send({ taskId, name: taskPayload.name });
          return taskPayload.name;
        },
      };
    },
  };
}

function createAckWorkerFactory(
  processed: AsyncChannel<number>,
): WorkerFactory<typeof ackTask> {
  return {
    task: ackTask,
    build() {
      return {
        async process(taskId) {
          processed.send(taskId);
        },
      };
    },
  };
}

function createSingletonWorkerFactory(
  processed: AsyncChannel<number>,
  gate: Gate,
): WorkerFactory<typeof singletonTask> {
  return {
    task: singletonTask,
    build() {
      return {
        async process(taskId) {
          processed.send(taskId);
          await gate.wait();
        },
      };
    },
  };
}

function track<
  T extends { close?: () => Promise<void> | void; cleanup?: () => void },
>(resource: T): T {
  resources.push({
    close: async () => {
      resource.close?.();
      resource.cleanup?.();
    },
  });
  return resource;
}
