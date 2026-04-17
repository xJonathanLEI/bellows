import { randomUUID } from "node:crypto";
import { Client } from "pg";
import { afterEach, expect, test } from "vitest";
import { PostgresBackend } from "../src/backends/postgres.js";
import {
  definePublishTask,
  defineSingletonTask,
  TaskFailure,
  TaskSuccess,
  WorkerDispatcher,
  type WorkerFactory,
} from "../src/index.js";
import {
  AsyncChannel,
  assertNamesEchoed,
  Gate,
  type ProcessedTask,
} from "./helpers.js";

const echoTask = definePublishTask<{ name: string }, string>("echo");
const ackTask = definePublishTask<void>("ack");
const singletonTask = defineSingletonTask("singleton_echo");
const blockingTask = definePublishTask<void>("blocking");
const retryTask = definePublishTask<void>("retry_once");
const reschedulingPublishedTask = definePublishTask<void, number>(
  "rescheduling_published",
);
const scheduledSingletonTask = defineSingletonTask("scheduled_singleton");
const adminDatabaseUrl =
  process.env.BELLOWS_TS_TEST_POSTGRES_URL ??
  "postgres://postgres:postgres@localhost:5432/postgres";

const resources: Array<{ close: () => Promise<void> | void }> = [];

afterEach(async () => {
  for (const resource of resources.splice(0).reverse()) {
    await resource.close();
  }
});

test("postgres backend", async () => {
  const database = track(await TestPostgresDatabase.create("backend"));
  const backend = track(await PostgresBackend.connect(database.url));
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

test("postgres publish awaitable returns typed callback", async () => {
  const database = track(await TestPostgresDatabase.create("awaitable_string"));
  const backend = track(await PostgresBackend.connect(database.url));
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

test("postgres publish awaitable supports unit callback", async () => {
  const database = track(await TestPostgresDatabase.create("awaitable_unit"));
  const backend = track(await PostgresBackend.connect(database.url));
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

test("postgres publish future delays task availability", async () => {
  const database = track(await TestPostgresDatabase.create("future_publish"));
  const backend = track(await PostgresBackend.connect(database.url));
  await backend.initialize();
  const processed = track(new AsyncChannel<ProcessedTask>());

  const dispatcher = new WorkerDispatcher(
    backend,
    createEchoWorkerFactory(processed),
  );
  const dispatcherHandle = await dispatcher.launch();

  const published = await backend.publishFuture(
    echoTask,
    { name: "Alice" },
    Date.now() + 200,
  );

  await sleep(50);
  expect(processed.tryRecv()).toBeNull();

  const received = await recvWithTimeout(processed);
  expect(received).toEqual({ taskId: published.taskId, name: "Alice" });

  await dispatcherHandle.drain();
});

test("postgres singleton task dispatch", async () => {
  const database = track(await TestPostgresDatabase.create("singleton"));
  const backend = track(await PostgresBackend.connect(database.url));
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

test("dispatcher drains multiple preexisting tasks without waiting", async () => {
  const database = track(
    await TestPostgresDatabase.create("drains_multiple_preexisting_tasks"),
  );
  const backend = track(await PostgresBackend.connect(database.url));
  await backend.initialize();
  const started = track(new AsyncChannel<number>());
  const gate = new Gate();

  const first = await backend.publish(blockingTask, undefined);
  const second = await backend.publish(blockingTask, undefined);

  const dispatcher = new WorkerDispatcher(
    backend,
    createBlockingWorkerFactory(started, gate),
  );
  const dispatcherHandle = await dispatcher.launch();

  const startedFirst = await recvWithTimeout(started);
  const startedSecond = await recvWithTimeout(started);

  expect([first.taskId, second.taskId]).toContain(startedFirst);
  expect([first.taskId, second.taskId]).toContain(startedSecond);
  expect(startedFirst).not.toBe(startedSecond);

  const drainPromise = dispatcherHandle.drain();
  gate.release();
  gate.release();
  await drainPromise;
});

test("postgres sweeping", async () => {
  const database = track(await TestPostgresDatabase.create("sweeping"));
  const backend = track(await PostgresBackend.connect(database.url));
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

test("worker failure is retried", async () => {
  const database = track(
    await TestPostgresDatabase.create("worker_failure_is_retried"),
  );
  const backend = track(await PostgresBackend.connect(database.url));
  await backend.initialize();
  const processed = track(new AsyncChannel<number>());
  let attempts = 0;

  const dispatcher = new WorkerDispatcher(
    backend,
    createRetryWorkerFactory(processed, () => attempts++),
  );
  const dispatcherHandle = await dispatcher.launch();

  const published = await backend.publish(retryTask, undefined);

  expect(await processed.recv()).toBe(published.taskId);
  expect(attempts).toBe(2);

  await dispatcherHandle.drain();
});

test("successful published task can schedule next run", async () => {
  const database = track(
    await TestPostgresDatabase.create("successful_published_schedule_next_run"),
  );
  const backend = track(await PostgresBackend.connect(database.url));
  await backend.initialize();
  const processed = track(new AsyncChannel<number>());
  let attempts = 0;
  const nextRunAtMs = Date.now() + 200;

  const dispatcher = new WorkerDispatcher(
    backend,
    createReschedulingPublishedWorkerFactory(
      processed,
      () => attempts++,
      nextRunAtMs,
    ),
  );
  const dispatcherHandle = await dispatcher.launch();

  const awaitableTask = await backend.publishAwaitable(
    reschedulingPublishedTask,
    undefined,
  );

  const firstTaskId = await recvWithTimeout(processed);
  expect(await awaitableTask.wait()).toBe(firstTaskId);

  await sleep(50);
  expect(processed.tryRecv()).toBeNull();

  const secondTaskId = await recvWithTimeout(processed);
  expect(secondTaskId).toBe(firstTaskId);
  expect(attempts).toBe(2);

  await dispatcherHandle.drain();
});

test("successful singleton task can schedule next run", async () => {
  const database = track(
    await TestPostgresDatabase.create("successful_singleton_schedule_next_run"),
  );
  const backend = track(await PostgresBackend.connect(database.url));
  await backend.initialize();
  const processed = track(new AsyncChannel<number>());
  const gate = new Gate();
  let attempts = 0;
  const nextRunAtMs = Date.now() + 200;

  const dispatcher = new WorkerDispatcher(
    backend,
    createScheduledSingletonWorkerFactory(
      processed,
      gate,
      () => attempts++,
      nextRunAtMs,
    ),
  );
  const dispatcherHandle = await dispatcher.launch();

  const firstTaskId = await recvWithTimeout(processed);

  await sleep(50);
  expect(processed.tryRecv()).toBeNull();

  const secondTaskId = await recvWithTimeout(processed);
  expect(secondTaskId).toBe(firstTaskId);
  expect(attempts).toBe(2);

  const drainPromise = dispatcherHandle.drain();
  gate.release();
  await drainPromise;

  expect(processed.tryRecv()).toBeNull();
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
          return TaskSuccess.done(taskPayload.name);
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
          return TaskSuccess.done(undefined);
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
          return TaskSuccess.done(undefined);
        },
      };
    },
  };
}

function createBlockingWorkerFactory(
  started: AsyncChannel<number>,
  gate: Gate,
): WorkerFactory<typeof blockingTask> {
  return {
    task: blockingTask,
    build() {
      return {
        async process(taskId) {
          started.send(taskId);
          await gate.wait();
          return TaskSuccess.done(undefined);
        },
      };
    },
  };
}

function createRetryWorkerFactory(
  processed: AsyncChannel<number>,
  recordAttempt: () => number,
): WorkerFactory<typeof retryTask> {
  return {
    task: retryTask,
    build() {
      return {
        async process(taskId) {
          const attempt = recordAttempt();
          if (attempt === 0) {
            return TaskFailure.retryImmediately();
          }

          processed.send(taskId);
          return TaskSuccess.done(undefined);
        },
      };
    },
  };
}

class TestPostgresDatabase {
  private constructor(
    readonly databaseName: string,
    readonly url: string,
  ) {}

  static async create(testName: string): Promise<TestPostgresDatabase> {
    const databaseName = `bellows_${testName}_${process.pid}_${randomUUID().replaceAll("-", "")}`;
    const admin = new Client({ connectionString: adminDatabaseUrl });
    await admin.connect();
    await admin.query(`CREATE DATABASE "${databaseName}"`);
    await admin.end();

    return new TestPostgresDatabase(
      databaseName,
      adminDatabaseUrl.replace(/\/[^/]+$/, `/${databaseName}`),
    );
  }

  async cleanup(): Promise<void> {
    const admin = new Client({ connectionString: adminDatabaseUrl });
    await admin.connect();
    await admin.query(
      `
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = $1
  AND pid <> pg_backend_pid()
      `,
      [this.databaseName],
    );
    await admin.query(`DROP DATABASE "${this.databaseName}"`);
    await admin.end();
  }
}

function createReschedulingPublishedWorkerFactory(
  processed: AsyncChannel<number>,
  recordAttempt: () => number,
  nextRunAtMs: number,
): WorkerFactory<typeof reschedulingPublishedTask> {
  return {
    task: reschedulingPublishedTask,
    build() {
      return {
        async process(taskId) {
          processed.send(taskId);
          const attempt = recordAttempt();
          if (attempt === 0) {
            return TaskSuccess.scheduleNextRun(taskId, nextRunAtMs);
          }

          return TaskSuccess.done(taskId);
        },
      };
    },
  };
}

function createScheduledSingletonWorkerFactory(
  processed: AsyncChannel<number>,
  gate: Gate,
  recordAttempt: () => number,
  nextRunAtMs: number,
): WorkerFactory<typeof scheduledSingletonTask> {
  return {
    task: scheduledSingletonTask,
    build() {
      return {
        async process(taskId) {
          processed.send(taskId);
          const attempt = recordAttempt();
          if (attempt === 0) {
            return TaskSuccess.scheduleNextRun(undefined, nextRunAtMs);
          }

          await gate.wait();
          return TaskSuccess.done(undefined);
        },
      };
    },
  };
}

async function recvWithTimeout<T>(channel: AsyncChannel<T>): Promise<T> {
  const value = await Promise.race([
    channel.recv(),
    new Promise<null>((resolve) => {
      setTimeout(() => {
        resolve(null);
      }, 1_000);
    }),
  ]);

  expect(value).not.toBeNull();
  return value as T;
}

function track<
  T extends {
    close?: () => Promise<void> | void;
    cleanup?: () => Promise<void> | void;
  },
>(resource: T): T {
  resources.push({
    close: async () => {
      await resource.close?.();
      await resource.cleanup?.();
    },
  });
  return resource;
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}
