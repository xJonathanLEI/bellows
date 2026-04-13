import { randomUUID } from "node:crypto";
import { Client } from "pg";
import { afterEach, expect, test } from "vitest";
import { PostgresBackend } from "../src/backends/postgres.js";
import {
  definePublishTask,
  defineSingletonTask,
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
