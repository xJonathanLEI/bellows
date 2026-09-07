import { randomUUID } from "node:crypto";
import { Client } from "pg";
import { afterEach, expect, test } from "vitest";
import {
  initializePostgresSchema,
  PostgresBackend,
} from "../src/backends/postgres.js";
import { PostgresExecutionBackend } from "../src/backends/postgres-execution.js";
import { validatePostgresSchemaName } from "../src/backends/postgres-operations.js";
import {
  definePublishTask,
  defineSingletonTask,
  LeaseLostError,
  runTaskOnce,
  TaskFailure,
  TaskLeasedError,
  TaskNotFoundError,
  TaskSuccess,
  TaskUnavailableError,
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

test.each([
  "default",
  "named",
])("postgres execution backend delivers callbacks with a %s schema", async (schemaMode) => {
  const database = track(
    await TestPostgresDatabase.create("execution_callback"),
  );
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  try {
    const schema =
      schemaMode === "named"
        ? `callbacks_${randomUUID().replaceAll("-", "")}`
        : undefined;
    if (schema) {
      await admin.query(`CREATE SCHEMA "${schema}"`);
    }
    const publisher = track(
      await PostgresBackend.connect(database.url, { schema }),
    );
    await publisher.initialize();
    const executor = track(
      schema
        ? await PostgresExecutionBackend.connect(database.url, { schema })
        : await PostgresExecutionBackend.connect(database.url),
    );
    const awaitable = await publisher.publishAwaitable(echoTask, {
      name: "Alice",
    });
    const processed = track(new AsyncChannel<ProcessedTask>());
    await runTaskOnce(executor, createEchoWorkerFactory(processed), 17, {
      type: "task",
      taskId: awaitable.taskId,
    });

    const table = schema ? `"${schema}".bellows_tasks` : "bellows_tasks";
    const remaining = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM ${table}`,
    );
    expect(Number(remaining.rows[0].count)).toBe(0);
    expect(await awaitable.wait()).toBe("Alice");
    expect(await processed.recv()).toEqual({
      taskId: awaitable.taskId,
      name: "Alice",
    });
    processed.close();
    expect(await processed.recv()).toBeNull();
  } finally {
    await admin.end();
  }
});

test("postgres schema validation accepts valid names without a length limit", () => {
  for (const schema of ["a", "_", "_tasks_17", "public", "a".repeat(128)]) {
    expect(validatePostgresSchemaName(schema)).toBe(schema);
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

test("postgres execution backend uses a named schema without a listener", async () => {
  const database = track(
    await TestPostgresDatabase.create("execution_backend_named_schema"),
  );
  const schema = `execution_${randomUUID().replaceAll("-", "")}`;
  const schemaIdentifier = `"${schema}"`;
  const applicationName = `bellows_execution_${process.pid}_${randomUUID().replaceAll("-", "")}`;
  const executionUrl = new URL(database.url);
  executionUrl.searchParams.set("application_name", applicationName);

  const admin = new Client({ connectionString: database.url });
  await admin.connect();

  try {
    await admin.query(`CREATE SCHEMA ${schemaIdentifier}`);
    const backend = track(
      await PostgresExecutionBackend.connect(executionUrl.toString(), {
        schema,
      }),
    );
    const uninitializedTable = await admin.query<{ table_name: string | null }>(
      "SELECT to_regclass($1)::text AS table_name",
      [`${schemaIdentifier}.bellows_tasks`],
    );
    expect(uninitializedTable.rows[0].table_name).toBeNull();
    await initializePostgresSchema(database.url, schema);
    await initializePostgresSchema(database.url, schema);

    const task = definePublishTask<{ name: string }>("execution_once");
    const inserted = await admin.query<{ task_id: string }>(
      `
INSERT INTO ${schemaIdentifier}.bellows_tasks (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    available_from_unix_ms
)
VALUES ($1, NULL, $2, NULL, NULL, NULL)
RETURNING task_id::text
      `,
      [task.name, task.codec.encode({ name: "Alice" })],
    );
    const taskId = Number(inserted.rows[0].task_id);
    let processedName: string | undefined;
    const started = new Gate();
    const finish = new Gate();

    const factory: WorkerFactory<typeof task> = {
      task,
      build(workerId) {
        expect(workerId).toBe(17);
        return {
          async process(receivedTaskId, payload) {
            expect(receivedTaskId).toBe(taskId);
            processedName = payload.name;
            started.release();
            await finish.wait();
            return TaskSuccess.done(undefined);
          },
        };
      },
    };

    const execution = runTaskOnce(backend, factory, 17, {
      type: "task",
      taskId,
    });

    await started.wait();
    expect(processedName).toBe("Alice");
    const lease = await admin.query<{
      lease_worker_id: string | null;
      available_from_unix_ms: string | null;
    }>(
      `SELECT lease_worker_id, available_from_unix_ms
       FROM ${schemaIdentifier}.bellows_tasks WHERE task_id = $1`,
      [taskId],
    );
    expect(Number(lease.rows[0].lease_worker_id)).toBe(17);
    expect(lease.rows[0].available_from_unix_ms).not.toBeNull();
    const defaultTable = await admin.query<{ table_name: string | null }>(
      "SELECT to_regclass('public.bellows_tasks')::text AS table_name",
    );
    expect(defaultTable.rows[0].table_name).toBeNull();
    finish.release();
    await execution;
    const remaining = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM ${schemaIdentifier}.bellows_tasks`,
    );
    expect(Number(remaining.rows[0].count)).toBe(0);

    // This serial workload uses one pooled connection, without a dedicated listener.
    const activity = await admin.query<{ count: string }>(
      `
SELECT count(*)::text AS count
FROM pg_stat_activity
WHERE application_name = $1 AND datname = current_database()
      `,
      [applicationName],
    );
    expect(Number(activity.rows[0].count)).toBe(1);
  } finally {
    await admin.end();
  }
});

test("postgres daemon backend supports a named schema and reinitialization", async () => {
  const database = track(
    await TestPostgresDatabase.create("daemon_backend_named_schema"),
  );
  const schema = `daemon_${randomUUID().replaceAll("-", "")}`;
  const schemaIdentifier = `"${schema}"`;
  const admin = new Client({ connectionString: database.url });
  await admin.connect();

  try {
    await admin.query(`CREATE SCHEMA ${schemaIdentifier}`);
    await initializePostgresSchema(database.url, schema);
    const table = `${schemaIdentifier}.bellows_tasks`;
    const triggerQuery = `SELECT oid FROM pg_trigger
      WHERE tgrelid = $1::regclass AND tgname = 'bellows_tasks_notify_available'`;
    const originalTrigger = await admin.query<{ oid: number }>(triggerQuery, [
      table,
    ]);
    const backend = track(
      await PostgresBackend.connect(database.url, { schema }),
    );
    const published = await backend.publish(echoTask, { name: "Alice" });
    await initializePostgresSchema(database.url, schema);
    await backend.initialize();
    const trigger = await admin.query<{ oid: number }>(triggerQuery, [table]);
    expect(trigger.rows).toEqual(originalTrigger.rows);
    const processed = track(new AsyncChannel<ProcessedTask>());
    const dispatcher = new WorkerDispatcher(
      backend,
      createEchoWorkerFactory(processed),
    );
    const dispatcherHandle = await dispatcher.launch();
    expect(await processed.recv()).toEqual({
      taskId: published.taskId,
      name: "Alice",
    });

    // Publishing after launch exercises notification discovery and typed callbacks.
    const awaitable = await backend.publishAwaitable(echoTask, { name: "Bob" });
    expect(await awaitable.wait()).toBe("Bob");
    expect((await processed.recv())?.name).toBe("Bob");
    await dispatcherHandle.drain();
    processed.close();
    expect(await processed.recv()).toBeNull();
    const remaining = await admin.query<{ count: string }>(
      `SELECT count(*) FROM ${table}`,
    );
    expect(Number(remaining.rows[0].count)).toBe(0);
    const defaultTable = await admin.query<{ table_name: string | null }>(
      "SELECT to_regclass('public.bellows_tasks')::text AS table_name",
    );
    expect(defaultTable.rows[0].table_name).toBeNull();
  } finally {
    await admin.end();
  }
});

test("postgres rejects invalid schemas before connecting", async () => {
  for (const schema of [
    "",
    "Public",
    "1schema",
    "a.b",
    'a"b',
    "a b",
    "a\n",
    "a\r",
    "a\nb",
    "é",
    "a-b",
  ]) {
    await expect(
      PostgresBackend.connect("not a database URL", { schema }),
    ).rejects.toThrow("Database schema names");
    await expect(
      PostgresExecutionBackend.connect("not a database URL", { schema }),
    ).rejects.toThrow("Database schema names");
    await expect(
      initializePostgresSchema("not a database URL", schema),
    ).rejects.toThrow("Database schema names");
  }
});

test("postgres missing schema does not fall back", async () => {
  const database = track(await TestPostgresDatabase.create("missing_schema"));
  const schema = `missing_${randomUUID().replaceAll("-", "")}`;
  await expect(
    initializePostgresSchema(database.url, schema),
  ).rejects.toThrow();
  const backend = track(
    await PostgresBackend.connect(database.url, { schema }),
  );
  await expect(backend.initialize()).rejects.toThrow();
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  try {
    const schemas = await admin.query(
      "SELECT 1 FROM pg_namespace WHERE nspname = $1",
      [schema],
    );
    expect(schemas.rows).toHaveLength(0);
    const defaultTable = await admin.query<{ table_name: string | null }>(
      "SELECT to_regclass('public.bellows_tasks')::text AS table_name",
    );
    expect(defaultTable.rows[0].table_name).toBeNull();
    await admin.query(`CREATE SCHEMA "${schema}"`);
    await backend.initialize();
    await backend.publish(ackTask, undefined);
  } finally {
    await admin.end();
  }
});

test("postgres named schema task operations", async () => {
  const database = track(await TestPostgresDatabase.create("named_operations"));
  const schema = `_tasks_${randomUUID().replaceAll("-", "")}`;
  const table = `"${schema}".bellows_tasks`;
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  try {
    await admin.query(`CREATE SCHEMA "${schema}"`);
    const backend = track(
      await PostgresBackend.connect(database.url, { schema }),
    );
    await backend.initialize();
    const expiration = Date.now() + 60_000;
    const later = expiration + 60_000;
    const taskState = async (taskId: number) => {
      const result = await admin.query<{
        lease_worker_id: string | null;
        available_from_unix_ms: string | null;
        callback_id: string | null;
      }>(
        `SELECT lease_worker_id, available_from_unix_ms, callback_id
         FROM ${table} WHERE task_id = $1`,
        [taskId],
      );
      expect(result.rows).toHaveLength(1);
      return result.rows[0];
    };
    const makeAvailable = async (taskId: number) => {
      await admin.query(
        `UPDATE ${table} SET available_from_unix_ms = NULL WHERE task_id = $1`,
        [taskId],
      );
    };
    await expect(
      backend.claimPublished(ackTask, 17, 999, expiration),
    ).rejects.toBeInstanceOf(TaskNotFoundError);
    await expect(
      backend.claimEarliestPublished(ackTask, 17, expiration),
    ).rejects.toEqual(new TaskUnavailableError(null));
    const future = await backend.publishFuture(ackTask, undefined, later);
    await expect(
      backend.claimPublished(ackTask, 17, future.taskId, expiration),
    ).rejects.toEqual(new TaskUnavailableError(later));
    await expect(
      backend.claimEarliestPublished(ackTask, 17, expiration),
    ).rejects.toEqual(new TaskUnavailableError(later));

    const first = await backend.publishAwaitable(ackTask, undefined);
    const second = await backend.publish(ackTask, undefined);
    const claimed = await backend.claimEarliestPublished(
      ackTask,
      17,
      expiration,
    );
    expect(claimed.taskId).toBe(first.taskId);
    await expect(
      backend.claimPublished(ackTask, 18, first.taskId, expiration),
    ).rejects.toBeInstanceOf(TaskLeasedError);
    await expect(backend.renew(18, first.taskId, later)).rejects.toBeInstanceOf(
      LeaseLostError,
    );
    await expect(backend.fail(18, first.taskId, null)).rejects.toBeInstanceOf(
      LeaseLostError,
    );
    await expect(
      backend.finish(ackTask, 18, first.taskId, undefined, null),
    ).rejects.toBeInstanceOf(LeaseLostError);
    await backend.renew(17, first.taskId, later);
    let state = await taskState(first.taskId);
    expect(state.lease_worker_id).toBe("17");
    expect(Number(state.available_from_unix_ms)).toBe(later);
    expect(state.callback_id).not.toBeNull();
    await backend.fail(17, first.taskId, later);
    state = await taskState(first.taskId);
    expect(state.lease_worker_id).toBeNull();
    expect(Number(state.available_from_unix_ms)).toBe(later);
    expect(state.callback_id).not.toBeNull();
    await expect(
      backend.claimPublished(ackTask, 17, first.taskId, expiration),
    ).rejects.toEqual(new TaskUnavailableError(later));
    expect(
      (await backend.claimEarliestPublished(ackTask, 17, expiration)).taskId,
    ).toBe(second.taskId);
    await backend.finish(ackTask, 17, second.taskId, undefined, null);

    await makeAvailable(first.taskId);
    await backend.claimPublished(ackTask, 17, first.taskId, expiration);
    await backend.finish(ackTask, 17, first.taskId, undefined, later);
    expect(await first.wait()).toBeUndefined();
    state = await taskState(first.taskId);
    expect(state.lease_worker_id).toBeNull();
    expect(Number(state.available_from_unix_ms)).toBe(later);
    expect(state.callback_id).toBeNull();
    await makeAvailable(first.taskId);
    expect(
      (await backend.claimEarliestPublished(ackTask, 17, expiration)).taskId,
    ).toBe(first.taskId);
    await backend.finish(ackTask, 17, first.taskId, undefined, null);
    await makeAvailable(future.taskId);
    await backend.claimPublished(ackTask, 17, future.taskId, expiration);
    await backend.finish(ackTask, 17, future.taskId, undefined, null);
    const remaining = await admin.query<{ count: string }>(
      `SELECT count(*) FROM ${table}`,
    );
    expect(Number(remaining.rows[0].count)).toBe(0);

    const singleton = await backend.claimSingleton(
      singletonTask,
      17,
      expiration,
    );
    await expect(
      backend.claimSingleton(singletonTask, 18, expiration),
    ).rejects.toBeInstanceOf(TaskLeasedError);
    await backend.renew(17, singleton.taskId, later);
    await backend.fail(17, singleton.taskId, null);
    const releasedState = {
      lease_worker_id: null,
      available_from_unix_ms: null,
      callback_id: null,
    };
    expect(await taskState(singleton.taskId)).toEqual(releasedState);
    expect(
      (await backend.claimSingleton(singletonTask, 18, expiration)).taskId,
    ).toBe(singleton.taskId);
    await backend.finish(singletonTask, 18, singleton.taskId, undefined, later);
    state = await taskState(singleton.taskId);
    expect(state.lease_worker_id).toBeNull();
    expect(Number(state.available_from_unix_ms)).toBe(later);
    await expect(
      backend.claimSingleton(singletonTask, 17, expiration),
    ).rejects.toEqual(new TaskUnavailableError(later));
    await makeAvailable(singleton.taskId);
    expect(
      (await backend.claimSingleton(singletonTask, 17, expiration)).taskId,
    ).toBe(singleton.taskId);
    await backend.finish(singletonTask, 17, singleton.taskId, undefined, null);
    expect(await taskState(singleton.taskId)).toEqual(releasedState);
  } finally {
    await admin.end();
  }
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
