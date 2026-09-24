import { randomUUID } from "node:crypto";
import { Client, Pool, type PoolClient } from "pg";
import { afterEach, expect, expectTypeOf, test, vi } from "vitest";
import {
  PostgresPublishedTaskIdError as FullBackendPublishedTaskIdError,
  initializePostgresSchema,
  PostgresBackend,
} from "../src/backends/postgres.js";
import { PostgresDiscoveryBackend } from "../src/backends/postgres-discovery.js";
import { PostgresExecutionBackend } from "../src/backends/postgres-execution.js";
import {
  PostgresSingletonTaskIdError,
  PostgresTaskOperations,
  validatePostgresSchemaName,
} from "../src/backends/postgres-operations.js";
import {
  PostgresPublishedTaskIdError,
  PostgresPublishingBackend,
  type PostgresPublishingExecutor,
  type PostgresPublishParameters,
} from "../src/backends/postgres-publishing.js";
import {
  definePublishTask,
  defineSingletonTask,
  LeaseLostError,
  type PublishedTask,
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

test.each([
  "9007199254740991",
  "9007199254740992",
  "9007199254740993",
  "9223372036854775807",
])("singleton claims preserve exact PostgreSQL row ID %s", async (taskId) => {
  const database = track(await TestPostgresDatabase.create("singleton_id"));
  await initializePostgresSchema(database.url, "public");
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  const backend = await PostgresExecutionBackend.connect(database.url);
  try {
    await admin.query(
      `INSERT INTO bellows_tasks (task_id, task_name, task_unique_key, payload_json)
         OVERRIDING SYSTEM VALUE VALUES ($1, $2, $2, 'null')`,
      [taskId, singletonTask.name],
    );
    const expiration = Date.now() + 60_000;
    if (taskId === "9007199254740991") {
      expect(
        await backend.claimSingleton(singletonTask, 17, expiration),
      ).toEqual({
        taskId: Number(taskId),
        taskPayload: undefined,
        leaseExpirationMs: expiration,
      });
    } else {
      const claim = backend.claimSingleton(singletonTask, 17, expiration);
      await expect(claim).rejects.toBeInstanceOf(PostgresSingletonTaskIdError);
      await expect(claim).rejects.toMatchObject({ taskId });
    }
    // Even a rejected conversion may follow a committed claim. No rounded ID is released.
    expect(
      (
        await admin.query(
          "SELECT task_id::text, task_unique_key, lease_worker_id::text FROM bellows_tasks",
        )
      ).rows,
    ).toEqual([
      {
        task_id: taskId,
        task_unique_key: singletonTask.name,
        lease_worker_id: "17",
      },
    ]);
  } finally {
    await backend.close();
    await admin.end();
  }
});

test.each([
  undefined,
  "discovery_workload",
])("discovery is exact, read-only and keyset bounded (schema=%s)", async (schema) => {
  const database = track(await TestPostgresDatabase.create("discovery"));
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  const table = `"${schema ?? "public"}"."bellows_tasks"`;
  let backend: PostgresDiscoveryBackend | undefined;
  try {
    if (schema) await admin.query(`CREATE SCHEMA "${schema}"`);
    await initializePostgresSchema(database.url, schema ?? "public");
    await admin.query('CREATE SCHEMA "other_workload"');
    await initializePostgresSchema(database.url, "other_workload");
    await admin.query(`INSERT INTO other_workload.bellows_tasks (task_name, payload_json)
                         VALUES ('wrong schema', '!')`);
    const readOnlyUrl = new URL(database.url);
    readOnlyUrl.searchParams.set(
      "options",
      "-c default_transaction_read_only=on",
    );
    backend = await PostgresDiscoveryBackend.connect(
      readOnlyUrl.toString(),
      schema ? { schema } : {},
    );
    const empty = await backend.beginSweep();
    expect(empty.upperId).toBeNull();
    expect(await backend.readPage(empty, null)).toEqual([]);
    await admin.query(`INSERT INTO ${table} (task_id, task_name, task_unique_key, payload_json)
        OVERRIDING SYSTEM VALUE VALUES (9223372036854775807, 'singleton', 'unique', '!')`);
    const singletonOnly = await backend.beginSweep();
    expect(singletonOnly.upperId).toBe("9223372036854775807");
    expect(await backend.readPage(singletonOnly, null)).toEqual([
      {
        taskId: "9223372036854775807",
        taskName: "singleton",
        isSingleton: true,
      },
    ]);
    await admin.query(`DELETE FROM ${table}`);
    await admin.query(`INSERT INTO ${table} (task_id, task_name, payload_json)
        OVERRIDING SYSTEM VALUE
        SELECT n * 10, CASE WHEN n % 2 = 0 THEN ' 未登録/🔥 ' ELSE 'unregistered' END,
               'invalid JSON: never decode' FROM generate_series(1, 205) n;
        INSERT INTO ${table} (task_id, task_name, payload_json) OVERRIDING SYSTEM VALUE VALUES
        (-9223372036854775808, 'minimum', '!'), (-1, 'negative', '!'), (0, '', '!'),
        (2100, 'past', '!'), (2110, 'exact', '!'), (2120, 'future', '!'),
        (2130, 'expired owner', '!'), (2140, 'occupied', '!'),
        (9007199254740991, 'safe', '!'), (9007199254740992, 'unsafe', '!'),
        (9007199254740993, 'unsafe exact', '!'), (9223372036854775806, 'upper', '!');
        UPDATE ${table} SET task_unique_key = task_id::text
        WHERE task_id IN (2110, 2120, 2130, 2140, 9007199254740993, 9223372036854775806)`);
    const window = await backend.beginSweep();
    expect(window.upperId).toBe("9223372036854775806");
    expect(await backend.readPage(empty, null)).toEqual([]);
    const clock = (
      await admin.query<{ now: string }>(
        "SELECT FLOOR(EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::bigint::text AS now",
      )
    ).rows[0]?.now;
    if (clock === undefined) throw new Error("Database clock missing");
    expect(BigInt(window.cutoffUnixMs)).toBeLessThanOrEqual(BigInt(clock));
    expect(BigInt(clock) - BigInt(window.cutoffUnixMs)).toBeLessThan(5_000n);
    await admin.query(
      `UPDATE ${table} SET available_from_unix_ms = CASE task_id
        WHEN 2100 THEN $1::bigint - 1 WHEN 2110 THEN $1::bigint WHEN 2130 THEN $1::bigint
        ELSE $1::bigint + 1 END,
        lease_worker_id = CASE WHEN task_id IN (2130, 2140) THEN 77 END
        WHERE task_id BETWEEN 2100 AND 2140`,
      [window.cutoffUnixMs],
    );
    await admin.query("SELECT pg_sleep(0.01)");
    const snapshot = async () =>
      (
        await admin.query(
          `SELECT row_to_json(t)::text FROM ${table} t ORDER BY task_id`,
        )
      ).rows;
    const before = await snapshot();
    const first = await backend.readPage(window, null);
    expect(first).toHaveLength(100);
    expect(first[0]).toEqual({
      taskId: "-9223372036854775808",
      taskName: "minimum",
      isSingleton: false,
    });
    expect(first[2]).toEqual({ taskId: "0", taskName: "", isSingleton: false });
    expect(first[4]?.taskName).toBe(" 未登録/🔥 ");
    expect(await snapshot()).toEqual(before);
    // Even a consumer failure must advance by the last returned identity, not an offset.
    let cursor = first.at(-1)?.taskId;
    if (cursor === undefined) throw new Error("First page missing");
    expect(cursor).toBe("970");
    await admin.query(`DELETE FROM ${table} WHERE task_id = 10;
        UPDATE ${table} SET available_from_unix_ms = 9223372036854775807 WHERE task_id = 20;
        INSERT INTO ${table} (task_id, task_name, payload_json) OVERRIDING SYSTEM VALUE
        VALUES (5, 'late behind cursor', '!'), (9223372036854775807, 'new publication', '!')`);
    const changed = await snapshot();
    const remaining: string[] = [];
    for (;;) {
      const page = await backend.readPage(window, cursor);
      expect(page.length).toBeLessThanOrEqual(100);
      const firstRow = page[0];
      const lastRow = page.at(-1);
      if (!firstRow || !lastRow) break;
      expect(BigInt(firstRow.taskId)).toBeGreaterThan(BigInt(cursor));
      cursor = lastRow.taskId;
      remaining.push(...page.map((row) => row.taskId));
      for (const row of page)
        expect(row.isSingleton).toBe(
          ["2110", "2130", "9007199254740993", "9223372036854775806"].includes(
            row.taskId,
          ),
        );
    }
    expect(remaining).toEqual([
      ...Array.from({ length: 108 }, (_, n) => String((n + 98) * 10)),
      "2100",
      "2110",
      "2130",
      "9007199254740991",
      "9007199254740992",
      "9007199254740993",
      "9223372036854775806",
    ]);
    expect(await snapshot()).toEqual(changed);
    const next = await backend.beginSweep();
    expect(next.upperId).toBe("9223372036854775807");
    expect(
      (await backend.readPage(next, null)).map((row) => row.taskId),
    ).toContain("5");
    expect(await backend.readPage(next, "9223372036854775806")).toEqual([
      {
        taskId: "9223372036854775807",
        taskName: "new publication",
        isSingleton: false,
      },
    ]);
    expect(await backend.readPage(next, "9223372036854775807")).toEqual([]);
    const nowDue = await backend.readPage(next, "2099");
    expect(nowDue.map((row) => row.taskId)).toEqual(
      expect.arrayContaining(["2120", "2140"]),
    );
  } finally {
    await backend?.close();
    await admin.end();
  }
  if (!backend) throw new Error("Backend missing");
  await expect(backend.beginSweep()).rejects.toThrow();
});

test("discovery never initializes tables and closes after query failure", async () => {
  const database = track(
    await TestPostgresDatabase.create("discovery_missing"),
  );
  const backend = await PostgresDiscoveryBackend.connect(database.url);
  try {
    await expect(backend.beginSweep()).rejects.toThrow(/does not exist/);
    await expect(
      backend.readPage({ cutoffUnixMs: "0", upperId: "1" }, null),
    ).rejects.toThrow(/does not exist/);
  } finally {
    await backend.close();
  }
});

test("postgres discovery exposes only discovery and lifecycle methods", () => {
  expectTypeOf<keyof PostgresDiscoveryBackend>().toEqualTypeOf<
    "beginSweep" | "readPage" | "close"
  >();
});

test("discovery awaits pool shutdown", async () => {
  const gate = new Gate();
  const end = vi.spyOn(Pool.prototype, "end").mockImplementation(async () => {
    await gate.wait();
  });
  try {
    const backend = await PostgresDiscoveryBackend.connect("not used");
    let settled = false;
    const close = backend.close().then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(end).toHaveBeenCalledOnce();
    expect(settled).toBe(false);
    gate.release();
    await close;
    expect(settled).toBe(true);
  } finally {
    end.mockRestore();
  }
});

test("failed claim follow-up never reports an existing due row missing", async () => {
  const database = track(await TestPostgresDatabase.create("claim_follow_up"));
  const backend = track(await PostgresBackend.connect(database.url));
  await backend.initialize();
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  try {
    const future = await backend.publishFuture(
      ackTask,
      undefined,
      Date.now() + 60_000,
    );
    // A statement trigger runs for a zero-row UPDATE, changing availability before the SELECT.
    await admin.query(`
      CREATE FUNCTION release_after_claim() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN UPDATE bellows_tasks SET available_from_unix_ms = NULL; RETURN NULL; END $$;
      CREATE TRIGGER release_after_claim AFTER UPDATE ON bellows_tasks
      FOR EACH STATEMENT WHEN (pg_trigger_depth() = 0) EXECUTE FUNCTION release_after_claim();
    `);
    await expect(
      backend.claimPublished(ackTask, 17, future.taskId, Date.now() + 60_000),
    ).rejects.toBeInstanceOf(TaskUnavailableError);
    await admin.query("DROP TRIGGER release_after_claim ON bellows_tasks");
    await admin.query(`
      CREATE FUNCTION skip_claim() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RETURN NULL; END $$;
      CREATE TRIGGER skip_claim BEFORE UPDATE ON bellows_tasks
      FOR EACH ROW WHEN (NEW.lease_worker_id = 17) EXECUTE FUNCTION skip_claim();
    `);
    for (const available of [null, Date.now() - 1_000]) {
      const task =
        available === null
          ? await backend.publish(ackTask, undefined)
          : await backend.publishFuture(ackTask, undefined, available);
      try {
        await backend.claimPublished(
          ackTask,
          17,
          task.taskId,
          Date.now() + 60_000,
        );
        expect.unreachable("claim should have been suppressed");
      } catch (error) {
        expect(error).toBeInstanceOf(TaskUnavailableError);
        expect(
          (error as TaskUnavailableError).availableFromMs,
        ).toBeLessThanOrEqual(Date.now());
        if (available !== null)
          expect((error as TaskUnavailableError).availableFromMs).toBe(
            available,
          );
      }
    }
    const singleton = await backend.claimSingleton(
      singletonTask,
      18,
      Date.now() + 60_000,
    );
    await backend.finish(singletonTask, 18, singleton.taskId, undefined, null);
    await expect(
      backend.claimSingleton(singletonTask, 17, Date.now() + 60_000),
    ).rejects.toBeInstanceOf(TaskUnavailableError);
  } finally {
    await admin.end();
  }
});

test.each([
  null,
  -1_000,
  60_000,
])("follow-up availability survives delayed I/O: offset=%s", async (offset) => {
  vi.useFakeTimers();
  try {
    const now = Date.now();
    const deadline = offset === null ? null : now + offset;
    const gate = new Gate();
    const queried = new Gate();
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rowCount: 0, rows: [] })
      .mockImplementationOnce(async () => {
        queried.release();
        await gate.wait();
        return {
          rowCount: 1,
          rows: [
            {
              lease_worker_id: null,
              available_from_unix_ms: deadline?.toString() ?? null,
            },
          ],
        };
      });
    const ops = new PostgresTaskOperations({ query } as unknown as Pool);
    const claim = ops.claimPublished(ackTask, 17, 1, now + 20_000);
    const check = expect(claim).rejects.toEqual(
      new TaskUnavailableError(deadline ?? now + 321),
    );
    await queried.wait();
    vi.setSystemTime(now + 321);
    gate.release();
    await check;
  } finally {
    vi.useRealTimers();
  }
});

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

test.each([
  "delete",
  "reschedule",
  "singleton",
])("postgres execution rolls back failed callback finalization: %s", async (mode) => {
  const database = track(
    await TestPostgresDatabase.create("callback_rollback"),
  );
  const schema = `callbacks_${randomUUID().replaceAll("-", "")}`;
  const table = `"${schema}".bellows_tasks`;
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  try {
    await admin.query(`CREATE SCHEMA "${schema}"`);
    await initializePostgresSchema(database.url, schema);
    const publisher = track(
      await PostgresBackend.connect(database.url, { schema }),
    );
    const executor = track(
      await PostgresExecutionBackend.connect(database.url, { schema }),
    );
    const singleton = defineSingletonTask<string>("callback_singleton");
    const expiration = Date.now() + 60_000;
    const task = mode === "singleton" ? singleton : echoTask;
    const taskId =
      mode === "singleton"
        ? (await executor.claimSingleton(singleton, 17, expiration)).taskId
        : (await publisher.publish(echoTask, { name: "callback" })).taskId;
    if (mode !== "singleton") {
      await executor.claimPublished(echoTask, 17, taskId, expiration);
    }
    await admin.query(
      `UPDATE ${table} SET callback_id = $1 WHERE task_id = $2`,
      [123, taskId],
    );
    const state = async () =>
      (await admin.query(`SELECT * FROM ${table} WHERE task_id = $1`, [taskId]))
        .rows;
    const before = await state();
    const available = mode === "delete" ? null : expiration;
    const failingCodecTask = {
      ...task,
      callbackCodec: {
        ...task.callbackCodec,
        encode: () => {
          throw new Error("intentional callback serialization failure");
        },
      },
    };
    await expect(
      executor.finish(failingCodecTask, 17, taskId, "callback", available),
    ).rejects.toThrow("intentional callback serialization failure");
    expect(await state()).toEqual(before);
    // PostgreSQL rejects oversized NOTIFY payloads. The task mutation must roll back too.
    await expect(
      executor.finish(task, 17, taskId, "x".repeat(9000), available),
    ).rejects.toThrow();
    expect(await state()).toEqual(before);
    if (mode === "singleton") {
      const publishedSameName = definePublishTask<void>(singleton.name);
      await expect(
        executor.claimPublished(publishedSameName, 18, taskId, expiration),
      ).rejects.toBeInstanceOf(TaskNotFoundError);
      await expect(
        executor.claimEarliestPublished(publishedSameName, 18, expiration),
      ).rejects.toEqual(new TaskUnavailableError(null));
      await expect(
        executor.finish(publishedSameName, 17, taskId, undefined, null),
      ).rejects.toBeInstanceOf(LeaseLostError);
    } else {
      await expect(
        executor.finish(singleton, 17, taskId, "callback", null),
      ).rejects.toBeInstanceOf(LeaseLostError);
    }
    expect(await state()).toEqual(before);
    await executor.finish(task, 17, taskId, 'hello "🦀"', available);
    if (mode === "delete") {
      expect(await state()).toEqual([]);
    } else {
      expect(await state()).toEqual([
        {
          ...before[0],
          lease_worker_id: null,
          callback_id: null,
          available_from_unix_ms: String(expiration),
        },
      ]);
    }
  } finally {
    await admin.end();
  }
});

test("postgres execution orders claims, skips locked rows, and checks competing owners", async () => {
  const database = track(await TestPostgresDatabase.create("execution_owners"));
  const publisher = track(await PostgresBackend.connect(database.url));
  await publisher.initialize();
  const executor = track(await PostgresExecutionBackend.connect(database.url));
  const competitor = track(
    await PostgresExecutionBackend.connect(database.url),
  );
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  let transactionOpen = false;
  try {
    const old = await publisher.publishFuture(
      echoTask,
      { name: "old" },
      Date.now() - 1000,
    );
    const first = await publisher.publish(echoTask, { name: "first" });
    const second = await publisher.publish(echoTask, { name: "second" });
    const expiration = Date.now() + 60_000;
    await expect(
      executor.claimPublished(ackTask, 17, first.taskId, expiration),
    ).rejects.toBeInstanceOf(TaskNotFoundError);
    await admin.query("BEGIN");
    transactionOpen = true;
    await admin.query(
      "SELECT task_id FROM bellows_tasks WHERE task_id = $1 FOR UPDATE",
      [first.taskId],
    );
    expect(
      (await executor.claimEarliestPublished(echoTask, 17, expiration)).taskId,
    ).toBe(second.taskId);
    await admin.query("ROLLBACK");
    transactionOpen = false;
    const claims = await Promise.allSettled([
      executor.claimPublished(echoTask, 17, first.taskId, expiration),
      competitor.claimPublished(echoTask, 18, first.taskId, expiration),
    ]);
    expect(claims.filter((claim) => claim.status === "fulfilled")).toHaveLength(
      1,
    );
    expect(claims.filter((claim) => claim.status === "rejected")).toEqual([
      { status: "rejected", reason: expect.any(TaskLeasedError) },
    ]);
    expect(
      (await executor.claimEarliestPublished(echoTask, 19, expiration)).taskId,
    ).toBe(old.taskId);
    await admin.query(
      "UPDATE bellows_tasks SET available_from_unix_ms = 0 WHERE task_id = $1",
      [first.taskId],
    );
    const claimed = await competitor.claimPublished(
      echoTask,
      20,
      first.taskId,
      expiration,
    );
    expect(claimed.taskPayload).toEqual({ name: "first" });
    for (const owner of [17, 18]) {
      await expect(
        executor.renew(owner, first.taskId, expiration),
      ).rejects.toBeInstanceOf(LeaseLostError);
      await expect(
        executor.fail(owner, first.taskId, null),
      ).rejects.toBeInstanceOf(LeaseLostError);
      await expect(
        executor.finish(echoTask, owner, first.taskId, "", null),
      ).rejects.toBeInstanceOf(LeaseLostError);
    }
  } finally {
    if (transactionOpen) {
      await admin.query("ROLLBACK");
    }
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
      PostgresPublishingBackend.connect("not a database URL", { schema }),
    ).rejects.toThrow("Database schema names");
    await expect(
      PostgresDiscoveryBackend.connect("not a database URL", { schema }),
    ).rejects.toThrow("Database schema names");
    await expect(
      initializePostgresSchema("not a database URL", schema),
    ).rejects.toThrow("Database schema names");
  }
});

test("postgres publishing backend exposes only publication and lifecycle methods", () => {
  expectTypeOf<keyof PostgresPublishingBackend>().toEqualTypeOf<
    "publish" | "publishFuture" | "close"
  >();
});

test("postgres publishing executor types accept native clients and minimal adapters", () => {
  expectTypeOf<Pool>().toExtend<PostgresPublishingExecutor>();
  expectTypeOf<Client>().toExtend<PostgresPublishingExecutor>();
  expectTypeOf<PoolClient>().toExtend<PostgresPublishingExecutor>();
  expectTypeOf<keyof PostgresPublishingExecutor>().toEqualTypeOf<"query">();
  expectTypeOf<PostgresPublishParameters>().toEqualTypeOf<
    [string, string, number | null, number | null]
  >();
  const executor = {
    async query(_sql: string, _parameters: PostgresPublishParameters) {
      return { rows: [{ task_id: "17" }] };
    },
  };
  expectTypeOf(
    PostgresPublishingBackend.fromExecutor(executor),
  ).toEqualTypeOf<PostgresPublishingBackend>();
  // Typechecked, not executed: the factory preserves task inference and singleton rejection.
  expectTypeOf((publisher: PostgresPublishingBackend) => {
    // @ts-expect-error The payload must match the task definition.
    void publisher.publish(echoTask, { name: 1 });
    // @ts-expect-error Future payloads must also match the task definition.
    void publisher.publishFuture(echoTask, { name: 1 }, 0);
    // @ts-expect-error Singleton activation is not publishable.
    void publisher.publish(singletonTask, undefined);
    // @ts-expect-error Singleton activation is not publishable in the future.
    void publisher.publishFuture(singletonTask, undefined, 0);
    return publisher.publish(echoTask, { name: "immediate" });
  }).returns.resolves.toEqualTypeOf<PublishedTask>();
  expectTypeOf((publisher: PostgresPublishingBackend) =>
    publisher.publishFuture(echoTask, { name: "future" }, 0),
  ).returns.resolves.toEqualTypeOf<PublishedTask>();
});

test.each([
  undefined,
  "app_tasks",
])("postgres publishing executor sends one encoded insert with schema %s", async (schema) => {
  const executor = {
    rawTaskId: "17",
    query: vi.fn(async function (
      this: { rawTaskId: string },
      _sql: string,
      _parameters: PostgresPublishParameters,
    ) {
      return { rows: [{ task_id: this.rawTaskId }] };
    }),
  };
  const publisher = PostgresPublishingBackend.fromExecutor(executor, {
    schema,
  });
  expect(executor.query).not.toHaveBeenCalled();
  const encode = vi.fn(echoTask.codec.encode);
  const task = { ...echoTask, codec: { ...echoTask.codec, encode } };
  const payload = { name: 'hello "🦀"\n' };
  for (const availability of [null, Date.now() + 60_000]) {
    executor.query.mockClear();
    encode.mockClear();
    const receipt = await (availability === null
      ? publisher.publish(task, payload)
      : publisher.publishFuture(task, payload, availability));
    expect(receipt).toEqual({ taskId: 17 });
    expect(encode).toHaveBeenCalledExactlyOnceWith(payload);
    expect(executor.query).toHaveBeenCalledExactlyOnceWith(expect.any(String), [
      task.name,
      echoTask.codec.encode(payload),
      null,
      availability,
    ]);
    const sql = executor.query.mock.calls[0][0].trim();
    const table = schema ? `"${schema}".bellows_tasks` : "bellows_tasks";
    expect(sql.startsWith(`INSERT INTO ${table} (`)).toBe(true);
    expect(sql).toContain("VALUES ($1, NULL, $2, $3, NULL, $4)");
    expect(sql.endsWith("RETURNING task_id::text AS task_id")).toBe(true);
    expect(sql.match(/INSERT INTO/g)).toHaveLength(1);
    expect(sql).not.toContain(payload.name);
  }
});

test("postgres publishing executor validation and codec failures never query", async () => {
  const query = vi.fn(async () => ({ rows: [{ task_id: "17" }] }));
  for (const schema of ["", "Public", "a.b", "a\n", "a\r", 'a"b']) {
    expect(() =>
      PostgresPublishingBackend.fromExecutor({ query }, { schema }),
    ).toThrow("Database schema names");
  }
  const publisher = PostgresPublishingBackend.fromExecutor({ query });
  const error = new Error("intentional payload serialization failure");
  const encode = vi.fn(() => {
    throw error;
  });
  const task = { ...echoTask, codec: { ...echoTask.codec, encode } };
  await expect(publisher.publish(task, { name: "immediate" })).rejects.toBe(
    error,
  );
  await expect(
    publisher.publishFuture(task, { name: "future" }, 100),
  ).rejects.toBe(error);
  expect(encode).toHaveBeenCalledTimes(2);
  expect(query).not.toHaveBeenCalled();
  expect(await publisher.publish(echoTask, { name: "usable" })).toEqual({
    taskId: 17,
  });
});

test("postgres publishing executor errors and close never take resource ownership", async () => {
  const cause = new Error("driver failure");
  const failure = new Error("adapter failure", { cause });
  const executor = {
    query: vi.fn(async () => ({ rows: [{ task_id: "17" }] })),
    connect: vi.fn(),
    begin: vi.fn(),
    commit: vi.fn(),
    rollback: vi.fn(),
    release: vi.fn(),
    end: vi.fn(),
    close: vi.fn(),
  };
  // A factory must neither create its own pool nor acquire the external executor.
  const poolConnect = vi.spyOn(Pool.prototype, "connect");
  const poolEnd = vi.spyOn(Pool.prototype, "end");
  try {
    const publisher = PostgresPublishingBackend.fromExecutor(executor);
    expect(() =>
      PostgresPublishingBackend.fromExecutor(executor, { schema: "a.b" }),
    ).toThrow("Database schema names");
    expect(executor.query).not.toHaveBeenCalled();
    expect(await publisher.publish(ackTask, undefined)).toEqual({ taskId: 17 });
    for (const availability of [null, 100]) {
      executor.query.mockClear();
      executor.query.mockRejectedValueOnce(failure);
      const publication =
        availability === null
          ? publisher.publish(ackTask, undefined)
          : publisher.publishFuture(ackTask, undefined, availability);
      await expect(publication).rejects.toBe(failure);
      await expect(publication).rejects.toHaveProperty("cause", cause);
      expect(executor.query).toHaveBeenCalledTimes(1);
    }
    await publisher.close();
    await publisher.close();
    expect(await publisher.publishFuture(ackTask, undefined, 200)).toEqual({
      taskId: 17,
    });
    for (const method of [
      executor.connect,
      executor.begin,
      executor.commit,
      executor.rollback,
      executor.release,
      executor.end,
      executor.close,
      poolConnect,
      poolEnd,
    ]) {
      expect(method).not.toHaveBeenCalled();
    }
  } finally {
    poolConnect.mockRestore();
    poolEnd.mockRestore();
  }
});

test("postgres publishing executor receipts keep exact IDs and canonical safe-integer checks", async () => {
  const query = vi.fn(async () => ({ rows: [{ task_id: "17" }] }));
  const publisher = PostgresPublishingBackend.fromExecutor({ query });
  for (const availability of [null, 100]) {
    for (const [rawTaskId, expected] of [
      ["0", 0],
      ["-1", -1],
      ["1", 1],
      ["9007199254740991", Number.MAX_SAFE_INTEGER],
      ["-9007199254740991", Number.MIN_SAFE_INTEGER],
      ["9007199254740992", null],
      ["9007199254740993", null],
      ["9223372036854775807", null],
      ["01", null],
      ["1.0", null],
      ["1e0", null],
      ["-0", null],
    ] as const) {
      query.mockClear();
      query.mockResolvedValueOnce({ rows: [{ task_id: rawTaskId }] });
      const publication =
        availability === null
          ? publisher.publish(ackTask, undefined)
          : publisher.publishFuture(ackTask, undefined, availability);
      if (expected === null) {
        await expect(publication).rejects.toBeInstanceOf(
          PostgresPublishedTaskIdError,
        );
        await expect(publication).rejects.toMatchObject({
          name: "PostgresPublishedTaskIdError",
          message:
            "PostgreSQL publication returned an ID not representable as a safe integer.",
          taskId: rawTaskId,
        });
      } else {
        expect(await publication).toEqual({ taskId: expected });
      }
      expect(query).toHaveBeenCalledTimes(1);
    }
  }
});

test("postgres publication ID errors are shared by both public backends", () => {
  expect(FullBackendPublishedTaskIdError).toBe(PostgresPublishedTaskIdError);
  expectTypeOf<
    PostgresPublishedTaskIdError["taskId"]
  >().toEqualTypeOf<string>();
});

test.each([
  ["full", "immediate"],
  ["full", "future"],
  ["publishing-only", "immediate"],
  ["publishing-only", "future"],
] as const)("postgres %s %s publication preserves safe receipts and exact unsupported IDs", async (backendMode, publicationMode) => {
  const database = track(await TestPostgresDatabase.create("publication_ids"));
  await initializePostgresSchema(database.url, "public");
  const backend =
    backendMode === "full" ? PostgresBackend : PostgresPublishingBackend;
  const publisher = track(await backend.connect(database.url));
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  const query = vi.spyOn(Pool.prototype, "query");
  try {
    const encode = vi.fn(echoTask.codec.encode);
    const task = { ...echoTask, codec: { ...echoTask.codec, encode } };
    const payload = { name: 'hello "🦀"\n' };
    const availableFromMs =
      publicationMode === "future" ? Date.now() + 60_000 : null;
    const rows = [];
    for (const [rawTaskId, numericTaskId] of [
      ["1", 1],
      ["9007199254740991", Number.MAX_SAFE_INTEGER],
      ["9007199254740992", null],
      ["9007199254740993", null],
      ["9223372036854775807", null],
    ] as const) {
      await admin.query(
        "SELECT setval('bellows_tasks_task_id_seq', $1::bigint, false)",
        [rawTaskId],
      );
      query.mockClear();
      encode.mockClear();
      const publication =
        availableFromMs === null
          ? publisher.publish(task, payload)
          : publisher.publishFuture(task, payload, availableFromMs);
      if (numericTaskId === null) {
        await expect(publication).rejects.toBeInstanceOf(
          PostgresPublishedTaskIdError,
        );
        await expect(publication).rejects.toMatchObject({
          name: "PostgresPublishedTaskIdError",
          message:
            "PostgreSQL publication returned an ID not representable as a safe integer.",
          taskId: rawTaskId,
        });
      } else {
        expect(await publication).toEqual({ taskId: numericTaskId });
      }
      expect(encode).toHaveBeenCalledExactlyOnceWith(payload);
      expect(query).toHaveBeenCalledExactlyOnceWith(
        expect.stringContaining("INSERT INTO"),
        [task.name, echoTask.codec.encode(payload), null, availableFromMs],
      );
      expect(
        (
          await admin.query(
            "SELECT last_value, is_called FROM bellows_tasks_task_id_seq",
          )
        ).rows,
      ).toEqual([{ last_value: rawTaskId, is_called: true }]);
      rows.push({
        task_id: rawTaskId,
        task_name: task.name,
        payload_json: echoTask.codec.encode(payload),
        task_unique_key: null,
        callback_id: null,
        lease_worker_id: null,
        available_from_unix_ms:
          availableFromMs === null ? null : String(availableFromMs),
      });
      expect(
        (await admin.query("SELECT * FROM bellows_tasks ORDER BY task_id"))
          .rows,
      ).toEqual(rows);
    }
  } finally {
    query.mockRestore();
    await admin.end();
  }
});

test.each([
  "default",
  "named",
])("postgres publishing stores rows, notifies consumers, and supports execution with a %s schema", async (mode) => {
  const database = track(await TestPostgresDatabase.create("publishing_rows"));
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  const schema = mode === "named" ? "publishing" : undefined;
  const table = `"${schema ?? "public"}".bellows_tasks`;
  let publisher: PostgresPublishingBackend | undefined;
  let closed = false;
  try {
    if (schema) {
      await admin.query(`CREATE SCHEMA "${schema}"`);
    }
    publisher = await PostgresPublishingBackend.connect(database.url, {
      schema,
    });
    expect(
      (await admin.query("SELECT to_regclass($1)::text AS name", [table])).rows,
    ).toEqual([{ name: null }]);
    await expect(publisher.publish(ackTask, undefined)).rejects.toMatchObject({
      code: "42P01",
    });
    await initializePostgresSchema(database.url, schema ?? "public");
    const listener = track(
      await PostgresBackend.connect(database.url, { schema }),
    );
    const signals = track(await listener.subscribe(echoTask));
    const executor = track(
      await PostgresExecutionBackend.connect(database.url, { schema }),
    );
    const before = Date.now();
    const deadline = before + 60_000;
    const payload = { name: 'hello "🦀"\n' };
    const immediate = await publisher.publish(echoTask, payload);
    const future = await publisher.publishFuture(echoTask, payload, deadline);
    const unit = await publisher.publish(ackTask, undefined);
    const futureUnit = await publisher.publishFuture(
      ackTask,
      undefined,
      deadline,
    );
    await publisher.close();
    closed = true;
    for (const [receipt, name, encoded, available] of [
      [immediate, echoTask.name, echoTask.codec.encode(payload), null],
      [future, echoTask.name, echoTask.codec.encode(payload), String(deadline)],
      [unit, ackTask.name, "null", null],
      [futureUnit, ackTask.name, "null", String(deadline)],
    ] as const) {
      expect(
        (
          await admin.query(`SELECT * FROM ${table} WHERE task_id = $1`, [
            receipt.taskId,
          ])
        ).rows,
      ).toEqual([
        {
          task_id: String(receipt.taskId),
          task_name: name,
          payload_json: encoded,
          task_unique_key: null,
          callback_id: null,
          lease_worker_id: null,
          available_from_unix_ms: available,
        },
      ]);
    }
    const immediateSignal = await signals.recv();
    expect(immediateSignal).toMatchObject({
      type: "new-task-available",
      taskId: immediate.taskId,
    });
    expect(immediateSignal?.availableFromMs).toBeGreaterThanOrEqual(before);
    expect(immediateSignal?.availableFromMs).toBeLessThanOrEqual(Date.now());
    expect(await signals.recv()).toEqual({
      type: "new-task-available",
      taskId: future.taskId,
      availableFromMs: deadline,
    });
    await expect(
      executor.claimPublished(ackTask, 17, immediate.taskId, deadline),
    ).rejects.toBeInstanceOf(TaskNotFoundError);
    await expect(
      executor.claimPublished(echoTask, 17, future.taskId, deadline),
    ).rejects.toEqual(new TaskUnavailableError(deadline));
    await admin.query(
      `UPDATE ${table} SET available_from_unix_ms = NULL WHERE task_id = $1`,
      [future.taskId],
    );
    for (const receipt of [immediate, future]) {
      expect(
        (await executor.claimPublished(echoTask, 17, receipt.taskId, deadline))
          .taskPayload,
      ).toEqual(payload);
      await executor.finish(
        echoTask,
        17,
        receipt.taskId,
        "no callback registered",
        null,
      );
    }
    await admin.query(
      `UPDATE ${table} SET available_from_unix_ms = NULL WHERE task_id = $1`,
      [futureUnit.taskId],
    );
    for (const receipt of [unit, futureUnit]) {
      await executor.claimPublished(ackTask, 17, receipt.taskId, deadline);
      await executor.finish(ackTask, 17, receipt.taskId, undefined, null);
    }
    expect((await admin.query(`SELECT count(*) FROM ${table}`)).rows).toEqual([
      { count: "0" },
    ]);
    if (schema) {
      expect(
        (
          await admin.query(
            "SELECT to_regclass('public.bellows_tasks')::text AS name",
          )
        ).rows,
      ).toEqual([{ name: null }]);
    }
  } finally {
    if (!closed) {
      await publisher?.close();
    }
    await admin.end();
  }
});

test("postgres publishing missing schema and table never fall back", async () => {
  const database = track(
    await TestPostgresDatabase.create("publishing_missing"),
  );
  await initializePostgresSchema(database.url, "public");
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  const publisher = track(
    await PostgresPublishingBackend.connect(database.url, {
      schema: "missing",
    }),
  );
  try {
    for (const exists of [false, true]) {
      if (exists) {
        await admin.query("CREATE SCHEMA missing");
      }
      await expect(publisher.publish(ackTask, undefined)).rejects.toMatchObject(
        { code: "42P01" },
      );
      expect(
        (
          await admin.query(
            "SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'missing') AS present",
          )
        ).rows,
      ).toEqual([{ present: exists }]);
      expect(
        (await admin.query("SELECT count(*) FROM public.bellows_tasks")).rows,
      ).toEqual([{ count: "0" }]);
    }
  } finally {
    await admin.end();
  }
});

test.each([
  "full",
  "publishing-only",
])("postgres %s publication preserves codec and SQL errors without retries", async (mode) => {
  const database = track(
    await TestPostgresDatabase.create("publishing_errors"),
  );
  await initializePostgresSchema(database.url, "public");
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  const backend = mode === "full" ? PostgresBackend : PostgresPublishingBackend;
  const publisher = track(await backend.connect(database.url));
  try {
    const error = new Error("intentional payload serialization failure");
    const rejected = {
      ...echoTask,
      codec: {
        ...echoTask.codec,
        encode: () => {
          throw error;
        },
      },
    };
    await expect(
      publisher.publish(rejected, { name: "rejected" }),
    ).rejects.toBe(error);
    await expect(
      publisher.publishFuture(rejected, { name: "rejected" }, Date.now()),
    ).rejects.toBe(error);
    expect(
      (await admin.query("SELECT is_called FROM bellows_tasks_task_id_seq"))
        .rows,
    ).toEqual([{ is_called: false }]);
    await admin.query(
      "ALTER TABLE bellows_tasks ADD CONSTRAINT reject_insert CHECK (false)",
    );
    for (const availableFromMs of [null, Date.now() + 60_000]) {
      const publication =
        availableFromMs === null
          ? publisher.publish(ackTask, undefined)
          : publisher.publishFuture(ackTask, undefined, availableFromMs);
      await expect(publication).rejects.toMatchObject({
        code: "23514",
        constraint: "reject_insert",
        table: "bellows_tasks",
      });
      await expect(publication).rejects.not.toBeInstanceOf(
        PostgresPublishedTaskIdError,
      );
    }
    expect(
      (await admin.query("SELECT last_value FROM bellows_tasks_task_id_seq"))
        .rows,
    ).toEqual([{ last_value: "2" }]);
    expect(
      (await admin.query("SELECT count(*) FROM bellows_tasks")).rows,
    ).toEqual([{ count: "0" }]);
    await admin.query(
      "ALTER TABLE bellows_tasks DROP CONSTRAINT reject_insert",
    );
    expect(await publisher.publish(ackTask, undefined)).toEqual({ taskId: 3 });

    const custom = {
      ...echoTask,
      codec: {
        encode: ({ name }: { name: string }) => `custom:${name}`,
        decode: (encoded: string) => ({
          name: encoded.slice("custom:".length),
        }),
      },
    };
    const receipt = await publisher.publish(custom, { name: "codec" });
    expect(
      (
        await admin.query(
          "SELECT payload_json FROM bellows_tasks WHERE task_id = $1",
          [receipt.taskId],
        )
      ).rows,
    ).toEqual([{ payload_json: "custom:codec" }]);
    const executor = track(
      await PostgresExecutionBackend.connect(database.url),
    );
    expect(
      (
        await executor.claimPublished(
          custom,
          17,
          receipt.taskId,
          Date.now() + 60_000,
        )
      ).taskPayload,
    ).toEqual({ name: "codec" });
    await executor.finish(custom, 17, receipt.taskId, "done", null);
  } finally {
    await admin.end();
  }
});

test("postgres publishing external pool is reusable and creates no owned pool or listener", async () => {
  const database = track(await TestPostgresDatabase.create("external_pool"));
  await initializePostgresSchema(database.url, "public");
  const pool = new Pool({ connectionString: database.url, max: 1 });
  const observer = new Client({ connectionString: database.url });
  await observer.connect();
  const end = vi.spyOn(pool, "end");
  try {
    const before = (await pool.query("SELECT pg_backend_pid() AS pid")).rows;
    const connect = vi.spyOn(pool, "connect");
    const publisher = PostgresPublishingBackend.fromExecutor(pool);
    expect(connect).not.toHaveBeenCalled();
    connect.mockRestore();
    const first = await publisher.publish(ackTask, undefined);
    await publisher.close();
    await publisher.close();
    expect(end).not.toHaveBeenCalled();
    const second = await publisher.publishFuture(
      ackTask,
      undefined,
      Date.now() + 60_000,
    );
    expect(second.taskId).not.toBe(first.taskId);
    expect(
      (
        await observer.query(
          "SELECT task_id FROM bellows_tasks ORDER BY task_id",
        )
      ).rows,
    ).toEqual([
      { task_id: String(first.taskId) },
      { task_id: String(second.taskId) },
    ]);
    expect((await pool.query("SELECT pg_backend_pid() AS pid")).rows).toEqual(
      before,
    );
    expect(
      (await pool.query("SELECT * FROM pg_listening_channels()")).rows,
    ).toEqual([]);
    expect(
      (
        await observer.query(
          "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()",
        )
      ).rows,
    ).toEqual([{ count: "2" }]);
    expect(pool.totalCount).toBe(1);
  } finally {
    end.mockRestore();
    await pool.end();
    await observer.end();
  }
});

test.each([
  false,
  true,
])("postgres publishing transaction commits or rolls back all rows (ORM adapter: %s)", async (adapted) => {
  const f = await publicationTransactionFixture();
  const schema = adapted ? "app_tasks" : undefined;
  if (schema) {
    await f.observer.query("CREATE SCHEMA app_tasks");
    await initializePostgresSchema(f.database.url, schema);
  }
  const table = `"${schema ?? "public"}".bellows_tasks`;
  for (const outcome of ["rollback", "application-error", "commit"]) {
    await f.begin();
    await f.client.query("INSERT INTO business VALUES (1)");
    const identity = (
      await f.client.query(
        "SELECT txid_current(), current_setting('search_path') AS search_path",
      )
    ).rows;
    // This API deliberately has a different result shape and is bound to the transaction client.
    const orm = {
      async execute(sql: string, values: PostgresPublishParameters) {
        return (await f.client.query<{ task_id: string }>(sql, values)).rows;
      },
    };
    const adapter = {
      query: vi.fn(async (sql: string, values: PostgresPublishParameters) => ({
        rows: await orm.execute(sql, values),
      })),
    };
    const publisher = PostgresPublishingBackend.fromExecutor(
      adapted ? adapter : f.client,
      { schema },
    );
    const payload = { name: 'transaction "🦀"\n' };
    const availability = Date.now() + 60_000;
    const receipts = [
      await publisher.publish(echoTask, payload),
      await publisher.publishFuture(echoTask, payload, availability),
    ];
    if (adapted) {
      expect(adapter.query).toHaveBeenCalledTimes(2);
    }
    await publisher.close();
    await publisher.close();
    f.assertCallerOwned();
    expect(
      (
        await f.client.query(
          "SELECT txid_current(), current_setting('search_path') AS search_path",
        )
      ).rows,
    ).toEqual(identity);
    expect(await publicationCounts(f.client, table)).toEqual(["1", "2"]);
    expect(await publicationCounts(f.observer, table)).toEqual(["0", "0"]);
    const expected = receipts.map(({ taskId }, index) => ({
      task_id: String(taskId),
      task_name: echoTask.name,
      task_unique_key: null,
      payload_json: JSON.stringify(payload),
      callback_id: null,
      lease_worker_id: null,
      available_from_unix_ms: index === 0 ? null : String(availability),
    }));
    expect(
      (await f.client.query(`SELECT * FROM ${table} ORDER BY task_id`)).rows,
    ).toEqual(expected);
    const dispatch = vi.fn();
    if (outcome === "commit") {
      await f.commit();
      expect(await publicationCounts(f.observer, table)).toEqual(["1", "2"]);
      expect(
        (await f.observer.query(`SELECT * FROM ${table} ORDER BY task_id`))
          .rows,
      ).toEqual(expected);
      dispatch(receipts);
    } else if (outcome === "application-error") {
      const applicationError = new Error("business failure after publication");
      try {
        throw applicationError;
      } catch (error) {
        expect(error).toBe(applicationError);
        await f.rollback();
      }
    } else {
      await f.rollback();
    }
    expect(dispatch).toHaveBeenCalledTimes(outcome === "commit" ? 1 : 0);
    if (outcome !== "commit") {
      expect(await publicationCounts(f.observer, table)).toEqual(["0", "0"]);
    }
    f.assertCallerOwned();
    await f.client.query("SELECT 1");
  }
  if (adapted) {
    expect(await publicationCounts(f.observer, "public.bellows_tasks")).toEqual(
      ["1", "0"],
    );
  }
});

test("postgres publishing transaction codec and statement failures remain caller-controlled", async () => {
  const f = await publicationTransactionFixture();
  const publisher = PostgresPublishingBackend.fromExecutor(f.client);
  await f.begin();
  await f.client.query("INSERT INTO business VALUES (1)");
  const query = vi.spyOn(f.client, "query");
  const codecError = new Error("intentional payload serialization failure");
  const rejected = {
    ...echoTask,
    codec: {
      ...echoTask.codec,
      encode: () => {
        throw codecError;
      },
    },
  };
  try {
    await expect(
      publisher.publish(rejected, { name: "rejected" }),
    ).rejects.toBe(codecError);
    await expect(
      publisher.publishFuture(rejected, { name: "rejected" }, Date.now()),
    ).rejects.toBe(codecError);
    expect(query).not.toHaveBeenCalled();
    await publisher.close();
    f.assertCallerOwned();
    expect(
      (await f.client.query("SELECT is_called FROM bellows_tasks_task_id_seq"))
        .rows,
    ).toEqual([{ is_called: false }]);
    expect(await publicationCounts(f.client)).toEqual(["1", "0"]);
    expect(await publicationCounts(f.observer)).toEqual(["0", "0"]);
    await f.rollback();

    await f.observer.query(
      "ALTER TABLE bellows_tasks ADD CONSTRAINT reject_insert CHECK (false)",
    );
    let attempts = 0;
    for (const availability of [null, Date.now() + 60_000]) {
      await f.begin();
      await f.client.query("INSERT INTO business VALUES (1)");
      query.mockClear();
      const publication =
        availability === null
          ? publisher.publish(ackTask, undefined)
          : publisher.publishFuture(ackTask, undefined, availability);
      await expect(publication).rejects.toMatchObject({
        code: "23514",
        constraint: "reject_insert",
        table: "bellows_tasks",
      });
      expect(query).toHaveBeenCalledTimes(1);
      // The exact driver exception, not a message-only replacement, reaches the caller.
      const original = await query.mock.results[0].value.catch(
        (error: unknown) => error,
      );
      await expect(publication).rejects.toBe(original);
      await publisher.close();
      await publisher.close();
      f.assertCallerOwned();
      await expect(f.client.query("SELECT 1")).rejects.toMatchObject({
        code: "25P02",
      });
      expect(await publicationCounts(f.observer)).toEqual(["0", "0"]);
      await f.rollback();
      expect(
        (
          await f.client.query(
            "SELECT last_value FROM bellows_tasks_task_id_seq",
          )
        ).rows,
      ).toEqual([{ last_value: String(++attempts) }]);
      expect(await publicationCounts(f.observer)).toEqual(["0", "0"]);
    }
    await f.observer.query(
      "ALTER TABLE bellows_tasks DROP CONSTRAINT reject_insert",
    );
    // A no-op external close does not disable later use, even after failure and rollback.
    await publisher.publish(ackTask, undefined);
    f.assertCallerOwned();
  } finally {
    query.mockRestore();
  }
});

test("postgres publishing transaction receipt errors retain exact pending IDs without finalizing", async () => {
  const f = await publicationTransactionFixture();
  const publisher = PostgresPublishingBackend.fromExecutor(f.client);
  for (const rawId of ["9007199254740993", "9223372036854775807"]) {
    await f.observer.query(
      `ALTER SEQUENCE bellows_tasks_task_id_seq RESTART WITH ${rawId}`,
    );
    await f.begin();
    await f.client.query("INSERT INTO business VALUES (1)");
    const publication = publisher.publish(ackTask, undefined);
    await expect(publication).rejects.toBeInstanceOf(
      PostgresPublishedTaskIdError,
    );
    await expect(publication).rejects.toMatchObject({ taskId: rawId });
    await publisher.close();
    f.assertCallerOwned();
    expect(
      (await f.client.query("SELECT task_id::text FROM bellows_tasks")).rows,
    ).toEqual([{ task_id: rawId }]);
    expect(await publicationCounts(f.client)).toEqual(["1", "1"]);
    expect(await publicationCounts(f.observer)).toEqual(["0", "0"]);
    await f.rollback();
    expect(await publicationCounts(f.observer)).toEqual(["0", "0"]);
  }
  await f.client.query("SELECT 1");
});

test("postgres publishing valid transaction receipt can precede commit failure without dispatch", async () => {
  const f = await publicationTransactionFixture();
  await f.begin();
  await f.client.query("INSERT INTO business VALUES (1), (1)");
  const publisher = PostgresPublishingBackend.fromExecutor(f.client);
  const receipt = await publisher.publish(ackTask, undefined);
  expect(receipt.taskId).toBeGreaterThan(0);
  expect(await publicationCounts(f.client)).toEqual(["2", "1"]);
  expect(await publicationCounts(f.observer)).toEqual(["0", "0"]);
  const dispatch = vi.fn();
  const workflow = async () => {
    await f.commit();
    dispatch(receipt);
  };
  await expect(workflow()).rejects.toMatchObject({ code: "23505" });
  expect(dispatch).not.toHaveBeenCalled();
  expect(await publicationCounts(f.observer)).toEqual(["0", "0"]);
  f.assertCallerOwned();
  await f.client.query("SELECT 1");
});

test("postgres publishing transaction notifications wait for commit and exclude rollback", async () => {
  const f = await publicationTransactionFixture();
  const notifications: string[] = [];
  f.observer.on("notification", ({ channel, payload }) => {
    expect(channel).toBe("bellows_tasks");
    notifications.push(payload ?? "");
  });
  await f.observer.query("LISTEN bellows_tasks");
  try {
    for (const commit of [false, true]) {
      await f.begin();
      const publisher = PostgresPublishingBackend.fromExecutor(f.client);
      const availability = Date.now() + 60_000;
      const receipts = [
        await publisher.publish(echoTask, { name: "notifications" }),
        await publisher.publishFuture(
          echoTask,
          { name: "notifications" },
          availability,
        ),
      ];
      await sleep(50);
      expect(notifications).toEqual([]);
      if (commit) {
        await f.commit();
        await expect
          .poll(() => notifications.length, { timeout: 1_000 })
          .toBe(2);
        expect(
          notifications.splice(0).map((value) => JSON.parse(value)),
        ).toEqual(
          receipts.map(({ taskId }, index) => ({
            kind: "new_task_available",
            task_name: echoTask.name,
            task_id: taskId,
            available_from_unix_ms: index === 0 ? null : availability,
          })),
        );
      } else {
        await f.rollback();
      }
      // This later event forms a barrier for rolled-back or duplicated trigger notifications.
      await f.client.query("SELECT pg_notify('bellows_tasks', 'barrier')");
      await expect.poll(() => notifications.length, { timeout: 1_000 }).toBe(1);
      expect(notifications.splice(0)).toEqual(["barrier"]);
    }
  } finally {
    await f.observer.query("UNLISTEN bellows_tasks");
  }
});

test("postgres publishing external schema options preserve search path and never initialize or clean up", async () => {
  const f = await publicationTransactionFixture();
  await f.observer.query("CREATE SCHEMA app_tasks; CREATE SCHEMA empty_schema");
  await initializePostgresSchema(f.database.url, "app_tasks");
  await f.begin();
  await f.client.query("SET LOCAL search_path = app_tasks, pg_catalog");
  for (const schema of [undefined, "public"]) {
    const publisher = PostgresPublishingBackend.fromExecutor(f.client, {
      schema,
    });
    await publisher.publish(ackTask, undefined);
    await publisher.close();
    await publisher.close();
    f.assertCallerOwned();
    expect((await f.client.query("SHOW search_path")).rows).toEqual([
      { search_path: "app_tasks, pg_catalog" },
    ]);
  }
  await f.commit();
  expect(await publicationCounts(f.observer, "public.bellows_tasks")).toEqual([
    "0",
    "1",
  ]);
  expect(
    await publicationCounts(f.observer, "app_tasks.bellows_tasks"),
  ).toEqual(["0", "1"]);
  for (const schema of [
    "not.valid",
    "missing_schema",
    "empty_schema",
    undefined,
  ]) {
    await f.begin();
    await f.client.query("SET LOCAL search_path = empty_schema, pg_catalog");
    if (schema === "not.valid") {
      const query = vi.spyOn(f.client, "query");
      try {
        expect(() =>
          PostgresPublishingBackend.fromExecutor(f.client, { schema }),
        ).toThrow("Database schema names");
        expect(query).not.toHaveBeenCalled();
      } finally {
        query.mockRestore();
      }
      await f.client.query("SELECT 1");
    } else {
      const publisher = PostgresPublishingBackend.fromExecutor(f.client, {
        schema,
      });
      await expect(publisher.publish(ackTask, undefined)).rejects.toMatchObject(
        {
          code: "42P01",
        },
      );
      await publisher.close();
      await publisher.close();
      await expect(f.client.query("SELECT 1")).rejects.toMatchObject({
        code: "25P02",
      });
    }
    f.assertCallerOwned();
    await f.rollback();
  }
  expect(
    (
      await f.observer.query(
        "SELECT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'missing_schema') AS created_schema, to_regclass('empty_schema.bellows_tasks') AS created_table",
      )
    ).rows,
  ).toEqual([{ created_schema: false, created_table: null }]);
  expect(await publicationCounts(f.observer, "public.bellows_tasks")).toEqual([
    "0",
    "1",
  ]);
  await f.client.query("SELECT 1");
});

async function publicationCounts(
  client: Client | PoolClient,
  table = "bellows_tasks",
) {
  const { rows } = await client.query<{ business: string; tasks: string }>(
    `SELECT (SELECT count(*) FROM business) AS business, (SELECT count(*) FROM ${table}) AS tasks`,
  );
  return [rows[0].business, rows[0].tasks];
}

async function publicationTransactionFixture() {
  const database = track(await TestPostgresDatabase.create("publishing_tx"));
  await initializePostgresSchema(database.url, "public");
  const observer = new Client({ connectionString: database.url });
  await observer.connect();
  await observer.query(
    "CREATE TABLE business (value INT UNIQUE DEFERRABLE INITIALLY DEFERRED)",
  );
  const pool = new Pool({ connectionString: database.url, max: 1 });
  const client = await pool.connect();
  const release = vi.spyOn(client, "release");
  const poolEnd = vi.spyOn(pool, "end");
  let transactionOpen = false;
  return track({
    database,
    observer,
    client,
    async begin() {
      await client.query("BEGIN");
      transactionOpen = true;
    },
    async commit() {
      try {
        await client.query("COMMIT");
      } finally {
        // These fixtures use a deferred constraint failure, which ends the transaction.
        transactionOpen = false;
      }
    },
    async rollback() {
      await client.query("ROLLBACK");
      transactionOpen = false;
    },
    assertCallerOwned() {
      expect(release).not.toHaveBeenCalled();
      expect(poolEnd).not.toHaveBeenCalled();
    },
    async close() {
      if (transactionOpen) {
        await this.rollback();
      }
      // Caller cleanup is deliberately outside the lifecycle-spy assertion window.
      release.mockRestore();
      poolEnd.mockRestore();
      client.release();
      await pool.end();
      await observer.end();
    },
  });
}

test("postgres publishing serial pool has no listener and awaited close removes its connections", async () => {
  const database = track(await TestPostgresDatabase.create("publishing_pool"));
  await initializePostgresSchema(database.url, "public");
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  const app = `publishing_${randomUUID().replaceAll("-", "")}`;
  const url = new URL(database.url);
  url.searchParams.set("application_name", app);
  const publisher = await PostgresPublishingBackend.connect(url.toString());
  let closed = false;
  try {
    for (let i = 0; i < 8; i++) {
      await publisher.publish(ackTask, undefined);
    }
    await waitForPublisherConnections(admin, app, 1);
    const activity = await admin.query<{ query: string }>(
      "SELECT query FROM pg_stat_activity WHERE application_name = $1",
      [app],
    );
    expect(
      activity.rows.every(
        ({ query }) => !query.toUpperCase().includes("LISTEN"),
      ),
    ).toBe(true);
    await publisher.close();
    closed = true;
    await waitForPublisherConnections(admin, app, 0);
    await expect(publisher.publish(ackTask, undefined)).rejects.toThrow(
      "Cannot use a pool after calling end on the pool",
    );
    await expect(
      publisher.publishFuture(ackTask, undefined, Date.now()),
    ).rejects.toThrow("Cannot use a pool after calling end on the pool");
  } finally {
    if (!closed) {
      await publisher.close();
    }
    await admin.end();
  }
});

test("postgres publishing close waits for a SQL-gated insert", async () => {
  const database = track(await TestPostgresDatabase.create("publishing_close"));
  await initializePostgresSchema(database.url, "public");
  const admin = new Client({ connectionString: database.url });
  await admin.connect();
  const app = `publishing_${randomUUID().replaceAll("-", "")}`;
  const url = new URL(database.url);
  url.searchParams.set("application_name", app);
  const publisher = await PostgresPublishingBackend.connect(url.toString());
  let insert: ReturnType<typeof publisher.publish> | undefined;
  let close: Promise<void> | undefined;
  let transactionOpen = false;
  try {
    await admin.query("BEGIN");
    transactionOpen = true;
    await admin.query("LOCK TABLE bellows_tasks IN ACCESS EXCLUSIVE MODE");
    let inserted = false;
    insert = publisher.publish(ackTask, undefined).then((receipt) => {
      inserted = true;
      return receipt;
    });
    await expect
      .poll(
        async () => {
          // The gate transaction otherwise caches activity before the lazy pool connects.
          await admin.query("SELECT pg_stat_clear_snapshot()");
          return (
            await admin.query(
              "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock') AS blocked",
              [app],
            )
          ).rows[0].blocked;
        },
        { timeout: 1_000 },
      )
      .toBe(true);
    let closed = false;
    close = publisher.close().then(() => {
      closed = true;
    });
    await sleep(20);
    expect(inserted).toBe(false);
    expect(closed).toBe(false);
    await admin.query("COMMIT");
    transactionOpen = false;
    const receipt = await insert;
    await close;
    expect(
      (
        await admin.query(
          "SELECT count(*) FROM bellows_tasks WHERE task_id = $1",
          [receipt.taskId],
        )
      ).rows,
    ).toEqual([{ count: "1" }]);
    await waitForPublisherConnections(admin, app, 0);
  } finally {
    if (transactionOpen) {
      await admin.query("ROLLBACK");
    }
    await insert?.catch(() => undefined);
    await (close ?? publisher.close());
    await admin.end();
  }
});

async function waitForPublisherConnections(
  admin: Client,
  app: string,
  expected: number,
): Promise<void> {
  await expect
    .poll(
      async () =>
        Number(
          (
            await admin.query(
              "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
              [app],
            )
          ).rows[0].count,
        ),
      { timeout: 1_000 },
    )
    .toBe(expected);
}

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

  // Drain stops new claims, not a claim already awaiting PostgreSQL.
  await Promise.all([
    dispatcherHandle.drain().then(() => processed.close()),
    (async () => {
      gate.release();
      for (
        let taskId = await processed.recv();
        taskId !== null;
        taskId = await processed.recv()
      ) {
        expect(taskId).toBe(firstTaskId);
        gate.release();
      }
    })(),
  ]);

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

  // An already-started claim may succeed when this worker releases ownership.
  await Promise.all([
    dispatcherHandle.drain().then(() => processed.close()),
    (async () => {
      gate.release();
      for (
        let taskId = await processed.recv();
        taskId !== null;
        taskId = await processed.recv()
      ) {
        expect(taskId).toBe(firstTaskId);
        gate.release();
      }
    })(),
  ]);

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
