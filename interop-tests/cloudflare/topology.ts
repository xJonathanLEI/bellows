import { afterEach, beforeEach, expect, test } from "vitest";
import {
  type CloudflarePostgresFixture,
  type ConsumedResponse,
  poll,
} from "./postgres-fixture.js";

const json = (body: unknown) => ({
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

export function cloudflareTopology(
  createFixture: () => CloudflarePostgresFixture,
): void {
  let fixture: CloudflarePostgresFixture;
  let expectedLogs: Array<{ level: string; message: string }>;

  beforeEach(async () => {
    expectedLogs = [];
    fixture = createFixture();
    await fixture.start();
  }, 10_000);

  function assertAttempt(
    response: ConsumedResponse,
    taskId: string,
    nextAction: { type: "done" } | { type: "retryAt"; atMs: number } = {
      type: "done",
    },
  ): void {
    expect(response.status, response.body).toBe(200);
    expect(JSON.parse(response.body)).toEqual({
      taskId,
      nextAction,
    });
  }

  test("immediate dispatch and completion leave no scheduler records while preserving warming", async () => {
    const gate = await fixture.gate();
    const taskId = await publish("/tasks", { name: "Fast dispatch" });
    await gate.blocked(1);
    const accepted = await fixture.schedule();
    expect(accepted.tasks).toEqual({});
    expect(accepted.metadata ?? null).toBeNull();
    expect(accepted.alarm).toBeGreaterThan(accepted.now);
    await dispatch("cloudflare_greeting", taskId);
    expect((await fixture.schedule()).alarm).toBe(accepted.alarm);
    await gate.release();
    await completed([{ taskId, name: "Fast dispatch" }]);
    await fixture.waitForIdle();
    const done = await fixture.schedule();
    expect(done.tasks).toEqual({});
    expect(done.metadata ?? null).toBeNull();
    expect(done.alarm).toBe(accepted.alarm);
  });

  test("Cron recovers a lost initial dispatch without task persistence or republishing", async () => {
    const gate = await fixture.gate();
    const taskId = await seed("cloudflare_greeting", { name: "Cron recovery" });
    await fixture.admin.query(
      `UPDATE ${fixture.table}
       SET available_from_unix_ms = floor(extract(epoch FROM statement_timestamp()) * 1000) - 1
       WHERE task_id = $1`,
      [taskId],
    );
    const before = await fixture.schedule();
    expect(before.tasks).toEqual({});
    expect(before.alarm).toBeNull();
    // A delayed event must use database wall time, not this nominal timestamp.
    await fixture.runScheduled("ok", new Date(0));
    await gate.blocked(1);
    const accepted = await fixture.schedule();
    expect(accepted.tasks).toEqual({});
    expect(accepted.metadata ?? null).toBeNull();
    expect(accepted.alarm).toBeGreaterThan(accepted.now);
    await dispatch("cloudflare_greeting", taskId);
    expect((await fixture.schedule()).alarm).toBe(accepted.alarm);
    await gate.release();
    await completed([{ taskId, name: "Cron recovery" }]);
    expect((await fixture.schedule()).tasks).toEqual({});
    expect(
      (
        await fixture.admin.query(
          `SELECT last_value FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
        )
      ).rows,
    ).toEqual([{ last_value: taskId }]);
  });

  for (const occupied of [false, true]) {
    test(`Cron excludes ${occupied ? "occupied leases" : "future tasks"} until due without pre-scheduling`, async () => {
      const name = occupied ? "Abandoned lease" : "Lost future dispatch";
      const taskId = await seed("cloudflare_greeting", { name });
      const atMs = Date.now() + 1_000;
      await fixture.admin.query(
        `UPDATE ${fixture.table} SET available_from_unix_ms = $1, lease_worker_id = $2 WHERE task_id = $3`,
        [atMs, occupied ? 123 : null, taskId],
      );
      const before = await fixture.state();
      // Neither a future nominal timestamp nor an old lease owner changes eligibility.
      await fixture.runScheduled("ok", new Date(atMs + 60_000));
      expect(Date.now()).toBeLessThan(atMs);
      expect(await fixture.state()).toEqual(before);
      expect(await fixture.executions(taskId)).toEqual([]);
      expect((await fixture.schedule()).tasks).toEqual({});
      expect((await fixture.schedule()).alarm).toBeNull();
      await fixture.waitForIdle();
      await databaseTime(atMs);
      expect((await fixture.state()).tasks[0].lease_worker_id).toBe(
        occupied ? "123" : null,
      );
      await fixture.runScheduled();
      await completed([{ taskId, name }]);
      expect(
        Number((await fixture.executions(taskId))[0].executed_at_ms),
      ).toBeGreaterThanOrEqual(atMs);
    });
  }

  async function databaseTime(atMs: number) {
    await poll(
      "database availability deadline",
      async () =>
        (
          await fixture.admin.query<{ due: boolean }>(
            "SELECT floor(extract(epoch FROM statement_timestamp()) * 1000) >= $1 AS due",
            [atMs],
          )
        ).rows[0].due,
      (due) => due,
    );
  }

  test("Cron discovery does not reserve a task whose lease changes before claim", async () => {
    const taskId = await seed("cloudflare_greeting", { name: "Claim race" });
    const claimGate = await fixture.gate("bellows_tasks");
    const businessGate = await fixture.gate();
    await fixture.runScheduled();
    await claimGate.blocked(1);
    const atMs = Date.now() + 1_000;
    await claimGate.client.query(
      `UPDATE ${fixture.table} SET lease_worker_id = 123, available_from_unix_ms = $1 WHERE task_id = $2`,
      [atMs, taskId],
    );
    await claimGate.release("COMMIT");
    await pending(taskId, atMs);
    expect((await fixture.state()).tasks[0].lease_worker_id).toBe("123");
    expect(await fixture.executions(taskId)).toEqual([]);
    expect(Date.now()).toBeLessThan(atMs);
    await businessGate.blocked(1);
    await businessGate.release();
    await completed([{ taskId, name: "Claim race" }]);
    expect(
      Number((await fixture.executions(taskId))[0].executed_at_ms),
    ).toBeGreaterThanOrEqual(atMs);
    await forgotten();
  });

  for (const mode of ["failure", "success"] as const) {
    test(`Cron recovers a committed ${mode} reschedule with a deliberately unregistered hint`, async () => {
      const atMs = Date.now() + 1_000;
      const name = `Lost ${mode} hint`;
      const taskId = await seed("cloudflare_scheduling", {
        name,
        mode,
        availableFromMs: atMs,
      });
      // Bypass the DO: consume the committed response but never register its retryAt.
      const response = await fixture.consume(
        fixture.processor.fetch(
          "/process",
          json({ taskId, taskName: "cloudflare_scheduling" }),
        ),
        "direct attempt with deliberately lost scheduling hint",
      );
      const action = JSON.parse(response.body).nextAction;
      assertAttempt(response, taskId, { type: "retryAt", atMs: action.atMs });
      expect(action.atMs).toBeGreaterThanOrEqual(atMs);
      expect(action.atMs).toBeLessThanOrEqual(atMs + 1);
      await fixture.waitForIdle();
      expect((await fixture.state()).tasks[0]).toMatchObject({
        task_id: taskId,
        lease_worker_id: null,
        available_from_unix_ms: String(atMs),
      });
      await fixture.runScheduled();
      expect(Date.now()).toBeLessThan(atMs);
      expect((await fixture.schedule()).tasks).toEqual({});
      expect((await fixture.schedule()).alarm).toBeNull();
      expect(await fixture.executions(taskId)).toHaveLength(1);
      await databaseTime(atMs);
      // No alarm exists that could be responsible for this recovery.
      expect((await fixture.schedule()).alarm).toBeNull();
      await fixture.runScheduled();
      const done = await poll(
        "Cron executes the second attempt under the original ID",
        () => fixture.state(),
        ({ tasks, processed }) =>
          tasks.length === 0 && processed[0]?.execution_count === 2,
      );
      expect(done.processed).toEqual([
        { task_id: taskId, name, execution_count: 2 },
      ]);
      const executions = await fixture.executions(taskId);
      expect(Number(executions[0].executed_at_ms)).toBeLessThan(atMs);
      expect(Number(executions[1].executed_at_ms)).toBeGreaterThanOrEqual(atMs);
      expect(
        (
          await fixture.admin.query(
            `SELECT last_value FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
          )
        ).rows,
      ).toEqual([{ last_value: taskId }]);
      await fixture.waitForIdle();
    });
  }

  test("overlapping Cron sweeps acknowledge the same due ID without concurrent business ownership", async () => {
    const taskId = await seed("cloudflare_greeting", { name: "Overlap" });
    const claims = await fixture.gate("bellows_tasks");
    const business = await fixture.gate();
    // Keep the row due while both independent sweeps discover and dispatch it.
    await Promise.all([fixture.runScheduled(), fixture.runScheduled()]);
    await claims.blocked(1);
    expect((await fixture.state()).tasks[0].lease_worker_id).toBeNull();
    expect((await fixture.schedule()).tasks).toEqual({});
    await claims.release();
    await business.blocked(1);
    await business.release();
    await completed([{ taskId, name: "Overlap" }]);
    expect(await fixture.executions(taskId)).toHaveLength(1);
    expect(
      (
        await fixture.admin.query(
          `SELECT last_value FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
        )
      ).rows,
    ).toEqual([{ last_value: taskId }]);
  });

  test("Cron reaches a second keyset page while every earlier task is still due and all business work is gated", async () => {
    const { rows } = await fixture.admin.query<{ task_id: string }>(
      `INSERT INTO ${fixture.table} (task_id, task_name, payload_json)
       OVERRIDING SYSTEM VALUE
       SELECT n * 3, 'cloudflare_greeting', json_build_object('name', 'Paged ' || n)::text
       FROM generate_series(1, 101) AS n RETURNING task_id`,
    );
    const claims = await fixture.gate("bellows_tasks");
    const business = await fixture.gate();
    await fixture.runScheduled();
    // All distinct requests, including page two, launch before any claim can change eligibility.
    await claims.blocked(rows.length);
    expect(
      (await fixture.state()).tasks.every(
        ({ lease_worker_id }) => lease_worker_id === null,
      ),
    ).toBe(true);
    expect((await fixture.schedule()).tasks).toEqual({});
    await claims.release();
    await business.blocked(rows.length);
    expect((await fixture.state()).processed).toEqual([]);
    await business.release();
    await completed(
      rows.map(({ task_id }, index) => ({
        taskId: task_id,
        name: `Paged ${index + 1}`,
      })),
    );
  });

  test("a discovery failure rejects the actual Cron invocation after cleanup and recovers after repair", async () => {
    const taskId = await seed("cloudflare_greeting", { name: "After repair" });
    await fixture.admin.query(
      `ALTER TABLE ${fixture.table} RENAME TO hidden_tasks`,
    );
    try {
      await fixture.runScheduled("exception");
      await fixture.waitForIdle();
      expect(await fixture.activeRequestClients()).toEqual([]);
      expect((await fixture.schedule()).tasks).toEqual({});
      expect((await fixture.schedule()).alarm).toBeNull();
    } finally {
      await fixture.admin.query(
        `ALTER TABLE "${fixture.schema}".hidden_tasks RENAME TO bellows_tasks`,
      );
    }
    await fixture.runScheduled();
    await completed([{ taskId, name: "After repair" }]);
  });

  test("Cron preserves unsupported exact identities, excludes singletons and isolates the configured schema", async () => {
    const otherSchema = `${fixture.schema}_other`;
    await fixture.admin.query(`CREATE SCHEMA "${otherSchema}"`);
    try {
      await fixture.admin.query(
        `CREATE TABLE "${otherSchema}".bellows_tasks (LIKE ${fixture.table});
         INSERT INTO "${otherSchema}".bellows_tasks (task_id, task_name, payload_json)
         VALUES (17, 'cloudflare_greeting', '{"name":"Wrong schema"}')`,
      );
      const identities = [
        "-1",
        "0",
        "9007199254740991",
        "9007199254740992",
        "9007199254740993",
        "9223372036854775807",
      ];
      for (const taskId of identities) {
        await fixture.admin.query(
          `INSERT INTO ${fixture.table} (task_id, task_name, payload_json)
           OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3)`,
          [
            taskId,
            "cloudflare_greeting",
            JSON.stringify({ name: `Exact ${taskId}` }),
          ],
        );
      }
      await fixture.admin.query(
        `INSERT INTO ${fixture.table} (task_id, task_name, payload_json, task_unique_key)
         OVERRIDING SYSTEM VALUE
         VALUES (1, '', 'not JSON', NULL),
                (2, 'cloudflare_greeting', 'not JSON', 'singleton')`,
      );
      await nextTaskId("3");
      const taskId = await seed("cloudflare_full_name", {
        firstName: "Exact",
        lastName: "Name",
      });
      const before = await fixture.state();
      await fixture.runScheduled("exception");
      const done = await poll(
        "valid candidates complete despite unsupported identities",
        () => fixture.state(),
        ({ tasks, processed }) =>
          tasks.length === before.tasks.length - 2 && processed.length === 2,
      );
      expect(done.processed).toEqual([
        { task_id: taskId, name: "Exact Name", execution_count: 1 },
        {
          task_id: "9007199254740991",
          name: "Exact 9007199254740991",
          execution_count: 1,
        },
      ]);
      expect(done.tasks).toEqual(
        before.tasks.filter(
          (task) =>
            task.task_id !== taskId && task.task_id !== "9007199254740991",
        ),
      );
      expect(
        (
          await fixture.admin.query(
            `SELECT task_id::text FROM "${otherSchema}".bellows_tasks`,
          )
        ).rows,
      ).toEqual([{ task_id: "17" }]);
      expect((await fixture.schedule()).tasks).toEqual({});
      await fixture.waitForIdle();
    } finally {
      await fixture.admin.query(`DROP SCHEMA "${otherSchema}" CASCADE`);
    }
  });

  async function pending(taskId: string, atMs?: number) {
    const state = await poll(
      `persisted pending instruction for ${taskId}`,
      () => fixture.schedule(),
      ({ tasks }) => {
        const task = tasks[`task:${taskId}`];
        return (
          task?.state.type === "pending" &&
          (atMs === undefined ||
            (task.nextAttemptAtMs >= atMs && task.nextAttemptAtMs <= atMs + 1))
        );
      },
    );
    return state.tasks[`task:${taskId}`];
  }

  async function forgotten() {
    await poll(
      "matching done instructions remove all accepted tasks",
      () => fixture.schedule(),
      ({ tasks }) => Object.keys(tasks).length === 0,
    );
  }

  test("automatically follows an externally occupied and extended lease without building early", async () => {
    const taskId = await seed("cloudflare_greeting", { name: "Lease hint" });
    const firstExpiration = Date.now() + 1_000;
    const secondExpiration = firstExpiration + 1_000;
    await fixture.admin.query(
      `UPDATE ${fixture.table} SET lease_worker_id = 123, available_from_unix_ms = $1 WHERE task_id = $2`,
      [firstExpiration, taskId],
    );
    const gate = await fixture.gate();
    await dispatch("cloudflare_greeting", taskId);
    await pending(taskId, firstExpiration);
    const first = await fixture.schedule();
    expect(first.now).toBeLessThan(firstExpiration);
    expect(await fixture.activeRequestClients()).toHaveLength(0);
    expect(await fixture.executions(taskId)).toEqual([]);
    await fixture.admin.query(
      `UPDATE ${fixture.table} SET available_from_unix_ms = $1 WHERE task_id = $2 AND lease_worker_id = 123`,
      [secondExpiration, taskId],
    );
    // No redispatch: the first alarm must observe the renewed lease and retain its later hint.
    await pending(taskId, secondExpiration);
    const extended = await fixture.schedule();
    expect(extended.metadata.nextAttemptId).toBeGreaterThan(
      first.metadata.nextAttemptId,
    );
    expect(extended.now).toBeLessThan(secondExpiration);
    expect(extended.alarm).toBe(
      extended.tasks[`task:${taskId}`].nextAttemptAtMs,
    );
    expect((await fixture.state()).tasks[0].lease_worker_id).toBe("123");
    expect(await fixture.activeRequestClients()).toHaveLength(0);
    expect(await fixture.executions(taskId)).toEqual([]);
    await gate.blocked(1);
    await gate.release();
    await completed([{ taskId, name: "Lease hint" }]);
    expect(
      Number((await fixture.executions(taskId))[0].executed_at_ms),
    ).toBeGreaterThanOrEqual(secondExpiration);
    await forgotten();
  }, 10_000);

  for (const mode of ["failure", "success", "immediate"] as const) {
    test(`automatically executes a committed ${mode} reschedule under the same ID`, async () => {
      const atMs = Date.now() + 1_500;
      const name = `${mode} reschedule`;
      const taskId = await publish("/scheduled", {
        name,
        mode,
        availableFromMs: atMs,
      });
      if (mode !== "immediate") {
        const record = await pending(taskId, atMs);
        expect(record.infrastructureFailures).toBe(0);
        // The DO cannot act on an intended schedule before PostgreSQL has committed it.
        const state = await fixture.state();
        expect(state.tasks).toHaveLength(1);
        expect(state.tasks[0]).toMatchObject({
          task_id: taskId,
          task_name: "cloudflare_scheduling",
          lease_worker_id: null,
          available_from_unix_ms: String(atMs),
        });
        expect(state.processed).toEqual([
          { task_id: taskId, name, execution_count: 1 },
        ]);
        expect(Date.now()).toBeLessThan(atMs);
      }
      const done = await poll(
        "second attempt completes without a producer request",
        () => fixture.state(),
        ({ tasks, processed }) =>
          tasks.length === 0 && processed[0]?.execution_count === 2,
      );
      expect(done.processed).toEqual([
        { task_id: taskId, name, execution_count: 2 },
      ]);
      const executions = await fixture.executions(taskId);
      expect(executions.map(({ execution_count }) => execution_count)).toEqual([
        1, 2,
      ]);
      if (mode !== "immediate") {
        expect(Number(executions[0].executed_at_ms)).toBeLessThan(atMs);
        expect(Number(executions[1].executed_at_ms)).toBeGreaterThanOrEqual(
          atMs,
        );
      }
      expect(
        (
          await fixture.admin.query(
            `SELECT last_value FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
          )
        ).rows,
      ).toEqual([{ last_value: taskId }]);
      await forgotten();
      await fixture.waitForIdle();
    }, 10_000);
  }

  test("recovers a scheduled task's lost committed response after real-storage reconstruction without republishing", async () => {
    const taskId = await seed("cloudflare_greeting", { name: "Lost response" });
    const atMs = Date.now() + 1_500;
    await fixture.admin.query(
      `UPDATE ${fixture.table} SET available_from_unix_ms = $1 WHERE task_id = $2`,
      [atMs, taskId],
    );
    const gate = await fixture.gate();
    await dispatch("cloudflare_greeting", taskId);
    await pending(taskId, atMs);
    await fixture.consume(
      fixture.processor.fetch("/__test/lose-response", json({ taskId })),
      "arm fixture response loss",
    );
    await gate.blocked(1);
    const running = await fixture.schedule();
    expect(running.tasks[`task:${taskId}`].state.type).toBe("running");
    const dispatcher = await fixture.dispatcher();
    await fixture.consume(
      dispatcher.fetch("https://dispatcher/__test/reconstruct"),
      "reconstruct with a persisted in-flight attempt",
    );
    expect(await fixture.schedule()).toMatchObject({
      metadata: running.metadata,
      tasks: running.tasks,
      alarm: running.alarm,
    });
    await gate.release();
    await completed([{ taskId, name: "Lost response" }]);
    const retry = await pending(taskId);
    expect(retry.infrastructureFailures).toBe(1);
    expectedLogs.push({
      level: "error",
      message: `task processor failed ${taskId} task processor returned HTTP 503: fixture response lost`,
    });
    await fixture.consume(
      dispatcher.fetch("https://dispatcher/__test/reconstruct"),
      "reconstruct the persisted infrastructure retry",
    );
    expect((await fixture.schedule()).tasks[`task:${taskId}`]).toEqual(retry);
    // Automatic retry observes the missing PostgreSQL row; no new business worker is built.
    const missingGate = await fixture.gate("bellows_tasks");
    const pids = await missingGate.blocked(1);
    const retrying = await fixture.schedule();
    expect(retrying.tasks[`task:${taskId}`].state.type).toBe("running");
    expect(retrying.metadata.nextAttemptId).toBeGreaterThan(
      running.metadata.nextAttemptId,
    );
    await missingGate.release();
    await fixture.waitForClientExit(pids);
    await forgotten();
    await completed([{ taskId, name: "Lost response" }]);
    expect(await fixture.executions(taskId)).toHaveLength(1);
    expect(
      (
        await fixture.admin.query(
          `SELECT last_value FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
        )
      ).rows,
    ).toEqual([{ last_value: taskId }]);
  }, 10_000);

  test("fans out simultaneous future deadlines across definitions while every business operation is gated", async () => {
    const gate = await fixture.gate();
    const atMs = Date.now() + 1_500;
    const tasks = await Promise.all(
      Array.from({ length: 12 }, async (_, index) => {
        const fullName = index % 2 === 1;
        const name = fullName ? `Family ${index}` : `Greeting ${index}`;
        const taskId = await publish(
          `${fullName ? "/full-names" : "/tasks"}?availableFromMs=${atMs}`,
          fullName
            ? { firstName: "Family", lastName: String(index) }
            : { name },
        );
        return { taskId, name };
      }),
    );
    await Promise.all(tasks.map(({ taskId }) => pending(taskId, atMs)));
    expect(Date.now()).toBeLessThan(atMs);
    expect((await fixture.state()).processed).toEqual([]);
    // Every automatic attempt reaches its separate connection before any can finish.
    await gate.blocked(tasks.length);
    expect(
      (await fixture.state()).tasks.every(
        ({ lease_worker_id }) => lease_worker_id !== null,
      ),
    ).toBe(true);
    await gate.release();
    await completed(tasks.sort((a, b) => Number(a.taskId) - Number(b.taskId)));
    for (const { taskId } of tasks) {
      const executions = await fixture.executions(taskId);
      expect(executions).toHaveLength(1);
      expect(Number(executions[0].executed_at_ms)).toBeGreaterThanOrEqual(atMs);
    }
    await forgotten();
  }, 10_000);

  test("publishes future work once and reconstructs its real-storage schedule before an automatic alarm", async () => {
    const atMs = Date.now() + 1_500;
    const gate = await fixture.gate();
    const dispatcher = await fixture.dispatcher();
    const taskId = await publish(`/tasks?availableFromMs=${atMs}`, {
      name: "Durable hint",
    });
    const inspect = () => fixture.schedule();
    const pending = await poll(
      "committed future hint",
      inspect,
      (state) => state.tasks[`task:${taskId}`]?.state.type === "pending",
    );
    expect(
      pending.tasks[`task:${taskId}`].nextAttemptAtMs,
    ).toBeGreaterThanOrEqual(atMs);
    expect(pending.tasks[`task:${taskId}`].nextAttemptAtMs).toBeLessThanOrEqual(
      atMs + 1,
    );
    expect(pending.alarm).toBe(pending.tasks[`task:${taskId}`].nextAttemptAtMs);
    expect(pending.now).toBeLessThan(atMs);
    expect((await fixture.state()).tasks[0].lease_worker_id).toBeNull();
    expect((await fixture.state()).tasks[0].available_from_unix_ms).toBe(
      String(atMs),
    );
    expect((await fixture.state()).processed).toEqual([]);
    await fixture.consume(
      dispatcher.fetch("https://dispatcher/__test/reconstruct"),
      "reconstruct delegate, retaining real storage",
    );
    expect(await inspect()).toMatchObject({
      metadata: pending.metadata,
      tasks: pending.tasks,
      alarm: pending.alarm,
    });
    // No second dispatch or manual alarm. This waits for a real service-bound worker
    // admitted by PostgreSQL after the platform automatically delivers the shared alarm.
    await gate.blocked(1);
    expect(Date.now()).toBeGreaterThanOrEqual(atMs);
    await gate.release();
    await completed([{ taskId, name: "Durable hint" }]);
    const executions = await fixture.executions(taskId);
    expect(executions).toHaveLength(1);
    expect(Number(executions[0].executed_at_ms)).toBeGreaterThanOrEqual(atMs);
    expect(
      (
        await fixture.admin.query(
          `SELECT last_value FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
        )
      ).rows,
    ).toEqual([{ last_value: taskId }]);
    const done = await poll(
      "done instruction deletes durable task",
      inspect,
      (state) => Object.keys(state.tasks).length === 0,
    );
    expect(done.metadata.nextAttemptId).toBeGreaterThan(
      pending.metadata.nextAttemptId,
    );
    expect(done.alarm).toBe(pending.metadata.nextHeartbeatAtMs);
  }, 10_000);

  afterEach(async ({ task }) => {
    try {
      try {
        if (task.result?.state === "fail") {
          await fixture.debug();
        }
      } finally {
        // Stop fixture-owned retained retries before releasing gates or dropping its schema.
        try {
          const dispatcher = await fixture.dispatcher();
          await fixture.consume(
            dispatcher.fetch("https://dispatcher/__test/clear"),
            "clear fixture schedule",
          );
        } finally {
          await fixture.close();
        }
      }
      // Check after shutdown too: canceled I/O and request-context warnings are
      // failures, not acceptable noise. Only explicit negative-test errors pass.
      expect(
        fixture.server.getLogs().map(({ level, message }) => ({
          level,
          message,
        })),
      ).toEqual(expectedLogs);
    } catch (error) {
      await fixture.debug();
      throw error;
    }
  }, 10_000);

  async function publish(path: string, payload: unknown): Promise<string> {
    const response = await fixture.consume(
      fixture.server.fetch(path, json(payload)),
      "producer acceptance and complete response body while processing is gated",
    );
    expect(response.status, response.body).toBe(202);
    expect(response.body).toMatch(/^[1-9][0-9]*$/);
    expect(Number.isSafeInteger(Number(response.body))).toBe(true);
    return response.body;
  }

  async function dispatch(taskName: string, taskId: string) {
    const dispatcher = await fixture.dispatcher();
    const response = await fixture.consume(
      dispatcher.fetch(
        "https://dispatcher/dispatch",
        json({ taskId, taskName }),
      ),
      `Durable Object acceptance for ${taskId}`,
    );
    expect(response.status, response.body).toBe(200);
    const body = JSON.parse(response.body) as {
      ok: boolean;
      taskId: string;
      duplicate?: boolean;
    };
    expect(body).toMatchObject({ ok: true, taskId });
    return body;
  }

  async function redispatch(taskName: string, taskId: string) {
    const response = await poll(
      `dispatcher to release the previous attempt for ${taskId}`,
      () => dispatch(taskName, taskId),
      (body) => body.duplicate !== true,
    );
    expect(response).toEqual({ ok: true, taskId });
  }

  async function claimed(
    taskId: string,
    taskName: string,
    payloadJson: string,
  ) {
    const state = await poll(
      `task ${taskId} to acquire a real lease`,
      () => fixture.state(),
      ({ tasks }) =>
        tasks.some(
          (task) => task.task_id === taskId && task.lease_worker_id !== null,
        ),
    );
    const task = state.tasks.find((row) => row.task_id === taskId);
    expect(task).toMatchObject({
      task_id: taskId,
      task_name: taskName,
      payload_json: payloadJson,
      task_unique_key: null,
      callback_id: null,
    });
    expect(Number.isSafeInteger(Number(task?.lease_worker_id))).toBe(true);
    expect(Number(task?.lease_worker_id)).toBeGreaterThan(0);
    expect(Number(task?.lease_worker_id)).toBeLessThan(2 ** 48);
    expect(Number(task?.available_from_unix_ms)).toBeGreaterThan(Date.now());
    expect(state.processed).toEqual([]);
    return task?.lease_worker_id;
  }

  async function completed(tasks: Array<{ taskId: string; name: string }>) {
    const state = await poll(
      "persisted side effects and deletion of completed published tasks",
      () => fixture.state(),
      (value) =>
        value.tasks.length === 0 && value.processed.length === tasks.length,
    );
    expect(state.processed).toEqual(
      tasks.map(({ taskId, name }) => ({
        task_id: taskId,
        name,
        execution_count: 1,
      })),
    );
    await fixture.waitForIdle();
  }

  async function seed(taskName: string, payload: unknown): Promise<string> {
    const { rows } = await fixture.admin.query<{ task_id: string }>(
      `INSERT INTO ${fixture.table} (task_name, payload_json)
       VALUES ($1, $2) RETURNING task_id`,
      [taskName, JSON.stringify(payload)],
    );
    return rows[0].task_id;
  }

  test("commits publication and fully acknowledges before claimed processing completes", async () => {
    const gate = await fixture.gate();
    const payload = { name: "Alice" };
    const taskId = await publish("/tasks", payload);
    // publish() has consumed the whole 202 body. The transaction still holds the
    // write gate, so neither an unconsumed response nor a sleep can keep work alive.
    await claimed(taskId, "cloudflare_greeting", JSON.stringify(payload));
    await gate.blocked(1);
    // Only lease operations + side effects remain. The producer client already closed.
    expect(await fixture.activeRequestClients()).toHaveLength(2);
    await gate.release();
    await completed([{ taskId, name: "Alice" }]);
  }, 10_000);

  test("closes after an insert constraint failure without retrying and publishes after repair", async () => {
    await fixture.admin.query(
      `ALTER TABLE ${fixture.table} ADD CONSTRAINT reject_insert CHECK (false)`,
    );
    const gate = await fixture.gate("bellows_tasks");
    const response = fixture.consume(
      fixture.server.fetch("/tasks", json({ name: "After repair" })),
      "failed producer publication and shutdown",
    );
    const pids = await gate.blocked(1);
    await gate.release();
    const failed = await response;
    expect(failed.status, failed.body).toBe(503);
    expect(JSON.parse(failed.body)).toEqual({
      error: "task publication failed",
    });
    await fixture.waitForClientExit(pids);
    expect(await fixture.activeRequestClients()).toHaveLength(0);
    expect(await fixture.state()).toEqual({ tasks: [], processed: [] });
    expect(
      (
        await fixture.admin.query(
          `SELECT last_value, is_called FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
        )
      ).rows,
    ).toEqual([{ last_value: "1", is_called: true }]);

    await fixture.admin.query(
      `ALTER TABLE ${fixture.table} DROP CONSTRAINT reject_insert`,
    );
    const taskId = await publish("/tasks", { name: "After repair" });
    expect(taskId).toBe("2");
    await completed([{ taskId, name: "After repair" }]);
  }, 10_000);

  async function nextTaskId(taskId: string): Promise<void> {
    await fixture.admin.query(
      "SELECT setval($1::regclass, $2::bigint, false)",
      [`"${fixture.schema}".bellows_tasks_task_id_seq`, taskId],
    );
  }

  test("accepts and processes publication at the maximum safe ID", async () => {
    await nextTaskId("9007199254740991");
    const gate = await fixture.gate();
    const payload = { name: "Maximum safe ID" };
    const taskId = await publish("/tasks", payload);
    expect(taskId).toBe("9007199254740991");
    await claimed(taskId, "cloudflare_greeting", JSON.stringify(payload));
    await gate.blocked(1);
    expect(await fixture.activeRequestClients()).toHaveLength(2);
    await gate.release();
    await completed([{ taskId, name: "Maximum safe ID" }]);
  }, 10_000);

  for (const taskId of [
    "9007199254740992",
    "9007199254740993",
    "9223372036854775807",
  ]) {
    test(`retains the exact unsupported publication ID ${taskId} without dispatch`, async () => {
      await nextTaskId(taskId);
      const gate = await fixture.gate("bellows_tasks");
      const response = fixture.consume(
        fixture.server.fetch("/tasks", json({ name: "Unsupported ID" })),
        "unsupported producer ID and shutdown",
      );
      const pids = await gate.blocked(1);
      await gate.release();
      const result = await response;
      expect(result.status, result.body).toBe(503);
      expect(JSON.parse(result.body)).toEqual({
        error: "task published, but dispatch acceptance was not confirmed",
        taskId,
      });
      await fixture.waitForClientExit(pids);
      expect(await fixture.activeRequestClients()).toHaveLength(0);
      expect(await fixture.state()).toEqual({
        tasks: [
          {
            task_id: taskId,
            task_name: "cloudflare_greeting",
            task_unique_key: null,
            payload_json: JSON.stringify({ name: "Unsupported ID" }),
            callback_id: null,
            lease_worker_id: null,
            available_from_unix_ms: null,
          },
        ],
        processed: [],
      });
      expect(
        (
          await fixture.admin.query(
            `SELECT last_value FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
          )
        ).rows,
      ).toEqual([{ last_value: taskId }]);
      // Strict teardown log checks also reject any attempted processor rejection.
    }, 10_000);
  }

  test("suppresses duplicate IDs across names while heterogeneous tasks execute concurrently", async () => {
    const gate = await fixture.gate();
    const greeting = { name: "Alice" };
    const fullName = { firstName: "Bob", lastName: "Smith" };
    const firstId = await publish("/tasks", greeting);
    const firstOwner = await claimed(
      firstId,
      "cloudflare_greeting",
      JSON.stringify(greeting),
    );
    expect(await dispatch("cloudflare_greeting", firstId)).toEqual({
      ok: true,
      taskId: firstId,
      duplicate: true,
    });

    const secondId = await publish("/full-names", fullName);
    expect(secondId).not.toBe(firstId);
    const secondOwner = await claimed(
      secondId,
      "cloudflare_full_name",
      JSON.stringify(fullName),
    );
    expect(secondOwner).not.toBe(firstOwner);
    // Both complete 202 bodies have been consumed; the single service-bound
    // processor is still blocked on two independent business connections.
    await gate.blocked(2);
    for (const taskId of [firstId, secondId]) {
      for (const taskName of ["cloudflare_greeting", "cloudflare_full_name"]) {
        expect(await dispatch(taskName, taskId)).toEqual({
          ok: true,
          taskId,
          duplicate: true,
        });
      }
    }
    await gate.blocked(2);
    expect(await fixture.activeRequestClients()).toHaveLength(4);
    await gate.release();
    await completed([
      { taskId: firstId, name: "Alice" },
      { taskId: secondId, name: "Bob Smith" },
    ]);
  }, 10_000);

  test("cannot claim a persisted full-name task under the registered greeting name", async () => {
    const payload = { firstName: "Ada", lastName: "Lovelace" };
    const taskId = await seed("cloudflare_full_name", payload);
    const unclaimed = await fixture.state();
    expect(unclaimed).toEqual({
      tasks: [
        {
          task_id: taskId,
          task_name: "cloudflare_full_name",
          payload_json: JSON.stringify(payload),
          task_unique_key: null,
          callback_id: null,
          lease_worker_id: null,
          available_from_unix_ms: null,
        },
      ],
      processed: [],
    });
    const response = await fixture.consume(
      fixture.processor.fetch(
        "/process",
        json({
          taskId,
          taskName: "cloudflare_greeting",
          payload: { name: "Not the persisted definition or payload" },
        }),
      ),
      "mismatched definition attempt and complete backend shutdown",
    );
    assertAttempt(response, taskId);
    expect(await fixture.state()).toEqual(unclaimed);
    expect(await fixture.activeRequestClients()).toHaveLength(0);

    const gate = await fixture.gate();
    expect(await dispatch("cloudflare_full_name", taskId)).toEqual({
      ok: true,
      taskId,
    });
    await claimed(taskId, "cloudflare_full_name", JSON.stringify(payload));
    await gate.blocked(1);
    expect(await fixture.activeRequestClients()).toHaveLength(2);
    await gate.release();
    await completed([{ taskId, name: "Ada Lovelace" }]);
  }, 10_000);

  test("preserves database ownership and harmlessly redelivers a completed ID", async () => {
    const gate = await fixture.gate();
    const payload = { name: "Claimed payload" };
    const taskId = await publish("/tasks", payload);
    const owner = await claimed(
      taskId,
      "cloudflare_greeting",
      JSON.stringify(payload),
    );
    await gate.blocked(1);
    const leased = (await fixture.state()).tasks.find(
      (row) => row.task_id === taskId,
    );
    const expiration = Number(leased?.available_from_unix_ms);
    const competing = await fixture.consume(
      fixture.processor.fetch(
        "/process",
        json({
          taskId,
          taskName: "cloudflare_greeting",
          payload: { name: "Not the claimed payload" },
        }),
      ),
      "competing processor attempt to finish without claiming or writing",
    );
    const action = JSON.parse(competing.body).nextAction;
    assertAttempt(competing, taskId, { type: "retryAt", atMs: action.atMs });
    // Rust conservatively rounds the local deadline up by at most one millisecond.
    expect(action.atMs).toBeGreaterThanOrEqual(expiration);
    expect(action.atMs).toBeLessThanOrEqual(expiration + 1);
    expect(
      await claimed(taskId, "cloudflare_greeting", JSON.stringify(payload)),
    ).toBe(owner);
    await gate.blocked(1);
    // The no-claim competitor has also awaited its request-scoped driver shutdown.
    expect(await fixture.activeRequestClients()).toHaveLength(2);
    await gate.release();
    await completed([{ taskId, name: "Claimed payload" }]);

    // Only after the task has been deleted, gate its absent-row claim statement.
    // Observing a blocked UPDATE and then that connection's exit proves the real
    // redelivery reached Postgres and ended, rather than asserting immediately
    // after another acceptance response. No live task row is locked.
    const redeliveryGate = await fixture.gate("bellows_tasks");
    await redispatch("cloudflare_greeting", taskId);
    const redeliveryPids = await redeliveryGate.blocked(1);
    await redeliveryGate.release();
    await fixture.waitForClientExit(redeliveryPids);
    await completed([{ taskId, name: "Claimed payload" }]);
  }, 10_000);

  test("reports future availability without adding blocked query latency", async () => {
    const taskId = await seed("cloudflare_greeting", { name: "Future" });
    const atMs = Date.now() + 60_000;
    await fixture.admin.query(
      `UPDATE ${fixture.table} SET available_from_unix_ms = $1 WHERE task_id = $2`,
      [atMs, taskId],
    );
    const gate = await fixture.gate("bellows_tasks");
    const response = fixture.consume(
      fixture.processor.fetch(
        "/process",
        json({ taskId, taskName: "cloudflare_greeting" }),
      ),
      "future scheduling instruction after blocked claim and backend shutdown",
    );
    await gate.blocked(1);
    // Hold a real query long enough to distinguish a paired clock conversion from query latency.
    await fixture.admin.query("SELECT pg_sleep(0.05)");
    await gate.release();
    const result = await response;
    const action = JSON.parse(result.body).nextAction;
    assertAttempt(result, taskId, { type: "retryAt", atMs: action.atMs });
    expect(action.atMs).toBeGreaterThanOrEqual(atMs);
    expect(action.atMs).toBeLessThanOrEqual(atMs + 1);
    const state = await fixture.state();
    expect(state.processed).toEqual([]);
    expect(state.tasks[0]).toMatchObject({
      task_id: taskId,
      lease_worker_id: null,
      available_from_unix_ms: String(atMs),
    });
    expect(await fixture.activeRequestClients()).toHaveLength(0);
  }, 10_000);

  test("retries a committed business failure automatically after repair", async () => {
    await fixture.admin.query(`
ALTER TABLE ${fixture.processedTable}
ADD CONSTRAINT reject_retry_name CHECK (name <> 'Retry after repair')
    `);
    const gate = await fixture.gate();
    const payload = { name: "Retry after repair" };
    const taskId = await publish("/tasks", payload);
    await claimed(taskId, "cloudflare_greeting", JSON.stringify(payload));
    await gate.blocked(1);
    await gate.release();

    const failed = await poll(
      "failed processing to release its lease without deleting the task",
      () => fixture.state(),
      ({ tasks }) =>
        tasks.length === 1 &&
        tasks[0].lease_worker_id === null &&
        tasks[0].available_from_unix_ms === null,
    );
    expect(failed.tasks[0]).toMatchObject({
      task_id: taskId,
      task_name: "cloudflare_greeting",
      payload_json: JSON.stringify({ name: "Retry after repair" }),
    });
    expect(failed.processed).toEqual([]);
    await fixture.admin.query(
      `ALTER TABLE ${fixture.processedTable} DROP CONSTRAINT reject_retry_name`,
    );
    await completed([{ taskId, name: "Retry after repair" }]);
  }, 10_000);

  test("validates real routes and payloads and retains rejected processor attempts", async () => {
    const assertError = (response: ConsumedResponse, status: number) => {
      expect(response.status, response.body).toBe(status);
      expect(JSON.parse(response.body)).toHaveProperty("error");
    };
    for (const [path, init, status] of [
      ["/missing", json({ name: "Alice" }), 404],
      ["/tasks", { method: "GET" }, 405],
      ["/tasks", { method: "POST", body: "{}" }, 415],
      ["/tasks", { ...json({}), body: "{" }, 400],
      ...["", "-1", "+1", "1.5", "1e3", "Infinity", "8640000000000001"].flatMap(
        (at) =>
          [
            [
              `/tasks?availableFromMs=${encodeURIComponent(at)}`,
              json({ name: "Future" }),
              400,
            ],
            [
              `/full-names?availableFromMs=${encodeURIComponent(at)}`,
              json({ firstName: "Future", lastName: "Name" }),
              400,
            ],
          ] as const,
      ),
      ...[
        null,
        [],
        {},
        { name: " ", mode: "success", availableFromMs: 1 },
        { name: "Scheduled", mode: "unknown", availableFromMs: 1 },
        { name: "Scheduled", mode: "failure", availableFromMs: -1 },
        { name: "Scheduled", mode: "failure", availableFromMs: 1.5 },
        {
          name: "Scheduled",
          mode: "failure",
          availableFromMs: 8_640_000_000_000_001,
        },
      ].map((body) => ["/scheduled", json(body), 400] as const),
      ...[
        null,
        [],
        "Alice",
        {},
        { name: 1 },
        { name: " " },
        { name: "a".repeat(201) },
        { name: "🦀".repeat(101) },
        { name: "\ufeff" },
      ].map((body) => ["/tasks", json(body), 400] as const),
    ] as const) {
      assertError(
        await fixture.consume(
          fixture.server.fetch(path, init),
          `invalid producer request ${path}`,
        ),
        status,
      );
    }
    const canonicalError = "taskId must be a canonical positive decimal string";
    for (const [path, init, status, error] of [
      ["/missing", json({ taskId: "1" }), 404, "not-found"],
      ["/process", { method: "GET" }, 405, "method-not-allowed"],
      [
        "/process",
        { method: "POST", body: "{}" },
        415,
        "content-type must be application/json",
      ],
      [
        "/process",
        { ...json({}), headers: { "content-type": "text/plain" } },
        415,
        "content-type must be application/json",
      ],
      ["/process", { ...json({}), body: "{" }, 400, "invalid JSON"],
      ...[null, [], "1", {}, { taskId: 1 }].map(
        (body) => ["/process", json(body), 400, canonicalError] as const,
      ),
      ...[
        "",
        "0",
        "-1",
        "01",
        "1.0",
        "1e2",
        "+1",
        " 1",
        "1 ",
        "1\n",
        "1\r\n",
        "١",
        "１",
        "NaN",
        "Infinity",
        "not-a-number",
        "99999999999999999",
      ].map(
        (taskId) =>
          [
            "/process",
            json({ taskId, taskName: "cloudflare_greeting" }),
            400,
            canonicalError,
          ] as const,
      ),
      [
        "/process",
        json({ taskId: "9007199254740992" }),
        400,
        "taskId must encode a positive safe integer canonically",
      ],
    ] as const) {
      const response = await fixture.consume(
        fixture.processor.fetch(path, init),
        `invalid processor request ${path}`,
      );
      expect(response.status, response.body).toBe(status);
      expect(JSON.parse(response.body)).toEqual({ error });
    }
    expect(await fixture.state()).toEqual({ tasks: [], processed: [] });
    expect(await fixture.activeRequestClients()).toHaveLength(0);

    for (const taskId of ["1", String(Number.MAX_SAFE_INTEGER)]) {
      const noClaim = await fixture.consume(
        fixture.processor.fetch(
          "/process",
          json({
            taskId,
            taskName: "cloudflare_greeting",
            payload: { name: "Ignored" },
          }),
        ),
        `safe-integer boundary ${taskId} with no task to claim`,
      );
      assertAttempt(noClaim, taskId);
      expect(await fixture.activeRequestClients()).toHaveLength(0);
      expect(await fixture.state()).toEqual({ tasks: [], processed: [] });
    }

    const taskId = "not-a-number";
    const expectedError = {
      level: "error",
      message:
        `task processor failed ${taskId} task processor returned HTTP 400: ` +
        '{"error":"taskId must be a canonical positive decimal string"}',
    };
    expect(await dispatch("cloudflare_greeting", taskId)).toEqual({
      ok: true,
      taskId,
    });
    await poll(
      "captured real processor rejection",
      () => fixture.server.getLogs(),
      (logs) => logs.length === 1,
    );
    expectedLogs.push(expectedError);
    expect(fixture.server.getLogs()[0]).toMatchObject(expectedError);

    await redispatch("cloudflare_greeting", taskId);
    await poll(
      "second rejected processor fetch to be consumed and logged",
      () => fixture.server.getLogs(),
      (logs) => logs.length === 2,
    );
    expectedLogs.push(expectedError);
    expect(fixture.server.getLogs()[1]).toMatchObject(expectedError);
    expect(await fixture.state()).toEqual({ tasks: [], processed: [] });
  }, 10_000);

  test("rejects malformed and unknown task names and permits corrected pending redispatch", async () => {
    const payload = { firstName: "Grace", lastName: "Hopper" };
    const taskId = await seed("cloudflare_full_name", payload);
    const unclaimed = await fixture.state();
    const dispatcher = await fixture.dispatcher();
    for (const taskName of [undefined, "", null, 1, [], {}]) {
      for (const [target, path] of [
        [dispatcher, "https://dispatcher/dispatch"],
        [fixture.processor, "/process"],
      ] as const) {
        const response = await fixture.consume(
          target.fetch(path, json({ taskId, taskName })),
          `invalid task name ${JSON.stringify(taskName)} at ${path}`,
        );
        expect(response.status, response.body).toBe(400);
        expect(JSON.parse(response.body)).toEqual({
          error: "taskName must be a non-empty string",
          ...(target === dispatcher ? { ok: false } : {}),
        });
        expect(await fixture.state()).toEqual(unclaimed);
        expect(await fixture.activeRequestClients()).toHaveLength(0);
      }
    }
    for (const taskName of [
      "cloudflare_unknown",
      "Cloudflare_full_name",
      "cloudflare_full_name ",
      "__proto__",
    ]) {
      const response = await fixture.consume(
        fixture.processor.fetch("/process", json({ taskId, taskName })),
        `unknown task name ${taskName}`,
      );
      expect(response.status, response.body).toBe(404);
      expect(JSON.parse(response.body)).toEqual({
        error: "unknown task name",
      });
      expect(await fixture.state()).toEqual(unclaimed);
      expect(await fixture.activeRequestClients()).toHaveLength(0);
    }

    expect(await dispatch("cloudflare_unknown", taskId)).toEqual({
      ok: true,
      taskId,
    });
    const expectedError = {
      level: "error",
      message:
        `task processor failed ${taskId} task processor returned HTTP 404: ` +
        '{"error":"unknown task name"}',
    };
    await poll(
      "unknown-name response to be consumed and logged",
      () => fixture.server.getLogs(),
      (logs) => logs.length === 1,
    );
    expectedLogs.push(expectedError);
    expect(fixture.server.getLogs()[0]).toMatchObject(expectedError);
    expect(await fixture.state()).toEqual(unclaimed);
    expect(await fixture.activeRequestClients()).toHaveLength(0);

    const gate = await fixture.gate();
    await redispatch("cloudflare_full_name", taskId);
    await claimed(taskId, "cloudflare_full_name", JSON.stringify(payload));
    await gate.blocked(1);
    expect(await fixture.activeRequestClients()).toHaveLength(2);
    await gate.release();
    await completed([{ taskId, name: "Grace Hopper" }]);
  }, 10_000);

  test("validates full-name payloads and preserves exact components at the UTF-16 limit", async () => {
    const payloadError =
      "body must be an object with non-blank firstName and lastName of at most 200 characters each";
    for (const [init, status, error] of [
      [{ method: "GET" }, 405, "method-not-allowed"],
      [
        { method: "POST", body: "{}" },
        415,
        "content-type must be application/json",
      ],
      [{ ...json({}), body: "{" }, 400, "invalid JSON"],
      ...[null, [], "Alice", {}, { name: "Alice" }].map(
        (body) => [json(body), 400, payloadError] as const,
      ),
      ...[
        undefined,
        null,
        1,
        [],
        {},
        "",
        " ",
        "\ufeff",
        "a".repeat(201),
        "🦀".repeat(101),
      ].flatMap(
        (value) =>
          [
            [json({ firstName: value, lastName: "Smith" }), 400, payloadError],
            [json({ firstName: "Alice", lastName: value }), 400, payloadError],
          ] as const,
      ),
    ] as const) {
      const response = await fixture.consume(
        fixture.server.fetch("/full-names", init),
        "invalid full-name producer request",
      );
      expect(response.status, response.body).toBe(status);
      expect(JSON.parse(response.body)).toEqual({ error });
    }
    expect(await fixture.state()).toEqual({ tasks: [], processed: [] });
    expect(await fixture.activeRequestClients()).toHaveLength(0);

    const payload = {
      firstName: "🦀".repeat(100),
      lastName: ` ${"é".repeat(198)} `,
    };
    const gate = await fixture.gate();
    const taskId = await publish("/full-names", payload);
    await claimed(taskId, "cloudflare_full_name", JSON.stringify(payload));
    await gate.blocked(1);
    expect(await fixture.activeRequestClients()).toHaveLength(2);
    await gate.release();
    await completed([
      { taskId, name: `${payload.firstName} ${payload.lastName}` },
    ]);
  }, 10_000);
}
