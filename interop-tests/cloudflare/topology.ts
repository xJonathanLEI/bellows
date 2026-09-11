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

  function assertAttempt(response: ConsumedResponse, taskId: string): void {
    expect(response.status, response.body).toBe(200);
    expect(JSON.parse(response.body)).toEqual({
      taskId,
      attemptFinished: true,
    });
  }

  afterEach(async ({ task }) => {
    try {
      try {
        if (task.result?.state === "fail") {
          await fixture.debug();
        }
      } finally {
        await fixture.close();
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
    assertAttempt(competing, taskId);
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

  test("releases a claim after a real constraint failure and permits explicit redispatch", async () => {
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
    await fixture.waitForIdle();

    const retry = await fixture.consume(
      fixture.processor.fetch(
        "/process",
        json({
          taskId,
          taskName: "cloudflare_greeting",
          payload: { name: "Not the claimed payload" },
        }),
      ),
      "handled constraint failure to finalize and drain before responding",
    );
    assertAttempt(retry, taskId);
    expect(await fixture.activeRequestClients()).toHaveLength(0);
    // HTTP 200 is not success, and the request cannot replace the failing claimed payload.
    expect(await fixture.state()).toEqual(failed);
    await fixture.admin.query(
      `ALTER TABLE ${fixture.processedTable} DROP CONSTRAINT reject_retry_name`,
    );
    await redispatch("cloudflare_greeting", taskId);
    await completed([{ taskId, name: "Retry after repair" }]);
  }, 10_000);

  test("validates real routes and payloads and releases a rejected processor fetch", async () => {
    const assertError = (response: ConsumedResponse, status: number) => {
      expect(response.status, response.body).toBe(status);
      expect(JSON.parse(response.body)).toHaveProperty("error");
    };
    for (const [path, init, status] of [
      ["/missing", json({ name: "Alice" }), 404],
      ["/tasks", { method: "GET" }, 405],
      ["/tasks", { method: "POST", body: "{}" }, 415],
      ["/tasks", { ...json({}), body: "{" }, 400],
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

  test("rejects malformed and unknown task names and releases downstream rejection for named redispatch", async () => {
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
