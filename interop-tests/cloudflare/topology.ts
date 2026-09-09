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

  async function publish(name: string): Promise<string> {
    const response = await fixture.consume(
      fixture.server.fetch("/tasks", json({ name })),
      "producer acceptance and complete response body while processing is gated",
    );
    expect(response.status, response.body).toBe(202);
    expect(response.body).toMatch(/^[1-9][0-9]*$/);
    expect(Number.isSafeInteger(Number(response.body))).toBe(true);
    return response.body;
  }

  async function dispatch(taskId: string) {
    const dispatcher = await fixture.dispatcher();
    const response = await fixture.consume(
      dispatcher.fetch("https://dispatcher/dispatch", json({ taskId })),
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

  async function redispatch(taskId: string) {
    const response = await poll(
      `dispatcher to release the previous attempt for ${taskId}`,
      () => dispatch(taskId),
      (body) => body.duplicate !== true,
    );
    expect(response).toEqual({ ok: true, taskId });
  }

  async function claimed(taskId: string, name: string) {
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
      task_name: "cloudflare_greeting",
      payload_json: JSON.stringify({ name }),
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

  test("commits publication and fully acknowledges before claimed processing completes", async () => {
    const gate = await fixture.gate();
    const taskId = await publish("Alice");
    // publish() has consumed the whole 202 body. The transaction still holds the
    // write gate, so neither an unconsumed response nor a sleep can keep work alive.
    await claimed(taskId, "Alice");
    await gate.blocked(1);
    // Only lease operations + side effects remain. The producer client already closed.
    expect(await fixture.activeRequestClients()).toHaveLength(2);
    await gate.release();
    await completed([{ taskId, name: "Alice" }]);
  }, 10_000);

  test("suppresses an outstanding duplicate while distinct tasks execute concurrently", async () => {
    const gate = await fixture.gate();
    const firstId = await publish("Alice");
    const firstOwner = await claimed(firstId, "Alice");
    expect(await dispatch(firstId)).toEqual({
      ok: true,
      taskId: firstId,
      duplicate: true,
    });

    const secondId = await publish("Bob");
    expect(secondId).not.toBe(firstId);
    const secondOwner = await claimed(secondId, "Bob");
    expect(secondOwner).not.toBe(firstOwner);
    await gate.blocked(2);
    expect(await fixture.activeRequestClients()).toHaveLength(4);
    await gate.release();
    await completed([
      { taskId: firstId, name: "Alice" },
      { taskId: secondId, name: "Bob" },
    ]);
  }, 10_000);

  test("preserves database ownership and harmlessly redelivers a completed ID", async () => {
    const gate = await fixture.gate();
    const taskId = await publish("Claimed payload");
    const owner = await claimed(taskId, "Claimed payload");
    await gate.blocked(1);
    const competing = await fixture.consume(
      fixture.processor.fetch(
        "/process",
        json({ taskId, payload: { name: "Not the claimed payload" } }),
      ),
      "competing processor attempt to finish without claiming or writing",
    );
    expect(competing.status, competing.body).toBe(200);
    expect(JSON.parse(competing.body)).toEqual({
      taskId,
      attemptFinished: true,
    });
    expect(await claimed(taskId, "Claimed payload")).toBe(owner);
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
    await redispatch(taskId);
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
    const taskId = await publish("Retry after repair");
    await claimed(taskId, "Retry after repair");
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
    // The runtime handles the database failure. An attempt's HTTP 200 would not
    // establish success; only these database assertions can establish the result.
    await fixture.admin.query(
      `ALTER TABLE ${fixture.processedTable} DROP CONSTRAINT reject_retry_name`,
    );
    await redispatch(taskId);
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
    for (const [path, init, status] of [
      ["/missing", json({ taskId: "1" }), 404],
      ["/process", { method: "GET" }, 405],
      ["/process", { method: "POST", body: "{}" }, 415],
      ["/process", { ...json({}), body: "{" }, 400],
      ...[null, [], "1", {}, { taskId: 1 }].map(
        (body) => ["/process", json(body), 400] as const,
      ),
      ...[
        "0",
        "-1",
        "01",
        "1.0",
        "1e2",
        "+1",
        " 1",
        "1 ",
        "NaN",
        "Infinity",
        "not-a-number",
        "9007199254740992",
        "99999999999999999",
      ].map((taskId) => ["/process", json({ taskId }), 400] as const),
    ] as const) {
      assertError(
        await fixture.consume(
          fixture.processor.fetch(path, init),
          `invalid processor request ${path}`,
        ),
        status,
      );
    }
    const noClaim = await fixture.consume(
      fixture.processor.fetch(
        "/process",
        json({ taskId: String(Number.MAX_SAFE_INTEGER) }),
      ),
      "safe-integer boundary with no task to claim",
    );
    expect(noClaim.status, noClaim.body).toBe(200);
    expect(await fixture.state()).toEqual({ tasks: [], processed: [] });

    const taskId = "not-a-number";
    const expectedError = {
      level: "error",
      message:
        `task processor failed ${taskId} task processor returned HTTP 400: ` +
        '{"error":"taskId must be a canonical positive decimal string"}',
    };
    expect(await dispatch(taskId)).toEqual({ ok: true, taskId });
    await poll(
      "captured real processor rejection",
      () => fixture.server.getLogs(),
      (logs) => logs.length === 1,
    );
    expectedLogs.push(expectedError);
    expect(fixture.server.getLogs()[0]).toMatchObject(expectedError);

    await redispatch(taskId);
    await poll(
      "second rejected processor fetch to be consumed and logged",
      () => fixture.server.getLogs(),
      (logs) => logs.length === 2,
    );
    expectedLogs.push(expectedError);
    expect(fixture.server.getLogs()[1]).toMatchObject(expectedError);
    expect(await fixture.state()).toEqual({ tasks: [], processed: [] });
  }, 10_000);
}
