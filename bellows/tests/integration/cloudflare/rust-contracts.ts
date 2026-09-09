import {
  type CloudflarePostgresFixture,
  deadline,
  poll,
} from "bellows-cloudflare-interop-tests/cloudflare/postgres-fixture";
import { publisherContracts } from "bellows-cloudflare-interop-tests/cloudflare/publisher-contracts";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { createTestHarness, type TestHarness } from "wrangler";
import { createCloudflarePostgresFixture } from "./postgres-fixture.js";

const json = (body: unknown) => ({
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

// Registered only by the Rust suite. Real Rust projects and real namespace,
// service, storage and streaming-response adapters; no Node mocks of Worker I/O.
export function rustContracts(configPath: URL): void {
  publisherContracts("Rust workerd PostgreSQL publisher", () =>
    createCloudflarePostgresFixture({
      producer: {
        configPath: new URL("./wrangler.publisher.jsonc", configPath),
        prebuiltWorkerDir: new URL("./build/harness/", configPath),
      },
    }),
  );

  describe.sequential("Rust workerd PostgreSQL contracts", () => {
    let fixture: CloudflarePostgresFixture;
    beforeEach(async () => {
      fixture = createCloudflarePostgresFixture({
        producer: {
          configPath: new URL("./wrangler.postgres.jsonc", configPath),
          prebuiltWorkerDir: new URL("./build/harness/", configPath),
        },
      });
      await fixture.start();
    }, 10_000);
    afterEach(async ({ task }) => {
      try {
        if (task.result?.state === "fail") await fixture.debug();
        await fixture.close();
        expect(fixture.server.getLogs()).toEqual([]);
      } catch (error) {
        await fixture.debug();
        throw error;
      }
    }, 10_000);
    test("drains a cancelled query and resumes cancelled close before returning", async () => {
      const gate = await fixture.gate();
      let ended = false;
      const response = fixture
        .consume(
          fixture.server.fetch("/postgres/close"),
          "cancelled-query connection shutdown",
        )
        .finally(() => {
          ended = true;
        });
      const pids = await gate.blocked(1);
      expect(ended).toBe(false);
      await gate.release();
      expect(await response).toEqual({ status: 200, body: '{"closed":true}' });
      await fixture.waitForClientExit(pids);
      expect((await fixture.state()).processed).toEqual([
        {
          task_id: "7",
          name: "cancelled query consumer",
          execution_count: 1,
        },
      ]);
      // Aborting a future cannot roll back an already-sent side effect. Cleanup still awaits I/O.
    }, 10_000);

    const publish = (mode: string) =>
      fixture.consume(
        fixture.server.fetch(`/postgres/publish/${mode}`),
        `publishing ${mode} and closing the backend`,
      );
    const row = (
      id: number,
      unit = false,
      available: string | null = null,
    ) => ({
      task_id: String(id),
      task_name: unit ? "publishing_contract_unit" : "publishing_contract",
      task_unique_key: null,
      payload_json: unit ? "null" : JSON.stringify(['hello "🦀"\n', [1, 2, 3]]),
      callback_id: null,
      lease_worker_id: null,
      available_from_unix_ms: available,
    });

    for (const mode of ["immediate", "future"]) {
      test(`commits ${mode} callback-bearing and unit tasks without dispatch`, async () => {
        const response = await publish(mode);
        expect(response.status, response.body).toBe(200);
        const receipt = JSON.parse(response.body);
        expect(receipt.closed).toBe(true);
        expect(receipt.taskId).toBe(1);
        expect(receipt.unitTaskId).toBe(2);
        const state = await fixture.state();
        expect(state.processed).toEqual([]);
        expect(state.tasks).toHaveLength(2);
        for (const [index, task] of state.tasks.entries()) {
          const available = task.available_from_unix_ms;
          if (mode === "future") {
            // Public Rust deadlines are monotonic instants, converted to wall-clock milliseconds.
            expect(Number(available)).toBeGreaterThanOrEqual(
              receipt.beforeMs + 60_000 - 1,
            );
            expect(Number(available)).toBeLessThanOrEqual(
              receipt.afterMs + 60_000 + 1,
            );
          } else {
            expect(available).toBeNull();
          }
          expect(task).toEqual(row(index + 1, index === 1, available));
        }
        await fixture.waitForIdle();
      }, 10_000);
    }

    test("awaits shutdown after a SQL error without retrying", async () => {
      await fixture.admin.query(
        `ALTER TABLE ${fixture.table} ADD CONSTRAINT reject_insert CHECK (false)`,
      );
      expect(await publish("immediate")).toEqual({
        status: 500,
        body: '{"closed":true,"failed":true}',
      });
      expect(await fixture.state()).toEqual({ tasks: [], processed: [] });
      expect(
        (
          await fixture.admin.query(
            `SELECT last_value FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
          )
        ).rows,
      ).toEqual([{ last_value: "1" }]);
      await fixture.waitForIdle();
    }, 10_000);

    for (const mode of ["gated", "cancelled"]) {
      test(`drains a SQL-gated insert and closes its sole request connection: ${mode}`, async () => {
        const gate = await fixture.gate("bellows_tasks");
        let ended = false;
        const response = publish(mode).finally(() => {
          ended = true;
        });
        const pids = await gate.blocked(1);
        expect(ended).toBe(false);
        expect(
          (await fixture.activeRequestClients()).map(({ pid }) => pid),
        ).toEqual(pids);
        expect(await fixture.state()).toEqual({ tasks: [], processed: [] });
        await gate.release();
        const result = await response;
        expect(result.status, result.body).toBe(200);
        expect(JSON.parse(result.body)).toMatchObject(
          mode === "cancelled"
            ? { cancelled: true, closed: true }
            : { taskId: 1, closed: true },
        );
        await fixture.waitForClientExit(pids);
        // Cancelling the query consumer does not roll back an already-sent autocommit insert.
        expect(await fixture.state()).toEqual({
          tasks: [row(1)],
          processed: [],
        });
      }, 10_000);
    }
  });

  describe.sequential("Rust workerd platform and SDK contracts", () => {
    let server: TestHarness;
    let started: boolean;
    let expectedLogs: Array<{ level: string; message: string }>;
    const outstanding = new Set<Promise<unknown>>();

    const consume = async (
      path: string,
      init?: Parameters<TestHarness["fetch"]>[1],
    ) => {
      const response = server.fetch(path, init).then(async (response) => ({
        status: response.status,
        body: await response.text(),
      }));
      outstanding.add(response);
      void response.then(
        () => outstanding.delete(response),
        () => outstanding.delete(response),
      );
      return await deadline(
        response,
        `Rust contract ${path} including its body`,
        2_000,
      );
    };
    const state = async () =>
      JSON.parse((await consume("/source/state")).body) as {
        fetched: number;
        drained: number;
      };
    const dispatch = async (taskId: string) => {
      const response = await consume("/dispatch", json({ taskId }));
      expect(response.status, response.body).toBe(200);
      return JSON.parse(response.body) as {
        ok: boolean;
        taskId: string;
        duplicate?: boolean;
      };
    };

    beforeEach(async () => {
      expectedLogs = [];
      started = false;
      server = createTestHarness({
        workers: [
          {
            configPath,
            prebuiltWorkerDir: new URL("./build/harness/", configPath),
          },
        ],
      });
      await deadline(server.listen(), "starting Rust contracts project", 8_000);
      started = true;
    }, 10_000);

    afterEach(async ({ task }) => {
      try {
        if (task.result?.state === "fail") server.debug();
        try {
          if (started) {
            await consume("/source/drain");
            await deadline(
              Promise.allSettled([...outstanding]),
              "drain contract responses",
              3_000,
            );
            await poll(
              "all contract response streams drained",
              state,
              (s) => s.fetched === s.drained,
            );
          }
        } finally {
          await deadline(server.close(), "close Rust contract harness", 5_000);
        }
        expect(
          server.getLogs().map(({ level, message }) => ({ level, message })),
        ).toEqual(expectedLogs);
      } catch (error) {
        server.debug();
        throw error;
      }
    }, 10_000);

    for (const mode of [
      "finish",
      "finish-running",
      "fail",
      "lost",
      "error",
      "lost-completed",
      "error-completed",
      "no-claim",
    ]) {
      test(`runs Rust timers/spawn/ownership/finalization: ${mode}`, async () => {
        const response = await consume(`/runtime/${mode}`);
        expect(response.status, response.body).toBe(200);
        expect(JSON.parse(response.body)).toEqual({
          ok: true,
          mode,
          builds: mode === "no-claim" ? 0 : 1,
          renewals: ["finish", "finish-running", "fail"].includes(mode) ? 1 : 0,
          finishes: mode.startsWith("finish") ? 1 : 0,
          failures: mode === "fail" ? 1 : 0,
        });
      }, 10_000);
    }

    for (const taskId of ["ok", "error"]) {
      test(`awaits the complete namespace dispatch response body: ${taskId}`, async () => {
        let ended = false;
        const response = consume(`/dispatch-task/${taskId}`).finally(() => {
          ended = true;
        });
        await poll(
          "namespace response started but body withheld",
          state,
          (s) => s.fetched === 1,
        );
        expect(await state()).toEqual({ fetched: 1, drained: 0 });
        expect(ended).toBe(false);
        await consume("/source/release");
        const result = await response;
        expect(result.status, result.body).toBe(200);
        expect(JSON.parse(result.body)).toEqual(
          taskId === "ok"
            ? { ok: true }
            : {
                error: `task dispatcher returned HTTP 503: ${"a".repeat(499)}`,
              },
        );
        expect(await state()).toEqual({ fetched: 1, drained: 1 });
      }, 10_000);

      test(`retains service fetch through body drainage and releases the ID: ${taskId}`, async () => {
        expect(await dispatch(taskId)).toEqual({ ok: true, taskId });
        await poll(
          "service response started but body withheld",
          state,
          (s) => s.fetched === 1,
        );
        expect(await dispatch(taskId)).toEqual({
          ok: true,
          taskId,
          duplicate: true,
        });
        expect(await state()).toEqual({ fetched: 1, drained: 0 });
        await consume("/source/release");
        await poll(
          "full service body consumption",
          state,
          (s) => s.drained === 1,
        );
        if (taskId === "error") {
          expectedLogs.push({
            level: "error",
            message: `task processor failed error task processor returned HTTP 503: ${"a".repeat(466)}`,
          });
          await poll(
            "observed processor HTTP error",
            () => server.getLogs(),
            (logs) => logs.length === 1,
          );
        }
        await poll(
          "previous ID released",
          () => dispatch(taskId),
          (s) => s.duplicate !== true,
        );
        await poll(
          "new service attempt launched",
          state,
          (s) => s.fetched === 2,
        );
        await consume("/source/release");
        await poll("second body fully consumed", state, (s) => s.drained === 2);
        if (taskId === "error") {
          expectedLogs.push({ ...expectedLogs[0] });
          await poll(
            "second HTTP error observed",
            () => server.getLogs(),
            (logs) => logs.length === 2,
          );
        }
      }, 10_000);
    }

    test("adapts heartbeat alarms as absolute times and schedules while idle", async () => {
      const alarmState = async () =>
        JSON.parse((await consume("/dispatcher/state")).body) as {
          alarm: number | null;
          now: number;
        };
      const before = await alarmState();
      expect(before.alarm).toBeNull();
      expect((await consume("/dispatcher/alarm")).status).toBe(200);
      const after = await alarmState();
      expect(after.alarm).toBeGreaterThanOrEqual(before.now + 30_000);
      expect(after.alarm).toBeLessThanOrEqual(after.now + 30_000);
      expect(await dispatch("ok")).toEqual({ ok: true, taskId: "ok" });
      expect((await alarmState()).alarm).toBe(after.alarm);
      await consume("/source/release");
      await poll(
        "processor drained before idle alarm",
        state,
        (s) => s.drained === 1,
      );
      const idle = await alarmState();
      await consume("/dispatcher/alarm");
      const rescheduled = await alarmState();
      expect(rescheduled.alarm).toBeGreaterThanOrEqual(idle.now + 30_000);
      expect(rescheduled.alarm).toBeLessThanOrEqual(rescheduled.now + 30_000);
    }, 10_000);
  });
}
