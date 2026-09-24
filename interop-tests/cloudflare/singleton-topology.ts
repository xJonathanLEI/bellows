import { afterEach, describe, expect, test } from "vitest";
import {
  type CloudflarePostgresFixture,
  type CloudflareProjects,
  poll,
} from "./postgres-fixture.js";

const configured = "cloudflare_singleton";
const unconfigured = " singleton:7 🦀 ";
const identity = (taskName = configured) => ({ kind: "singleton", taskName });
const json = (body: unknown) => ({
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

export function singletonTopology(
  createFixture: (
    singleton?: CloudflareProjects["singleton"],
  ) => CloudflarePostgresFixture,
): void {
  describe("singletons", () => {
    let fixture: CloudflarePostgresFixture | undefined;

    function current(): CloudflarePostgresFixture {
      if (!fixture) throw new Error("singleton fixture has not started");
      return fixture;
    }

    async function start(
      mode: NonNullable<CloudflareProjects["singleton"]>["mode"] = "park",
      bootstrap = true,
    ) {
      fixture = createFixture({ bootstrap, mode });
      await fixture.start();
      return fixture;
    }

    async function control(path: "clear" | "reconstruct") {
      const dispatcher = await current().dispatcher();
      const response = await current().consume(
        dispatcher.fetch(`https://dispatcher/__test/${path}`),
        `singleton fixture ${path}`,
      );
      expect(response.status, response.body).toBe(200);
    }

    async function attempts() {
      const response = await current().consume(
        current().processor.fetch("/__test/attempts"),
        "inspect completed singleton processor invocations",
      );
      expect(response.status, response.body).toBe(200);
      return JSON.parse(response.body) as Record<string, number>;
    }

    async function pending(name = configured, atMs?: number) {
      const key = `task:singleton:${name}`;
      const schedule = await poll(
        "one name-based singleton schedule",
        () => current().schedule(),
        ({ tasks }) => {
          const record = tasks[key];
          return (
            record?.state.type === "pending" &&
            (atMs === undefined ||
              (record.nextAttemptAtMs >= atMs &&
                record.nextAttemptAtMs <= atMs + 1))
          );
        },
      );
      expect(schedule.tasks[key].task).toEqual(identity(name));
      return schedule.tasks[key];
    }

    async function settled(count: number, name = configured) {
      const state = await poll(
        "singleton effects and committed future availability",
        () => current().state(),
        ({ tasks, processed }) =>
          processed.find((row) => row.name === name)?.execution_count ===
            count &&
          tasks.some(
            (row) =>
              row.task_name === name &&
              row.lease_worker_id === null &&
              Number(row.available_from_unix_ms) > Date.now(),
          ),
      );
      const rows = state.tasks.filter((row) => row.task_name === name);
      expect(rows).toHaveLength(1);
      const row = rows[0];
      expect(row).toMatchObject({
        task_unique_key: name,
        payload_json: "null",
        callback_id: null,
      });
      expect(
        state.processed.find((effect) => effect.name === name)?.task_id,
      ).toBe(row.task_id);
      await pending(name, Number(row.available_from_unix_ms));
      await current().waitForIdle();
      return row;
    }

    async function seed(
      name: string,
      atMs: number | null = null,
      owner: number | null = null,
    ) {
      const result = await current().admin.query<{ task_id: string }>(
        `INSERT INTO ${current().table}
         (task_name, task_unique_key, payload_json, available_from_unix_ms, lease_worker_id)
         VALUES ($1, $1, 'null', $2, $3) RETURNING task_id`,
        [name, atMs, owner],
      );
      return result.rows[0].task_id;
    }

    async function due(atMs: number) {
      await poll(
        "database singleton deadline",
        async () =>
          (
            await current().admin.query<{ due: boolean }>(
              "SELECT floor(extract(epoch FROM statement_timestamp()) * 1000) >= $1 AS due",
              [atMs],
            )
          ).rows[0].due,
        Boolean,
      );
    }

    afterEach(async ({ task }) => {
      if (!fixture) return;
      const current = fixture;
      try {
        if (task.result?.state === "fail") await current.debug();
      } finally {
        try {
          // All successful scenarios park subsequent runs; remove alarms before stopping workerd.
          await control("clear");
        } finally {
          await current.close();
          fixture = undefined;
        }
      }
      expect(
        current.server
          .getLogs()
          .filter(({ level }) => level === "error" || level === "warn"),
      ).toEqual([]);
    }, 10_000);

    for (const mode of ["success", "failure", "immediate", "done"] as const) {
      test(`scheduled bootstrap and automatic ${mode} recurrence retain one row and identity`, async () => {
        const f = await start(mode);
        expect(await f.state()).toEqual({ tasks: [], processed: [] });
        const gate = await f.gate();
        // Overlapping events must not create two rows or simultaneous business owners.
        await Promise.all([f.runScheduled(), f.runScheduled()]);
        await gate.blocked(1);
        const claimed = (await f.state()).tasks;
        expect(claimed).toHaveLength(1);
        const row = claimed[0];
        expect(row.task_unique_key).toBe(configured);
        expect(row.lease_worker_id).not.toBeNull();
        expect((await f.schedule()).tasks).toEqual({});
        const competing = await f.consume(
          f.processor.fetch("/process", json({ task: identity() })),
          "competing singleton claim while the valid owner is gated",
        );
        expect(competing.status, competing.body).toBe(200);
        const body = JSON.parse(competing.body);
        expect(body.task).toEqual(identity());
        expect(body.nextAction.type).toBe("retryAt");
        expect(body.nextAction.atMs).toBeGreaterThanOrEqual(
          Number(row.available_from_unix_ms),
        );
        expect(body.nextAction.atMs).toBeLessThanOrEqual(
          Number(row.available_from_unix_ms) + 1,
        );
        await gate.blocked(1);
        await gate.release();
        let firstDeadline: number | undefined;
        if (mode === "success" || mode === "failure") {
          firstDeadline = Number((await settled(1)).available_from_unix_ms);
          expect(Date.now()).toBeLessThan(firstDeadline);
        }
        const final = await settled(2);
        expect(final.task_id).toBe(row.task_id);
        expect(Object.keys((await f.schedule()).tasks)).toEqual([
          `task:singleton:${configured}`,
        ]);
        const executions = await f.executions(row.task_id);
        expect(executions).toHaveLength(2);
        if (firstDeadline !== undefined) {
          expect(Number(executions[0].executed_at_ms)).toBeLessThan(
            firstDeadline,
          );
          expect(Number(executions[1].executed_at_ms)).toBeGreaterThanOrEqual(
            firstDeadline,
          );
        }
        // These are real scheduled events, not a sweeper HTTP route or a minute wait.
        const before = await attempts();
        expect(before[configured]).toBe(3); // owner, occupied competitor, alarm
        await Promise.all([f.runScheduled(), f.runScheduled()]);
        expect(await attempts()).toEqual(before);
        expect((await f.state()).tasks).toEqual([final]);
        const schedule = await f.schedule();
        expect(schedule.alarm).toBeGreaterThan(schedule.now);
        expect(schedule.alarm).toBeLessThanOrEqual(schedule.now + 30_000);
        expect(schedule.alarm).toBeLessThan(
          Number(final.available_from_unix_ms),
        );
      }, 10_000);
    }

    test("warm bootstrap suppression never blocks recovery after fixture schedule loss", async () => {
      const f = await start();
      await f.runScheduled();
      const first = await settled(1);
      expect(await attempts()).toEqual({ [configured]: 1 });
      await control("clear"); // Keep the same delegate and its bootstrap set.
      await f.runScheduled();
      expect(await attempts()).toEqual({ [configured]: 1 });
      expect((await f.schedule()).tasks).toEqual({});
      await f.admin.query(
        `UPDATE ${f.table} SET available_from_unix_ms = NULL WHERE task_id = $1`,
        [first.task_id],
      );
      const gate = await f.gate();
      await Promise.all([f.runScheduled(), f.runScheduled()]);
      await gate.blocked(1);
      await gate.release();
      const recovered = await settled(2);
      expect(recovered.task_id).toBe(first.task_id);
      expect(await attempts()).toEqual({ [configured]: 2 });
      await f.runScheduled();
      expect(await attempts()).toEqual({ [configured]: 2 });
    }, 10_000);

    for (const occupied of [false, true]) {
      test(`reconstruction rechecks a ${occupied ? "leased" : "future"} singleton without early execution`, async () => {
        const f = await start();
        const atMs = Date.now() + 60_000;
        const id = await seed(configured, atMs, occupied ? 123 : null);
        await f.runScheduled();
        await pending(configured, atMs);
        await f.waitForIdle();
        const before = await f.state();
        expect(await attempts()).toEqual({ [configured]: 1 });
        await f.runScheduled();
        expect(await attempts()).toEqual({ [configured]: 1 });
        const schedule = await f.schedule();
        await control("reconstruct"); // Delegate reconstruction, not platform eviction.
        expect((await f.schedule()).tasks).toEqual(schedule.tasks);
        await f.runScheduled();
        await poll(
          "reconstructed bootstrap recheck",
          attempts,
          (value) => value[configured] === 2,
        );
        await pending(configured, atMs);
        await f.waitForIdle();
        expect(await f.state()).toEqual(before);
        expect(await f.executions(id)).toEqual([]);
        expect(Object.keys((await f.schedule()).tasks)).toEqual([
          `task:singleton:${configured}`,
        ]);
        await f.runScheduled();
        expect(await attempts()).toEqual({ [configured]: 2 });
      }, 10_000);
    }

    test("alarms follow extended singleton leases and automatically claim after expiry", async () => {
      const f = await start();
      const firstExpiration = Date.now() + 1_000;
      const secondExpiration = firstExpiration + 800;
      const id = await seed(configured, firstExpiration, 123);
      const gate = await f.gate();
      await f.runScheduled();
      await pending(configured, firstExpiration);
      await f.admin.query(
        `UPDATE ${f.table} SET available_from_unix_ms = $1 WHERE task_id = $2`,
        [secondExpiration, id],
      );
      await pending(configured, secondExpiration);
      expect(Date.now()).toBeLessThan(secondExpiration);
      expect(await f.executions(id)).toEqual([]);
      expect((await f.state()).tasks[0].lease_worker_id).toBe("123");
      await gate.blocked(1);
      const owner = (await f.state()).tasks[0].lease_worker_id;
      expect(owner).not.toBeNull();
      expect(owner).not.toBe("123");
      await gate.release();
      expect((await settled(1)).task_id).toBe(id);
      expect(
        Number((await f.executions(id))[0].executed_at_ms),
      ).toBeGreaterThanOrEqual(secondExpiration);
      expect(await attempts()).toEqual({ [configured]: 3 });
    }, 10_000);

    for (const mode of ["success", "failure"] as const) {
      test(`Cron recovers a committed singleton ${mode} hint that never reached the DO`, async () => {
        const f = await start(mode, false);
        await f.consume(
          f.processor.fetch(
            "/__test/lose-response",
            json({ taskName: configured }),
          ),
          "arm singleton response loss",
        );
        const lost = await f.consume(
          f.processor.fetch("/process", json({ task: identity() })),
          "consume lost singleton scheduling response",
        );
        expect(lost).toEqual({ status: 503, body: "fixture response lost" });
        await f.waitForIdle();
        const state = await f.state();
        expect(state.tasks).toHaveLength(1);
        const row = state.tasks[0];
        const atMs = Number(row.available_from_unix_ms);
        expect(row.lease_worker_id).toBeNull();
        expect(state.processed[0].execution_count).toBe(1);
        await f.runScheduled();
        expect(Date.now()).toBeLessThan(atMs);
        expect((await f.schedule()).tasks).toEqual({});
        expect((await f.schedule()).alarm).toBeNull();
        await due(atMs);
        await f.runScheduled();
        expect((await settled(2)).task_id).toBe(row.task_id);
        expect(
          Number((await f.executions(row.task_id))[1].executed_at_ms),
        ).toBeGreaterThanOrEqual(atMs);
        expect(await attempts()).toEqual({ [configured]: 2 });
      }, 10_000);
    }

    test("mixed discovery spans pages and launches unconfigured singletons plus bootstrap without a cap", async () => {
      const f = await start();
      const id = await seed(unconfigured);
      await f.admin.query(
        `INSERT INTO ${f.table} (task_name, payload_json)
         SELECT 'cloudflare_greeting', json_build_object('name', 'Mixed ' || n)::text
         FROM generate_series(1, 100) AS n`,
      );
      const claims = await f.gate("bellows_tasks");
      const business = await f.gate();
      await f.runScheduled();
      // Both pages and the final ensure batch enter processors before any claim completes.
      await claims.blocked(102);
      expect((await f.schedule()).tasks).toEqual({});
      await claims.release();
      await business.blocked(102);
      expect((await f.state()).tasks).toHaveLength(102);
      await business.release();
      expect((await settled(1, unconfigured)).task_id).toBe(id);
      await settled(1);
      const state = await poll(
        "all published work completes alongside retained singletons",
        () => f.state(),
        ({ tasks, processed }) =>
          tasks.length === 2 && processed.length === 102,
      );
      expect(state.processed.every((row) => row.execution_count === 1)).toBe(
        true,
      );
      expect(state.tasks.map((row) => row.task_unique_key).sort()).toEqual(
        [unconfigured, configured].sort(),
      );
      expect(Object.keys((await f.schedule()).tasks).sort()).toEqual(
        [unconfigured, configured]
          .map((name) => `task:singleton:${name}`)
          .sort(),
      );
      expect(await attempts()).toEqual({ [configured]: 1, [unconfigured]: 1 });
    }, 10_000);
  });
}
