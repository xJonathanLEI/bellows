import type { CloudflarePostgresFixture } from "bellows-cloudflare-interop-tests/cloudflare/postgres-fixture";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { createCloudflarePostgresFixture } from "./postgres-fixture.js";

export function publishingContracts(): void {
  describe.sequential("TypeScript workerd PostgreSQL publishing", () => {
    let fixture: CloudflarePostgresFixture;
    beforeEach(async () => {
      fixture = createCloudflarePostgresFixture({
        producer: {
          configPath: new URL("./wrangler.publishing.jsonc", import.meta.url),
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
      test(`commits ${mode} callback-bearing and void tasks without dispatch`, async () => {
        const response = await publish(mode);
        expect(response.status, response.body).toBe(200);
        const receipt = JSON.parse(response.body);
        expect(receipt.closed).toBe(true);
        expect(receipt.taskId).toBe(1);
        expect(receipt.unitTaskId).toBe(2);
        const available =
          mode === "future" ? String(receipt.beforeMs + 60_000) : null;
        expect(await fixture.state()).toEqual({
          tasks: [
            row(receipt.taskId, false, available),
            row(receipt.unitTaskId, true, available),
          ],
          processed: [],
        });
        await fixture.waitForIdle();
      }, 10_000);
    }

    test("awaits shutdown after a SQL error without retrying", async () => {
      await fixture.admin.query(
        `ALTER TABLE ${fixture.table} ADD CONSTRAINT reject_insert CHECK (false)`,
      );
      expect(await publish("immediate")).toEqual({
        status: 500,
        body: '{"failed":true,"closed":true}',
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

    test("awaits a SQL-gated insert and closes its sole request connection", async () => {
      const gate = await fixture.gate("bellows_tasks");
      let ended = false;
      const response = publish("gated").finally(() => {
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
      expect(JSON.parse(result.body)).toMatchObject({
        taskId: 1,
        closed: true,
      });
      await fixture.waitForClientExit(pids);
      expect(await fixture.state()).toEqual({ tasks: [row(1)], processed: [] });
    }, 10_000);
  });
}
