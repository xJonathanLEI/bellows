import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { type CloudflarePostgresFixture, poll } from "./postgres-fixture.js";

const json = (body: unknown) => ({
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(body),
});

// Registered once by each language-owned suite, never by the mixed topologies.
export function publisherContracts(
  name: string,
  createFixture: () => CloudflarePostgresFixture,
): void {
  describe.sequential(name, () => {
    let fixture: CloudflarePostgresFixture;
    let started: boolean;

    const consume = (path: string, body?: unknown) =>
      fixture.consume(
        fixture.server.fetch(path, json(body)),
        `publisher contract ${path} including its body`,
      );
    const state = async () => {
      const response = await consume("/publisher/state");
      expect(response.status, response.body).toBe(200);
      return JSON.parse(response.body) as {
        dispatches: Array<{ taskId: string; taskName: string }>;
        drained: number;
      };
    };
    const control = async (path: string, body?: unknown) => {
      const response = await consume(path, body);
      expect(response.status, response.body).toBe(200);
    };
    const sequence = async () =>
      (
        await fixture.admin.query(
          `SELECT last_value, is_called FROM "${fixture.schema}".bellows_tasks_task_id_seq`,
        )
      ).rows;

    beforeEach(async () => {
      started = false;
      fixture = createFixture();
      await fixture.start();
      started = true;
    }, 10_000);

    afterEach(async ({ task }) => {
      try {
        try {
          if (task.result?.state === "fail") await fixture.debug();
        } finally {
          try {
            // Release streams even after an assertion/response deadline, before fixture drainage.
            if (started) await control("/publisher/drain");
          } finally {
            await fixture.close();
          }
        }
        expect(fixture.server.getLogs()).toEqual([]);
      } catch (error) {
        await fixture.debug();
        throw error;
      }
    }, 10_000);

    for (const status of [200, 503]) {
      const title =
        status === 200
          ? "closes before dispatch and consumes the complete gated success body"
          : "retains a dispatch failure through body consumption and redispatches without republishing";
      test(title, async () => {
        await control("/publisher/response", { status });
        const gate = await fixture.gate("bellows_tasks");
        let ended = false;
        const response = consume("/publisher/publish").finally(() => {
          ended = true;
        });
        const pids = await gate.blocked(1);
        expect(
          (await fixture.activeRequestClients()).map(({ pid }) => pid),
        ).toEqual(pids);
        expect(await fixture.state()).toEqual({ tasks: [], processed: [] });
        expect(await state()).toEqual({ dispatches: [], drained: 0 });
        expect(ended).toBe(false);

        await gate.release();
        await poll(
          "dispatch headers and prefix with the response tail withheld",
          state,
          (value) => value.dispatches.length === 1,
        );
        const receipt = { taskId: "1" };
        const dispatch = { ...receipt, taskName: "publisher_contract" };
        expect(await state()).toEqual({
          dispatches: [dispatch],
          drained: 0,
        });
        await fixture.waitForClientExit(pids);
        expect(await fixture.activeRequestClients()).toHaveLength(0);
        const committed = {
          tasks: [
            {
              task_id: "1",
              task_name: "publisher_contract",
              task_unique_key: null,
              payload_json: JSON.stringify(['hello "🦀"\n', [1, 2, 3]]),
              callback_id: null,
              lease_worker_id: null,
              available_from_unix_ms: null,
            },
          ],
          processed: [],
        };
        expect(await fixture.state()).toEqual(committed);
        expect(await sequence()).toEqual([
          { last_value: "1", is_called: true },
        ]);
        expect(ended).toBe(false);

        await control("/publisher/release");
        const result = await response;
        expect(result.status, result.body).toBe(status);
        expect(JSON.parse(result.body)).toEqual(
          status === 200
            ? receipt
            : {
                stage: "dispatch",
                receipt,
                error: "PostgreSQL publisher failed at dispatch",
              },
        );
        expect(result.body).not.toContain("fixture-secret");
        expect(await state()).toEqual({ dispatches: [dispatch], drained: 1 });
        expect(await fixture.state()).toEqual(committed);

        if (status === 503) {
          await control("/publisher/response", { status: 200 });
          // Recover from the typed error's receipt, not a fresh publication.
          const retained = JSON.parse(result.body).receipt;
          ended = false;
          const redispatched = consume(
            "/publisher/redispatch",
            retained,
          ).finally(() => {
            ended = true;
          });
          await poll(
            "explicit redispatch of the retained ID",
            state,
            (value) => value.dispatches.length === 2,
          );
          expect(await state()).toEqual({
            dispatches: [dispatch, dispatch],
            drained: 1,
          });
          expect(await fixture.activeRequestClients()).toHaveLength(0);
          expect(ended).toBe(false);
          await control("/publisher/release");
          const recovered = await redispatched;
          expect(recovered.status, recovered.body).toBe(200);
          expect(JSON.parse(recovered.body)).toEqual(receipt);
          expect(await state()).toEqual({
            dispatches: [dispatch, dispatch],
            drained: 2,
          });
        }
        expect(await fixture.state()).toEqual(committed);
        expect(await sequence()).toEqual([
          { last_value: "1", is_called: true },
        ]);
        await fixture.waitForIdle();
      }, 10_000);
    }
  });
}
