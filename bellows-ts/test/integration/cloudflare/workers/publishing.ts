import { PostgresPublishingBackend } from "../../../../src/backends/postgres-publishing.js";
import { definePublishTask } from "../../../../src/index.js";

interface Env {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
}

const task = definePublishTask<[string, number[]], string[]>(
  "publishing_contract",
);
const unitTask = definePublishTask<void>("publishing_contract_unit");

// Test-only request controls, not an application producer or a public dispatch protocol.
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const mode = new URL(request.url).pathname.replace(
      "/postgres/publish/",
      "",
    );
    if (!["immediate", "future", "gated"].includes(mode)) {
      return new Response("not-found", { status: 404 });
    }
    const backend = await PostgresPublishingBackend.connect(
      env.HYPERDRIVE.connectionString,
      { schema: env.BELLOWS_SCHEMA },
    );
    try {
      const payload: [string, number[]] = ['hello "🦀"\n', [1, 2, 3]];
      const beforeMs = Date.now();
      const available = beforeMs + 60_000;
      // Promises cannot be cancelled: retain and await the insert before closing the pool.
      const publishing =
        mode === "future"
          ? backend.publishFuture(task, payload, available)
          : backend.publish(task, payload);
      const receipt = await publishing;
      const unit =
        mode === "immediate"
          ? await backend.publish(unitTask, undefined)
          : mode === "future"
            ? await backend.publishFuture(unitTask, undefined, available)
            : undefined;
      return Response.json({
        taskId: receipt.taskId,
        unitTaskId: unit?.taskId ?? null,
        beforeMs,
        afterMs: Date.now(),
        closed: true,
      });
    } catch {
      return Response.json({ failed: true, closed: true }, { status: 500 });
    } finally {
      // The response is not returned until shutdown finishes, including on SQL errors.
      await backend.close();
    }
  },
} satisfies ExportedHandler<Env>;
