import { randomInt } from "node:crypto";
import { Client } from "pg";
import { PostgresExecutionBackend } from "../../../../src/backends/postgres-execution.js";
import { validatePostgresSchemaName } from "../../../../src/backends/postgres-operations.js";
import {
  runTaskOnce,
  TaskSuccess,
  type WorkerFactory,
} from "../../../../src/index.js";
import { greetingTask } from "../task.js";

interface ProcessorEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
}

export default {
  async fetch(request, env): Promise<Response> {
    if (new URL(request.url).pathname !== "/process") {
      return Response.json({ error: "not-found" }, { status: 404 });
    }
    if (request.method !== "POST") {
      return Response.json(
        { error: "method-not-allowed" },
        { status: 405, headers: { allow: "POST" } },
      );
    }
    if (
      !request.headers
        .get("content-type")
        ?.toLowerCase()
        .includes("application/json")
    ) {
      return Response.json(
        { error: "content-type must be application/json" },
        { status: 415 },
      );
    }

    let body: unknown;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "invalid JSON" }, { status: 400 });
    }
    if (
      body === null ||
      typeof body !== "object" ||
      Array.isArray(body) ||
      !("taskId" in body) ||
      typeof body.taskId !== "string" ||
      !/^[1-9][0-9]{0,15}$/.test(body.taskId)
    ) {
      return Response.json(
        { error: "taskId must be a canonical positive decimal string" },
        { status: 400 },
      );
    }
    const taskId = Number(body.taskId);
    if (!Number.isSafeInteger(taskId) || String(taskId) !== body.taskId) {
      return Response.json(
        { error: "taskId must encode a positive safe integer canonically" },
        { status: 400 },
      );
    }

    try {
      const schema = validatePostgresSchemaName(env.BELLOWS_SCHEMA);
      const backend = await PostgresExecutionBackend.connect(
        env.HYPERDRIVE.connectionString,
        { schema },
      );
      try {
        const factory: WorkerFactory<typeof greetingTask> = {
          task: greetingTask,
          build() {
            return {
              async process(claimedTaskId, payload) {
                // Only a successful claim reaches this worker and opens this client.
                const client = new Client({
                  connectionString: env.HYPERDRIVE.connectionString,
                });
                try {
                  await client.connect();
                  await client.query(
                    `
INSERT INTO "${schema}".processed_tasks AS processed (task_id, name, execution_count)
VALUES ($1, $2, 1)
ON CONFLICT (task_id) DO UPDATE
SET name = EXCLUDED.name, execution_count = processed.execution_count + 1
                    `,
                    [claimedTaskId, payload.name],
                  );
                  return TaskSuccess.done(undefined);
                } finally {
                  await client.end();
                }
              },
            };
          },
        };
        // randomInt's range must be smaller than 2^48; all these IDs are safe integers.
        const workerId = randomInt(1, 2 ** 48);
        await runTaskOnce(backend, factory, workerId, { type: "task", taskId });
      } finally {
        await backend.close();
      }

      // runTaskOnce also resolves for no-claim and handled-failure cases.
      // Database state, not this attempt response, establishes completion.
      return Response.json({ taskId: body.taskId, attemptFinished: true });
    } catch {
      return Response.json(
        { error: "task processing attempt failed" },
        { status: 500 },
      );
    }
  },
} satisfies ExportedHandler<ProcessorEnv>;
