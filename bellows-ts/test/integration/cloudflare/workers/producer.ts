import { DurableObject } from "cloudflare:workers";
import { Client } from "pg";
import { validatePostgresSchemaName } from "../../../../src/backends/postgres-operations.js";
import {
  dispatchTask,
  RetainedTaskDispatcher,
} from "../../../../src/cloudflare.js";
import type { TaskPayload } from "../../../../src/index.js";
import { greetingTask } from "../task.js";

interface ProducerEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
  DISPATCHER: DurableObjectNamespace<TaskDispatcher>;
  PROCESSOR: Fetcher;
}

// Example glue for an immediate, callback-free task, not a general publishing backend.
async function publishTask(
  client: Client,
  schema: string,
  payload: TaskPayload<typeof greetingTask>,
): Promise<string> {
  const table = `"${validatePostgresSchemaName(schema)}".bellows_tasks`;
  const result = await client.query<{ task_id: string }>(
    `
INSERT INTO ${table} (
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
    [greetingTask.name, greetingTask.codec.encode(payload)],
  );
  return result.rows[0].task_id;
}

export default {
  async fetch(request, env): Promise<Response> {
    if (new URL(request.url).pathname !== "/tasks") {
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
      !("name" in body) ||
      typeof body.name !== "string" ||
      body.name.trim().length === 0 ||
      body.name.length > 200
    ) {
      return Response.json(
        {
          error:
            "body must be an object with a non-blank name of at most 200 characters",
        },
        { status: 400 },
      );
    }

    let taskId: string | undefined;
    try {
      const client = new Client({
        connectionString: env.HYPERDRIVE.connectionString,
      });
      try {
        await client.connect();
        taskId = await publishTask(client, env.BELLOWS_SCHEMA, {
          name: body.name,
        });
      } finally {
        await client.end();
      }

      // The standalone INSERT is committed and its client closed before dispatch.
      await dispatchTask(env.DISPATCHER, taskId);
      // Acceptance is not completion; the processor may still be running.
      return new Response(taskId, {
        status: 202,
        headers: {
          "content-type": "text/plain; charset=utf-8",
          "cache-control": "no-store",
        },
      });
    } catch {
      return Response.json(
        taskId === undefined
          ? { error: "task publication failed" }
          : {
              error:
                "task published, but dispatch acceptance was not confirmed",
              taskId,
            },
        { status: 503 },
      );
    }
  },
} satisfies ExportedHandler<ProducerEnv>;

export class TaskDispatcher extends DurableObject<ProducerEnv> {
  private readonly dispatcher: RetainedTaskDispatcher;

  constructor(ctx: DurableObjectState, env: ProducerEnv) {
    super(ctx, env);
    this.dispatcher = new RetainedTaskDispatcher(ctx.storage, env.PROCESSOR);
  }

  fetch(request: Request): Promise<Response> {
    return this.dispatcher.fetch(request);
  }

  alarm(): Promise<void> {
    return this.dispatcher.alarm();
  }
}
