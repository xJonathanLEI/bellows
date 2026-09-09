import { DurableObject } from "cloudflare:workers";
import {
  createPostgresPublisher,
  PostgresPublisherError,
} from "../../../../src/cloudflare/postgres.js";
import { RetainedTaskDispatcher } from "../../../../src/cloudflare.js";
import { greetingTask } from "../task.js";

interface ProducerEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
  DISPATCHER: DurableObjectNamespace<TaskDispatcher>;
  PROCESSOR: Fetcher;
}

const publisher = createPostgresPublisher((env: ProducerEnv) => ({
  connectionString: env.HYPERDRIVE.connectionString,
  schema: env.BELLOWS_SCHEMA,
  task: greetingTask,
  dispatcher: env.DISPATCHER,
}));

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

    try {
      const { taskId } = await publisher.publish(env, {
        name: body.name,
      });
      // Acceptance is not completion; the processor may still be running.
      return new Response(taskId, {
        status: 202,
        headers: {
          "content-type": "text/plain; charset=utf-8",
          "cache-control": "no-store",
        },
      });
    } catch (error) {
      const taskId =
        error instanceof PostgresPublisherError
          ? error.receipt?.taskId
          : undefined;
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
