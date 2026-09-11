import { DurableObject } from "cloudflare:workers";
import {
  createPostgresPublisher,
  PostgresPublisherError,
  type PostgresPublisherReceipt,
} from "../../../../src/cloudflare/postgres.js";
import { RetainedTaskDispatcher } from "../../../../src/cloudflare.js";
import { fullNameTask, greetingTask } from "../task.js";

interface ProducerEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
  DISPATCHER: DurableObjectNamespace<TaskDispatcher>;
  PROCESSOR: Fetcher;
}

const publisherConfig = (env: ProducerEnv) => ({
  connectionString: env.HYPERDRIVE.connectionString,
  schema: env.BELLOWS_SCHEMA,
  dispatcher: env.DISPATCHER,
});
const greetingPublisher = createPostgresPublisher((env: ProducerEnv) => ({
  ...publisherConfig(env),
  task: greetingTask,
}));
const fullNamePublisher = createPostgresPublisher((env: ProducerEnv) => ({
  ...publisherConfig(env),
  task: fullNameTask,
}));

function validName(value: unknown): value is string {
  return (
    typeof value === "string" && value.trim().length > 0 && value.length <= 200
  );
}

export default {
  async fetch(request, env): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (path !== "/tasks" && path !== "/full-names") {
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
    try {
      let receipt: PostgresPublisherReceipt;
      if (path === "/tasks") {
        if (
          body === null ||
          typeof body !== "object" ||
          Array.isArray(body) ||
          !("name" in body) ||
          !validName(body.name)
        ) {
          return Response.json(
            {
              error:
                "body must be an object with a non-blank name of at most 200 characters",
            },
            { status: 400 },
          );
        }
        receipt = await greetingPublisher.publish(env, { name: body.name });
      } else {
        if (
          body === null ||
          typeof body !== "object" ||
          Array.isArray(body) ||
          !("firstName" in body) ||
          !validName(body.firstName) ||
          !("lastName" in body) ||
          !validName(body.lastName)
        ) {
          return Response.json(
            {
              error:
                "body must be an object with non-blank firstName and lastName of at most 200 characters each",
            },
            { status: 400 },
          );
        }
        receipt = await fullNamePublisher.publish(env, {
          firstName: body.firstName,
          lastName: body.lastName,
        });
      }
      // Acceptance is not completion; the processor may still be running.
      return new Response(receipt.taskId, {
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
