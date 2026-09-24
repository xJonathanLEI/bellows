import { DurableObject } from "cloudflare:workers";
import {
  createPostgresPublisher,
  createPostgresSweeper,
  PostgresPublisherError,
  type PostgresPublisherReceipt,
} from "../../../../src/cloudflare/postgres.js";
import { RetainedTaskDispatcher } from "../../../../src/cloudflare.js";
import {
  fullNameTask,
  greetingTask,
  schedulingTask,
  singletonTask,
} from "../task.js";

interface ProducerEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
  BELLOWS_SINGLETON_BOOTSTRAP?: string;
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
const schedulingPublisher = createPostgresPublisher((env: ProducerEnv) => ({
  ...publisherConfig(env),
  task: schedulingTask,
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
  scheduled: createPostgresSweeper((env: ProducerEnv) => ({
    ...publisherConfig(env),
    singletons:
      env.BELLOWS_SINGLETON_BOOTSTRAP === "true" ? [singletonTask] : [],
  })).scheduled,
  async fetch(request, env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    if (path !== "/tasks" && path !== "/full-names" && path !== "/scheduled") {
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
      const at = url.searchParams.get("availableFromMs");
      const availableFromMs = at === null ? undefined : Number(at);
      if (
        (at !== null && (at.length === 0 || /[^0-9]/.test(at))) ||
        (availableFromMs !== undefined &&
          (!Number.isSafeInteger(availableFromMs) ||
            availableFromMs < 0 ||
            availableFromMs > 8_640_000_000_000_000))
      ) {
        return Response.json(
          { error: "invalid availability" },
          { status: 400 },
        );
      }
      if (path === "/scheduled") {
        if (
          body === null ||
          typeof body !== "object" ||
          !("name" in body) ||
          !validName(body.name) ||
          !("mode" in body) ||
          (body.mode !== "failure" &&
            body.mode !== "success" &&
            body.mode !== "immediate") ||
          !("availableFromMs" in body) ||
          typeof body.availableFromMs !== "number" ||
          !Number.isSafeInteger(body.availableFromMs) ||
          body.availableFromMs < 0 ||
          body.availableFromMs > 8_640_000_000_000_000
        ) {
          return Response.json(
            { error: "invalid scheduling payload" },
            { status: 400 },
          );
        }
        receipt = await schedulingPublisher.publish(env, {
          name: body.name,
          mode: body.mode,
          availableFromMs: body.availableFromMs,
        });
      } else if (path === "/tasks") {
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
        const payload = { name: body.name };
        receipt =
          availableFromMs === undefined
            ? await greetingPublisher.publish(env, payload)
            : await greetingPublisher.publishFuture(
                env,
                payload,
                availableFromMs,
              );
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
        const payload = {
          firstName: body.firstName,
          lastName: body.lastName,
        };
        receipt =
          availableFromMs === undefined
            ? await fullNamePublisher.publish(env, payload)
            : await fullNamePublisher.publishFuture(
                env,
                payload,
                availableFromMs,
              );
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
  private dispatcher: RetainedTaskDispatcher;

  constructor(ctx: DurableObjectState, env: ProducerEnv) {
    super(ctx, env);
    this.dispatcher = new RetainedTaskDispatcher(ctx.storage, env.PROCESSOR);
  }

  async fetch(request: Request): Promise<Response> {
    // Fixture-only inspection/reconstruction; these routes are not exposed by the producer HTTP API.
    switch (new URL(request.url).pathname) {
      case "/__test/clear":
        await this.ctx.storage.deleteAll();
        await this.ctx.storage.deleteAlarm();
        return new Response(null);
      case "/__test/reconstruct":
        this.dispatcher = new RetainedTaskDispatcher(
          this.ctx.storage,
          this.env.PROCESSOR,
        );
        return new Response(null);
      case "/__test/state":
        return Response.json({
          metadata: await this.ctx.storage.get("scheduler"),
          tasks: Object.fromEntries(
            await this.ctx.storage.list({ prefix: "task:" }),
          ),
          alarm: await this.ctx.storage.getAlarm(),
          now: Date.now(),
        });
    }
    return this.dispatcher.fetch(request);
  }

  alarm(): Promise<void> {
    return this.dispatcher.alarm();
  }
}
