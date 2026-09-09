import { DurableObject } from "cloudflare:workers";
import {
  createPostgresPublisher,
  PostgresPublisherError,
} from "../../../../src/cloudflare/postgres.js";
import { dispatchTask } from "../../../../src/cloudflare.js";
import { definePublishTask } from "../../../../src/index.js";

interface Env {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
  DISPATCHER: DurableObjectNamespace<PublisherReceiver>;
}

const task = definePublishTask<[string, number[]], string[]>(
  "publisher_contract",
);
const publisher = createPostgresPublisher((env: Env) => ({
  connectionString: env.HYPERDRIVE.connectionString,
  schema: env.BELLOWS_SCHEMA,
  task,
  dispatcher: env.DISPATCHER,
}));

// Test-only controls, separate from the application producer and retained dispatcher.
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (path === "/publisher/publish") {
      try {
        return Response.json(
          await publisher.publish(env, ['hello "🦀"\n', [1, 2, 3]]),
        );
      } catch (error) {
        if (!(error instanceof PostgresPublisherError)) throw error;
        return Response.json(
          {
            stage: error.stage,
            receipt: error.receipt ?? null,
            error: error.message,
          },
          { status: 503 },
        );
      }
    }
    if (path === "/publisher/redispatch") {
      const { taskId } = await request.json<{ taskId: string }>();
      await dispatchTask(env.DISPATCHER, taskId);
      return Response.json({ taskId });
    }
    return env.DISPATCHER.getByName("global").fetch(request);
  },
} satisfies ExportedHandler<Env>;

export class PublisherReceiver extends DurableObject<Env> {
  private status = 200;
  private readonly dispatches: unknown[] = [];
  private drained = 0;
  private readonly releases: Array<() => void> = [];
  private draining = false;

  async fetch(request: Request): Promise<Response> {
    switch (new URL(request.url).pathname) {
      case "/publisher/state":
        return Response.json({
          dispatches: this.dispatches,
          drained: this.drained,
        });
      case "/publisher/response":
        this.status = (await request.json<{ status: number }>()).status;
        return new Response(null);
      case "/publisher/release":
        this.releases.shift()?.();
        return new Response(null);
      case "/publisher/drain":
        this.draining = true;
        for (const release of this.releases.splice(0)) release();
        return new Response(null);
      case "/dispatch": {
        this.dispatches.push(await request.json());
        const released = this.draining
          ? Promise.resolve()
          : new Promise<void>((resolve) => this.releases.push(resolve));
        const encoder = new TextEncoder();
        let sent = false;
        return new Response(
          new ReadableStream<Uint8Array>({
            pull: async (controller) => {
              if (!sent) {
                sent = true;
                // Withhold the tail after more than the dispatch helper's diagnostic excerpt.
                controller.enqueue(
                  encoder.encode(`fixture-secret:${"a".repeat(10_000)}`),
                );
              } else {
                await released;
                controller.enqueue(encoder.encode("🦀:complete"));
                controller.close();
                this.drained += 1;
              }
            },
          }),
          { status: this.status },
        );
      }
      default:
        return new Response("not-found", { status: 404 });
    }
  }
}
