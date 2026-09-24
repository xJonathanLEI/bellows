import { Client } from "pg";
import {
  createPostgresProcessor,
  createPostgresProcessorTask,
} from "../../../../src/cloudflare/postgres.js";
import {
  TaskFailure,
  TaskSuccess,
  type WorkerFactory,
} from "../../../../src/index.js";
import {
  fullNameTask,
  greetingTask,
  schedulingTask,
  singletonTask,
  unconfiguredSingletonTask,
} from "../task.js";

interface ProcessorEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
  BELLOWS_SINGLETON_MODE?: string;
}

const processor = createPostgresProcessor((env: ProcessorEnv) => {
  const connectionString = env.HYPERDRIVE.connectionString;
  const schema = env.BELLOWS_SCHEMA;
  const singletonMode = env.BELLOWS_SINGLETON_MODE ?? "park";
  let operation: Promise<number> | undefined;
  function record(taskId: number, name: string): Promise<number> {
    operation = (async () => {
      // Only a successful claim opens this separate business connection.
      const client = new Client({ connectionString });
      try {
        await client.connect();
        const result = await client.query<{ execution_count: number }>(
          `
INSERT INTO "${schema}".processed_tasks AS processed (task_id, name, execution_count)
VALUES ($1, $2, 1)
ON CONFLICT (task_id) DO UPDATE
SET name = EXCLUDED.name, execution_count = processed.execution_count + 1
RETURNING execution_count
          `,
          [taskId, name],
        );
        return result.rows[0].execution_count;
      } finally {
        await client.end();
      }
    })();
    return operation;
  }
  const greetingFactory: WorkerFactory<typeof greetingTask> = {
    task: greetingTask,
    build() {
      return {
        async process(taskId, payload) {
          await record(taskId, payload.name);
          return TaskSuccess.done(undefined);
        },
      };
    },
  };
  const schedulingFactory: WorkerFactory<typeof schedulingTask> = {
    task: schedulingTask,
    build() {
      return {
        async process(taskId, payload) {
          const count = await record(taskId, payload.name);
          if (count !== 1) return TaskSuccess.done(undefined);
          switch (payload.mode) {
            case "failure":
              return TaskFailure.retryAt(payload.availableFromMs);
            case "success":
              return TaskSuccess.scheduleNextRun(
                undefined,
                payload.availableFromMs,
              );
            case "immediate":
              return TaskFailure.retryImmediately();
          }
        },
      };
    },
  };
  const fullNameFactory: WorkerFactory<typeof fullNameTask> = {
    task: fullNameTask,
    build() {
      return {
        async process(taskId, payload) {
          await record(taskId, `${payload.firstName} ${payload.lastName}`);
          return TaskSuccess.done(undefined);
        },
      };
    },
  };
  return {
    connectionString,
    schema,
    tasks: [
      createPostgresProcessorTask(greetingFactory),
      createPostgresProcessorTask(fullNameFactory),
      createPostgresProcessorTask(schedulingFactory),
      ...[singletonTask, unconfiguredSingletonTask].map((task) =>
        createPostgresProcessorTask({
          task,
          build() {
            return {
              async process(taskId, payload) {
                if (payload !== undefined)
                  throw new Error("singleton payload must be undefined");
                const count = await record(taskId, task.name);
                const atMs =
                  Date.now() +
                  (count === 1 && singletonMode !== "park" ? 1_000 : 60_000);
                if (count === 1) {
                  if (singletonMode === "failure")
                    return TaskFailure.retryAt(atMs);
                  if (singletonMode === "immediate")
                    return TaskFailure.retryImmediately();
                  if (singletonMode === "done")
                    return TaskSuccess.done(undefined);
                }
                return TaskSuccess.scheduleNextRun(undefined, atMs);
              },
            };
          },
        }),
      ),
    ],
    async cleanup() {
      // The runtime handles worker failures; drain work and its finally after lease loss.
      await operation?.catch(() => {});
    },
  };
});

// Fixture-only response loss, after all real finalization and owned cleanup.
// The producer never forwards these control routes.
const lostResponses = new Set<string>();
const singletonAttempts = new Map<string, number>();
export default {
  async fetch(request: Request, env: ProcessorEnv): Promise<Response> {
    if (new URL(request.url).pathname === "/__test/attempts") {
      return Response.json(Object.fromEntries(singletonAttempts));
    }
    if (new URL(request.url).pathname === "/__test/lose-response") {
      const { taskId, taskName } = await request.json<{
        taskId?: string;
        taskName?: string;
      }>();
      const key = taskName ?? taskId;
      if (key === undefined)
        return new Response("missing identity", { status: 400 });
      lostResponses.add(key);
      return new Response(null);
    }
    const response = await processor.fetch(request, env);
    if (response.status !== 200) return response;
    const body = await response.json<{
      task:
        | { kind: "published"; taskId: string; taskName: string }
        | { kind: "singleton"; taskName: string };
      nextAction: { type: string };
    }>();
    if (body.task.kind === "singleton") {
      singletonAttempts.set(
        body.task.taskName,
        (singletonAttempts.get(body.task.taskName) ?? 0) + 1,
      );
    }
    if (
      (body.task.kind === "singleton" || body.nextAction.type === "done") &&
      lostResponses.delete(
        body.task.kind === "singleton" ? body.task.taskName : body.task.taskId,
      )
    ) {
      // Lose the committed instruction without changing PostgreSQL's outcome.
      return new Response("fixture response lost", { status: 503 });
    }
    return Response.json(body);
  },
} satisfies ExportedHandler<ProcessorEnv>;
