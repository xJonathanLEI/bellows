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
import { fullNameTask, greetingTask, schedulingTask } from "../task.js";

interface ProcessorEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
}

const processor = createPostgresProcessor((env: ProcessorEnv) => {
  const connectionString = env.HYPERDRIVE.connectionString;
  const schema = env.BELLOWS_SCHEMA;
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
export default {
  async fetch(request: Request, env: ProcessorEnv): Promise<Response> {
    if (new URL(request.url).pathname === "/__test/lose-response") {
      const { taskId } = await request.json<{ taskId: string }>();
      lostResponses.add(taskId);
      return new Response(null);
    }
    const response = await processor.fetch(request, env);
    if (response.status !== 200 || lostResponses.size === 0) return response;
    const body = await response.json<{
      taskId: string;
      nextAction: { type: string };
    }>();
    if (body.nextAction.type === "done" && lostResponses.delete(body.taskId)) {
      // The committed done instruction is lost, not changed into a business retry.
      return new Response("fixture response lost", { status: 503 });
    }
    return Response.json(body);
  },
} satisfies ExportedHandler<ProcessorEnv>;
