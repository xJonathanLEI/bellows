import { Client } from "pg";
import {
  createPostgresProcessor,
  createPostgresProcessorTask,
} from "../../../../src/cloudflare/postgres.js";
import { TaskSuccess, type WorkerFactory } from "../../../../src/index.js";
import { fullNameTask, greetingTask } from "../task.js";

interface ProcessorEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
}

export default createPostgresProcessor((env: ProcessorEnv) => {
  const connectionString = env.HYPERDRIVE.connectionString;
  const schema = env.BELLOWS_SCHEMA;
  let operation: Promise<TaskSuccess<void>> | undefined;
  function record(taskId: number, name: string): Promise<TaskSuccess<void>> {
    operation = (async () => {
      // Only a successful claim opens this separate business connection.
      const client = new Client({ connectionString });
      try {
        await client.connect();
        await client.query(
          `
INSERT INTO "${schema}".processed_tasks AS processed (task_id, name, execution_count)
VALUES ($1, $2, 1)
ON CONFLICT (task_id) DO UPDATE
SET name = EXCLUDED.name, execution_count = processed.execution_count + 1
          `,
          [taskId, name],
        );
        return TaskSuccess.done(undefined);
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
        process(taskId, payload) {
          return record(taskId, payload.name);
        },
      };
    },
  };
  const fullNameFactory: WorkerFactory<typeof fullNameTask> = {
    task: fullNameTask,
    build() {
      return {
        process(taskId, payload) {
          return record(taskId, `${payload.firstName} ${payload.lastName}`);
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
    ],
    async cleanup() {
      // The runtime handles worker failures; drain work and its finally after lease loss.
      await operation?.catch(() => {});
    },
  };
});
