// Compile-only contract against Wrangler's generated Workers types, not a deployed entrypoint.
import {
  createPostgresProcessor,
  createPostgresProcessorTask,
  createPostgresSweeper,
} from "../../../../src/cloudflare/postgres.js";
import { defineSingletonTask, TaskSuccess } from "../../../../src/index.js";

interface TaskSweeperEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
  DISPATCHER: DurableObjectNamespace;
}

const periodicTask = defineSingletonTask("periodic");

const sweepConfigFor = (env: TaskSweeperEnv) => ({
  connectionString: env.HYPERDRIVE.connectionString,
  schema: env.BELLOWS_SCHEMA,
  dispatcher: env.DISPATCHER,
  singletons: [periodicTask, defineSingletonTask<number>("count")],
});

export default createPostgresSweeper(
  sweepConfigFor,
) satisfies ExportedHandler<TaskSweeperEnv>;

// The README's singleton factory uses the same definition, not a publication token.
export const processor = createPostgresProcessor((env: TaskSweeperEnv) => ({
  connectionString: env.HYPERDRIVE.connectionString,
  schema: env.BELLOWS_SCHEMA,
  tasks: [
    createPostgresProcessorTask({
      task: periodicTask,
      build: () => ({
        async process(taskId, payload) {
          console.log(taskId, payload);
          return TaskSuccess.scheduleNextRun(undefined, Date.now() + 60_000);
        },
      }),
    }),
  ],
})) satisfies ExportedHandler<TaskSweeperEnv>;
