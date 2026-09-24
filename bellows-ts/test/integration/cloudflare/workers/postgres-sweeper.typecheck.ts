// Compile-only contract against Wrangler's generated Workers types, not a deployed entrypoint.
import { createPostgresSweeper } from "../../../../src/cloudflare/postgres.js";

interface TaskSweeperEnv {
  HYPERDRIVE: Hyperdrive;
  BELLOWS_SCHEMA: string;
  DISPATCHER: DurableObjectNamespace;
}

const sweepConfigFor = (env: TaskSweeperEnv) => ({
  connectionString: env.HYPERDRIVE.connectionString,
  schema: env.BELLOWS_SCHEMA,
  dispatcher: env.DISPATCHER,
});

export default createPostgresSweeper(
  sweepConfigFor,
) satisfies ExportedHandler<TaskSweeperEnv>;
