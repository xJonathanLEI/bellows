import { PostgresBackend } from "../backends/postgres.js";
import { echoTask, emitEvent } from "./shared.js";

const [databaseUrl, name] = parseArgs();
const backend = await PostgresBackend.connect(databaseUrl);
await backend.initialize();

const awaitable = await backend.publishAwaitable(echoTask, { name });
emitEvent({ event: "published", taskId: awaitable.taskId, name });

const awaitedName = await awaitable.wait();
emitEvent({ event: "awaited", taskId: awaitable.taskId, name: awaitedName });

await backend.close();

function parseArgs(): [string, string] {
  const [databaseUrl, name, ...rest] = process.argv.slice(2);

  if (databaseUrl === undefined) {
    throw new Error("expected database URL as the first argument");
  }

  if (name === undefined) {
    throw new Error("expected task name as the second argument");
  }

  if (rest.length > 0) {
    throw new Error(
      "unexpected extra arguments passed to the TypeScript publisher",
    );
  }

  return [databaseUrl, name];
}
