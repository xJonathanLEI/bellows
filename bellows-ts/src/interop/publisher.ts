import { Client } from "pg";
import { PostgresBackend } from "../backends/postgres.js";
import {
  type PostgresPublicationRow,
  preparePostgresPublicationFromEncodedRows,
} from "../backends/postgres-publishing.js";
import { echoTask, emitEvent } from "./shared.js";

const [databaseUrl, name, fragmentMode] = parseArgs();
const backend = await PostgresBackend.connect(databaseUrl);
await backend.initialize();

if (fragmentMode) {
  const fragment = preparePostgresPublicationFromEncodedRows(echoTask, {
    source: { relation: "inputs", payloadColumn: "payload" },
    parameterOffset: 1,
  });
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query<PostgresPublicationRow>(
      `WITH inputs AS (SELECT json_build_object('name', $1::text)::text AS payload),
       published AS (${fragment.sql}) SELECT task_id FROM published`,
      [name, ...fragment.parameters],
    );
    const receipts = fragment.readReceipts(result.rows);
    const receipt = receipts[0];
    if (receipts.length !== 1 || receipt === undefined) {
      throw new Error("expected exactly one fragment publication receipt");
    }
    await client.query("COMMIT");
    emitEvent({ event: "published", taskId: receipt.taskId, name });
  } finally {
    await client.end();
  }
  // Fragment publication deliberately registers no callback.
} else {
  const awaitable = await backend.publishAwaitable(echoTask, { name });
  emitEvent({ event: "published", taskId: awaitable.taskId, name });

  const awaitedName = await awaitable.wait();
  emitEvent({ event: "awaited", taskId: awaitable.taskId, name: awaitedName });
}

await backend.close();

function parseArgs(): [string, string, boolean] {
  const [databaseUrl, name, mode, ...rest] = process.argv.slice(2);

  if (databaseUrl === undefined) {
    throw new Error("expected database URL as the first argument");
  }

  if (name === undefined) {
    throw new Error("expected task name as the second argument");
  }

  if (rest.length > 0 || (mode !== undefined && mode !== "fragment")) {
    throw new Error(
      "unexpected extra arguments passed to the TypeScript publisher",
    );
  }

  return [databaseUrl, name, mode === "fragment"];
}
