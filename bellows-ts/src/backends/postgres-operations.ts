import { Client, type Pool, type PoolClient } from "pg";
import {
  type ClaimedTask,
  type FailedTask,
  type FinishedTask,
  LeaseLostError,
  type PublishedTask,
  type PublishTaskDefinition,
  type RenewedTaskLease,
  type SingletonTaskDefinition,
  type TaskCallback,
  type TaskDefinition,
  type TaskExecutionBackend,
  TaskLeasedError,
  TaskNotFoundError,
  TaskUnavailableError,
} from "../types.js";

export const POSTGRES_NOTIFY_CHANNEL = "bellows_tasks";

const INITIALIZE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS bellows_tasks (
    task_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_name TEXT NOT NULL,
    task_unique_key TEXT,
    payload_json TEXT NOT NULL,
    callback_id BIGINT,
    lease_worker_id BIGINT,
    available_from_unix_ms BIGINT,
    CHECK (lease_worker_id IS NULL OR available_from_unix_ms IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS bellows_tasks_unique_key_idx
    ON bellows_tasks (task_unique_key);

CREATE INDEX IF NOT EXISTS bellows_tasks_available_idx
    ON bellows_tasks (task_name, task_unique_key, available_from_unix_ms, task_id);

CREATE OR REPLACE FUNCTION bellows_notify_task_available()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify(
        'bellows_tasks',
        json_build_object(
            'kind', 'new_task_available',
            'task_name', NEW.task_name,
            'task_id', NEW.task_id,
            'available_from_unix_ms', NEW.available_from_unix_ms
        )::text
    );

    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'bellows_tasks_notify_available'
          AND tgrelid = 'bellows_tasks'::regclass
    ) THEN
        CREATE TRIGGER bellows_tasks_notify_available
        AFTER INSERT OR UPDATE OF lease_worker_id, available_from_unix_ms ON bellows_tasks
        FOR EACH ROW
        EXECUTE FUNCTION bellows_notify_task_available();
    END IF;
END;
$$;
`;

const POSTGRES_SCHEMA_NAME_PATTERN = /^(?!.*[\r\n])[a-z_][a-z0-9_]*$/;
const POSTGRES_INITIALIZE_ADVISORY_LOCK = 5024011519;

export interface PostgresBackendOptions {
  readonly schema?: string;
}

export function validatePostgresSchemaName(schemaName: string): string {
  if (
    typeof schemaName !== "string" ||
    !POSTGRES_SCHEMA_NAME_PATTERN.test(schemaName)
  ) {
    throw new Error(
      "Database schema names must contain only lowercase letters, digits, and underscores.",
    );
  }

  return schemaName;
}

export async function initializePostgresSchema(
  databaseUrl: string,
  schemaName: string,
): Promise<void> {
  const parsedSchemaName = validatePostgresSchemaName(schemaName);
  const client = new Client({ connectionString: databaseUrl });

  try {
    await client.connect();
    await initializePostgresClient(client, parsedSchemaName);
  } finally {
    await client.end().catch(() => undefined);
  }
}

export async function initializePostgresPool(
  pool: Pool,
  options: PostgresBackendOptions = {},
): Promise<void> {
  const parsedSchemaName = parseSchemaOption(options);
  const client = await pool.connect();

  try {
    await initializePostgresClient(client, parsedSchemaName);
  } finally {
    client.release();
  }
}

export class PostgresTaskOperations implements TaskExecutionBackend {
  // Explicit qualification avoids relying on a connection-level search_path through pooling.
  private readonly tableName: string;

  constructor(
    private readonly pool: Pool,
    options: PostgresBackendOptions = {},
  ) {
    this.tableName = qualifiedTasksTable(parseSchemaOption(options));
  }

  async publish<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
    callbackId: number | null,
    availableFromMs: number | null,
  ): Promise<PublishedTask> {
    const result = await this.pool.query<{ task_id: string }>(
      `
INSERT INTO ${this.tableName} (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    available_from_unix_ms
)
VALUES ($1, NULL, $2, $3, NULL, $4)
RETURNING task_id::text AS task_id
      `,
      [task.name, task.codec.encode(payload), callbackId, availableFromMs],
    );

    return { taskId: Number(result.rows[0].task_id) };
  }

  async claimPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    const claimedResult = await this.pool.query<{ payload_json: string }>(
      `
UPDATE ${this.tableName}
SET lease_worker_id = $1,
    available_from_unix_ms = $2
WHERE task_id = $3
  AND task_name = $4
  AND task_unique_key IS NULL
  AND (
        available_from_unix_ms IS NULL
        OR available_from_unix_ms <= $5
      )
RETURNING payload_json
      `,
      [workerId, leaseExpirationMs, taskId, task.name, Date.now()],
    );

    if (claimedResult.rowCount === 0) {
      throw await this.loadClaimFailure(this.pool, taskId, task.name);
    }

    return {
      taskId,
      taskPayload: task.codec.decode(claimedResult.rows[0].payload_json),
      leaseExpirationMs,
    };
  }

  async claimEarliestPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    const claimedResult = await this.pool.query<{
      task_id: string;
      payload_json: string;
    }>(
      `
WITH next_task AS (
    SELECT task_id
    FROM ${this.tableName}
    WHERE task_name = $1
      AND task_unique_key IS NULL
      AND (
            available_from_unix_ms IS NULL
            OR available_from_unix_ms <= $2
          )
    ORDER BY available_from_unix_ms NULLS FIRST, task_id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE ${this.tableName} AS tasks
SET lease_worker_id = $3,
    available_from_unix_ms = $4
FROM next_task
WHERE tasks.task_id = next_task.task_id
RETURNING tasks.task_id::text AS task_id, tasks.payload_json
      `,
      [task.name, Date.now(), workerId, leaseExpirationMs],
    );

    if (claimedResult.rowCount === 0) {
      const availableFromResult = await this.pool.query<{
        available_from_unix_ms: string | null;
      }>(
        `
SELECT MIN(available_from_unix_ms)::text AS available_from_unix_ms
FROM ${this.tableName}
WHERE task_name = $1
  AND task_unique_key IS NULL
  AND available_from_unix_ms > $2
        `,
        [task.name, Date.now()],
      );

      throw new TaskUnavailableError(
        availableFromResult.rows[0]?.available_from_unix_ms === null
          ? null
          : Number(availableFromResult.rows[0]?.available_from_unix_ms ?? null),
      );
    }

    return {
      taskId: Number(claimedResult.rows[0].task_id),
      taskPayload: task.codec.decode(claimedResult.rows[0].payload_json),
      leaseExpirationMs,
    };
  }

  async claimSingleton<TCallback>(
    task: SingletonTaskDefinition<TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<undefined>> {
    const claimedResult = await this.pool.query<{ task_id: string }>(
      `
INSERT INTO ${this.tableName} AS tasks (
    task_name,
    task_unique_key,
    payload_json,
    callback_id,
    lease_worker_id,
    available_from_unix_ms
)
VALUES ($1, $2, 'null', NULL, $3, $4)
ON CONFLICT (task_unique_key) DO UPDATE
SET lease_worker_id = EXCLUDED.lease_worker_id,
    available_from_unix_ms = EXCLUDED.available_from_unix_ms
WHERE tasks.task_name = EXCLUDED.task_name
  AND (
        tasks.available_from_unix_ms IS NULL
        OR tasks.available_from_unix_ms <= $5
      )
RETURNING task_id::text AS task_id
      `,
      [task.name, task.name, workerId, leaseExpirationMs, Date.now()],
    );

    if (claimedResult.rowCount === 0) {
      const currentResult = await this.pool.query<{
        lease_worker_id: string | null;
        available_from_unix_ms: string | null;
      }>(
        `
SELECT lease_worker_id::text AS lease_worker_id,
       available_from_unix_ms::text AS available_from_unix_ms
FROM ${this.tableName}
WHERE task_name = $1
  AND task_unique_key = $2
        `,
        [task.name, task.name],
      );

      if (currentResult.rowCount === 0) {
        throw new TaskNotFoundError();
      }

      const current = currentResult.rows[0];
      if (
        current.available_from_unix_ms !== null &&
        Number(current.available_from_unix_ms) > Date.now()
      ) {
        if (current.lease_worker_id !== null) {
          throw new TaskLeasedError(Number(current.available_from_unix_ms));
        }

        throw new TaskUnavailableError(Number(current.available_from_unix_ms));
      }

      throw new TaskNotFoundError();
    }

    return {
      taskId: Number(claimedResult.rows[0].task_id),
      taskPayload: undefined,
      leaseExpirationMs,
    };
  }

  async renew(
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<RenewedTaskLease> {
    const result = await this.pool.query(
      `
UPDATE ${this.tableName}
SET available_from_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
      `,
      [leaseExpirationMs, taskId, workerId],
    );

    if (result.rowCount === 0) {
      throw new LeaseLostError();
    }

    return { newExpirationMs: leaseExpirationMs };
  }

  async fail(
    workerId: number,
    taskId: number,
    availableFromMs: number | null,
  ): Promise<FailedTask> {
    const result = await this.pool.query(
      `
UPDATE ${this.tableName}
SET lease_worker_id = NULL,
    available_from_unix_ms = $1
WHERE task_id = $2
  AND lease_worker_id = $3
      `,
      [availableFromMs, taskId, workerId],
    );

    if (result.rowCount === 0) {
      throw new LeaseLostError();
    }

    return { taskId };
  }

  async finish<TTask extends TaskDefinition>(
    task: TTask,
    workerId: number,
    taskId: number,
    callbackPayload: TaskCallback<TTask>,
    availableFromMs: number | null,
  ): Promise<FinishedTask> {
    const callbackPayloadJson = task.callbackCodec.encode(callbackPayload);
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");

      const finishedRow =
        task.kind === "singleton"
          ? (
              await client.query<{
                callback_id: number | string | null;
              }>(
                `
WITH claimed AS (
    SELECT task_id, callback_id
    FROM ${this.tableName}
    WHERE task_id = $1
      AND lease_worker_id = $2
      AND task_unique_key IS NOT NULL
    FOR UPDATE
), updated AS (
    UPDATE ${this.tableName} AS tasks
    SET lease_worker_id = NULL,
        callback_id = NULL,
        available_from_unix_ms = $3
    WHERE tasks.task_id IN (SELECT task_id FROM claimed)
    RETURNING tasks.task_id
)
SELECT claimed.callback_id
FROM claimed
JOIN updated ON updated.task_id = claimed.task_id
                `,
                [taskId, workerId, availableFromMs],
              )
            ).rows[0]
          : availableFromMs !== null
            ? (
                await client.query<{
                  callback_id: number | string | null;
                }>(
                  `
WITH claimed AS (
    SELECT task_id, callback_id
    FROM ${this.tableName}
    WHERE task_id = $1
      AND lease_worker_id = $2
      AND task_unique_key IS NULL
    FOR UPDATE
), updated AS (
    UPDATE ${this.tableName} AS tasks
    SET lease_worker_id = NULL,
        callback_id = NULL,
        available_from_unix_ms = $3
    WHERE tasks.task_id IN (SELECT task_id FROM claimed)
    RETURNING tasks.task_id
)
SELECT claimed.callback_id
FROM claimed
JOIN updated ON updated.task_id = claimed.task_id
                  `,
                  [taskId, workerId, availableFromMs],
                )
              ).rows[0]
            : (
                await client.query<{
                  callback_id: number | string | null;
                }>(
                  `
DELETE FROM ${this.tableName}
WHERE task_id = $1
  AND lease_worker_id = $2
  AND task_unique_key IS NULL
RETURNING callback_id
                  `,
                  [taskId, workerId],
                )
              ).rows[0];

      if (!finishedRow) {
        await client.query("ROLLBACK");
        throw new LeaseLostError();
      }

      if (finishedRow.callback_id !== null) {
        await client.query(
          `
SELECT pg_notify(
    $1,
    json_build_object(
      'kind',
      'task_callback',
      'task_name',
      $2::text,
      'callback_id',
      $3::bigint,
      'callback_payload_json',
      $4::text
    )::text
)
          `,
          [
            POSTGRES_NOTIFY_CHANNEL,
            task.name,
            finishedRow.callback_id,
            callbackPayloadJson,
          ],
        );
      }

      await client.query("COMMIT");
      return { taskId };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async loadClaimFailure(
    client: Pool | PoolClient,
    taskId: number,
    taskName: string,
  ): Promise<TaskLeasedError | TaskUnavailableError | TaskNotFoundError> {
    const currentResult = await client.query<{
      lease_worker_id: string | null;
      available_from_unix_ms: string | null;
    }>(
      `
SELECT lease_worker_id::text AS lease_worker_id,
       available_from_unix_ms::text AS available_from_unix_ms
FROM ${this.tableName}
WHERE task_id = $1
  AND task_name = $2
  AND task_unique_key IS NULL
      `,
      [taskId, taskName],
    );

    if (currentResult.rowCount === 0) {
      return new TaskNotFoundError();
    }

    const current = currentResult.rows[0];
    if (
      current.available_from_unix_ms !== null &&
      Number(current.available_from_unix_ms) > Date.now()
    ) {
      if (current.lease_worker_id !== null) {
        return new TaskLeasedError(Number(current.available_from_unix_ms));
      }

      return new TaskUnavailableError(Number(current.available_from_unix_ms));
    }

    return new TaskNotFoundError();
  }
}

function parseSchemaOption(
  options: PostgresBackendOptions,
): string | undefined {
  return options.schema === undefined
    ? undefined
    : validatePostgresSchemaName(options.schema);
}

function qualifiedTasksTable(schemaName: string | undefined): string {
  if (schemaName === undefined) {
    return "bellows_tasks";
  }

  return `${quoteIdentifier(schemaName)}.bellows_tasks`;
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

async function initializePostgresClient(
  client: Pick<Client, "query">,
  schemaName: string | undefined,
): Promise<void> {
  try {
    await client.query("BEGIN");
    await client.query(
      `SELECT pg_advisory_xact_lock(${POSTGRES_INITIALIZE_ADVISORY_LOCK})`,
    );
    if (schemaName !== undefined) {
      await client.query(
        `SET LOCAL search_path TO ${quoteIdentifier(schemaName)}`,
      );
    }
    await client.query(INITIALIZE_SCHEMA_SQL);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}
