import { Pool } from "pg";
import type {
  PublishedTask,
  PublishTaskDefinition,
  TaskPublishingBackend,
} from "../types.js";
import {
  type PostgresBackendOptions,
  PostgresTaskOperations,
} from "./postgres-operations.js";

export type { PostgresBackendOptions } from "./postgres-operations.js";

/**
 * Publishing-only PostgreSQL backend, without a listener, callback registry, or execution API.
 * Callback-bearing definitions remain publishable; insert triggers still notify native consumers.
 * Awaitable publication needs the full backend's listener-backed callback delivery.
 *
 * Initialize the existing schema separately with `initializePostgresSchema` from `backends/postgres`.
 * Future publication stores availability; it does not schedule a future Worker request.
 * Publication does not atomically dispatch to a Durable Object, retry, or join an application
 * transaction. An error after sending an insert does not prove it failed to commit.
 *
 * On Workers, use a request-scoped Hyperdrive connection and await `close()` in `finally` before
 * returning the response. Never retain connections across requests.
 */
export class PostgresPublishingBackend implements TaskPublishingBackend {
  private constructor(
    private readonly pool: Pool,
    private readonly operations: PostgresTaskOperations,
  ) {}

  /** Creates a pool for an optional existing schema; does not initialize tables or set search_path. */
  static async connect(
    databaseUrl: string,
    options: PostgresBackendOptions = {},
  ): Promise<PostgresPublishingBackend> {
    const pool = new Pool({ connectionString: databaseUrl });

    try {
      const operations = new PostgresTaskOperations(pool, options);
      return new PostgresPublishingBackend(pool, operations);
    } catch (error) {
      await pool.end().catch(() => undefined);
      throw error;
    }
  }

  /** Awaits pool shutdown, including checked-out connections. Subsequent operations fail. */
  async close(): Promise<void> {
    await this.pool.end();
  }

  async publish<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
  ): Promise<PublishedTask> {
    return await this.operations.publish(task, payload, null, null);
  }

  async publishFuture<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
    availableFromMs: number,
  ): Promise<PublishedTask> {
    return await this.operations.publish(task, payload, null, availableFromMs);
  }
}
