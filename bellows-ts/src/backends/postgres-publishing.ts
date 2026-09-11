import { Pool } from "pg";
import type {
  PublishedTask,
  PublishTaskDefinition,
  TaskPublishingBackend,
} from "../types.js";
import {
  type PostgresBackendOptions,
  type PostgresPublishingExecutor,
  PostgresPublishingOperations,
} from "./postgres-operations.js";

export type {
  PostgresBackendOptions,
  PostgresPublishingExecutor,
  PostgresPublishParameters,
} from "./postgres-operations.js";
export { PostgresPublishedTaskIdError } from "./postgres-operations.js";

/**
 * Publishing-only PostgreSQL backend, without a listener, callback registry, or execution API.
 * Callback-bearing definitions remain publishable without callback registration; singleton
 * definitions are not publishable. Insert triggers still notify native consumers at commit.
 * Awaitable publication needs the full backend's listener-backed callback delivery.
 *
 * Initialize the existing schema separately with `initializePostgresSchema` from `backends/postgres`.
 * Future publication stores availability; it does not schedule a future Worker request.
 * `connect()` owns its pool; `fromExecutor()` borrows caller resources and can join an application
 * transaction. Publication does not atomically dispatch to a Durable Object or retry.
 * An error after sending an insert does not prove it failed to commit.
 * Receipts contain safe numeric IDs; `PostgresPublishedTaskIdError` retains the exact inserted
 * ID as a string when it cannot be represented safely.
 *
 * On Workers, use a request-scoped Hyperdrive connection. For `connect()`, await `close()` in
 * `finally` before returning the response; caller-owned executors require caller cleanup instead.
 * Never retain connections across requests.
 * For immediate publication followed by dispatch, `createPostgresPublisher` from
 * `cloudflare/postgres` owns that lifecycle and returns checked string receipts.
 */
export class PostgresPublishingBackend implements TaskPublishingBackend {
  private constructor(
    private readonly ownedPool: Pool | undefined,
    private readonly operations: PostgresPublishingOperations,
  ) {}

  /** Creates a pool for an optional existing schema; does not initialize tables or set search_path. */
  static async connect(
    databaseUrl: string,
    options: PostgresBackendOptions = {},
  ): Promise<PostgresPublishingBackend> {
    const pool = new Pool({ connectionString: databaseUrl });

    try {
      const operations = new PostgresPublishingOperations(pool, options);
      return new PostgresPublishingBackend(pool, operations);
    } catch (error) {
      await pool.end().catch(() => undefined);
      throw error;
    }
  }

  /**
   * Uses a caller-owned pool, client, or transaction adapter without connecting, initializing
   * tables, or changing search_path. Explicit schema options are validated synchronously and
   * qualify the task table; omitting schema uses the executor's search path without a public fallback.
   * Bellows never finalizes or closes the executor, even on errors or repeated `close()` calls.
   *
   * To publish atomically with business mutations, supply their same transaction-bound executor
   * and keep this publisher within the transaction's scope. Receipts and receipt-validation errors
   * are provisional until caller commit succeeds. PostgreSQL delivers trigger notifications at
   * commit; explicit dispatch remains a separate caller action after successful commit.
   * A post-commit dispatch failure does not undo publication: retain the receipt and recover
   * dispatch rather than blindly republishing.
   */
  static fromExecutor(
    executor: PostgresPublishingExecutor,
    options: PostgresBackendOptions = {},
  ): PostgresPublishingBackend {
    return new PostgresPublishingBackend(
      undefined,
      new PostgresPublishingOperations(executor, options),
    );
  }

  /**
   * Awaits shutdown of the pool created by `connect()`, including checked-out connections.
   * For `fromExecutor()`, this is a repeatable no-op: it neither closes nor disables the executor.
   */
  async close(): Promise<void> {
    await this.ownedPool?.end();
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
