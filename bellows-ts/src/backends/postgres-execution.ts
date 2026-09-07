import { Pool } from "pg";
import type {
  ClaimedTask,
  FailedTask,
  FinishedTask,
  PublishTaskDefinition,
  RenewedTaskLease,
  SingletonTaskDefinition,
  TaskCallback,
  TaskDefinition,
  TaskExecutionBackend,
} from "../types.js";
import {
  type PostgresBackendOptions,
  PostgresTaskOperations,
} from "./postgres-operations.js";

export class PostgresExecutionBackend implements TaskExecutionBackend {
  private constructor(
    private readonly pool: Pool,
    private readonly operations: PostgresTaskOperations,
  ) {}

  static async connect(
    databaseUrl: string,
    options: PostgresBackendOptions = {},
  ): Promise<PostgresExecutionBackend> {
    const pool = new Pool({ connectionString: databaseUrl });

    try {
      const operations = new PostgresTaskOperations(pool, options);
      return new PostgresExecutionBackend(pool, operations);
    } catch (error) {
      await pool.end().catch(() => undefined);
      throw error;
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  async claimPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    return await this.operations.claimPublished(
      task,
      workerId,
      taskId,
      leaseExpirationMs,
    );
  }

  async claimEarliestPublished<TPayload, TCallback>(
    task: PublishTaskDefinition<TPayload, TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    return await this.operations.claimEarliestPublished(
      task,
      workerId,
      leaseExpirationMs,
    );
  }

  async claimSingleton<TCallback>(
    task: SingletonTaskDefinition<TCallback>,
    workerId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<undefined>> {
    return await this.operations.claimSingleton(
      task,
      workerId,
      leaseExpirationMs,
    );
  }

  async renew(
    workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<RenewedTaskLease> {
    return await this.operations.renew(workerId, taskId, leaseExpirationMs);
  }

  async fail(
    workerId: number,
    taskId: number,
    availableFromMs: number | null,
  ): Promise<FailedTask> {
    return await this.operations.fail(workerId, taskId, availableFromMs);
  }

  async finish<TTask extends TaskDefinition>(
    task: TTask,
    workerId: number,
    taskId: number,
    callbackPayload: TaskCallback<TTask>,
    availableFromMs: number | null,
  ): Promise<FinishedTask> {
    return await this.operations.finish(
      task,
      workerId,
      taskId,
      callbackPayload,
      availableFromMs,
    );
  }
}
