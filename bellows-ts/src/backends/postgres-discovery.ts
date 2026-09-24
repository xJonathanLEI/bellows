import { Pool } from "pg";
import {
  type PostgresBackendOptions,
  type PostgresDiscoveryCandidate,
  PostgresDiscoveryOperations,
  type PostgresSweepWindow,
} from "./postgres-operations.js";

export type {
  PostgresBackendOptions,
  PostgresDiscoveryCandidate,
  PostgresSweepWindow,
} from "./postgres-operations.js";

/**
 * Owned read-only PostgreSQL discovery without listeners, payload decoding, or claims.
 * Initialize schemas separately. The selected schema must belong entirely to the consuming
 * workload. On Workers use Hyperdrive with query caching disabled, never retain the backend across
 * events, and await close on success and error paths.
 */
export class PostgresDiscoveryBackend {
  private constructor(
    private readonly pool: Pool,
    private readonly operations: PostgresDiscoveryOperations,
  ) {}

  /** Validates/qualifies an optional existing schema; otherwise uses the default search path. */
  static async connect(
    databaseUrl: string,
    options: PostgresBackendOptions = {},
  ): Promise<PostgresDiscoveryBackend> {
    const pool = new Pool({ connectionString: databaseUrl });
    try {
      return new PostgresDiscoveryBackend(
        pool,
        new PostgresDiscoveryOperations(pool, options),
      );
    } catch (error) {
      await pool.end().catch(() => undefined);
      throw error;
    }
  }

  /** Captures database statement time and maximum task ID together, without a transaction. */
  async beginSweep(): Promise<PostgresSweepWindow> {
    return await this.operations.beginSweep();
  }

  /**
   * Reads at most 100 eligible identities in numeric ID order. Start with null, then use the
   * last returned ID even if consuming it failed; an empty page ends the pass. Page size bounds
   * query results, not consumer concurrency. Concurrent changes behind the cursor await another
   * pass; discovery does not reserve execution. Rows with null availability or an
   * availability at/before the cutoff are eligible, including expired leases with populated
   * owners. IDs retain the full signed-BIGINT range as text; never
   * round them through Number. Consumer-specific ID validation belongs after discovery.
   */
  async readPage(
    window: PostgresSweepWindow,
    lastSeenId: string | null,
  ): Promise<PostgresDiscoveryCandidate[]> {
    return await this.operations.readPage(window, lastSeenId);
  }

  /** Awaits owned pool shutdown. */
  async close(): Promise<void> {
    await this.pool.end();
  }
}
