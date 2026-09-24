import {
  type PostgresBackendOptions,
  PostgresDiscoveryBackend,
  type PostgresDiscoveryCandidate,
} from "../backends/postgres-discovery.js";
import {
  type DurableObjectNamespaceLike,
  dispatchTask,
} from "../cloudflare.js";

/** The entire selected schema must belong to the workload served by this dispatcher. */
export interface PostgresSweeperConfig extends PostgresBackendOptions {
  /** Obtain from this event's Hyperdrive binding, with query caching disabled. */
  readonly connectionString: string;
  readonly dispatcher: DurableObjectNamespaceLike;
}

/**
 * Settled candidate counts, partial on error. Accepted includes duplicate acknowledgements,
 * not completion or durable tracking.
 */
export interface PostgresSweepReport {
  readonly discovered: number;
  readonly accepted: number;
  readonly failed: number;
}

export type PostgresSweeperStage =
  | "configuration"
  | "acquisition"
  | "discovery"
  | "candidate"
  | "dispatch"
  | "backend-close";

/**
 * First failure with settled counts and any later close error.
 * Only the message is sanitized; do not automatically log causes or candidate identities.
 */
export class PostgresSweeperError extends Error {
  constructor(
    readonly stage: PostgresSweeperStage,
    cause: unknown,
    readonly report: PostgresSweepReport,
    readonly candidate?: PostgresDiscoveryCandidate,
    /** Present even when the later close threw `undefined`. */
    readonly backendCloseError?: { readonly cause: unknown },
  ) {
    super(`PostgreSQL sweeper failed at ${stage}`, { cause });
    this.name = "PostgresSweeperError";
  }
}

/**
 * Read-only best-effort recovery through the existing `global` dispatcher, without a registry.
 *
 * Each call owns a fresh backend and awaits dispatches and shutdown, including on errors.
 * Fixed database time and an upper ID bound each pass, not a snapshot. Pagination does not limit
 * dispatch concurrency; concurrent changes may await another sweep.
 *
 * Both methods work detached. `scheduled` rejects with stage-only diagnostics; inspect typed
 * errors through `sweep`. Register the Cron Trigger separately. Recovery latency and cleanup
 * after platform termination are not guaranteed.
 */
export function createPostgresSweeper<TEnv>(
  configure: (env: TEnv) => PostgresSweeperConfig,
): {
  sweep(env: TEnv): Promise<PostgresSweepReport>;
  scheduled(event: unknown, env: TEnv): Promise<void>;
} {
  const sweep = async (env: TEnv): Promise<PostgresSweepReport> => {
    const report = { discovered: 0, accepted: 0, failed: 0 };
    let config: PostgresSweeperConfig;
    let backend: PostgresDiscoveryBackend;
    let stage: PostgresSweeperStage = "configuration";
    try {
      config = configure(env);
      stage = "acquisition";
      backend = await PostgresDiscoveryBackend.connect(
        config.connectionString,
        {
          schema: config.schema,
        },
      );
    } catch (cause) {
      throw new PostgresSweeperError(stage, cause, Object.freeze(report));
    }

    let failure:
      | {
          stage: PostgresSweeperStage;
          cause: unknown;
          candidate?: PostgresDiscoveryCandidate;
        }
      | undefined;
    // Keep only live work, not one result (or response body) per task in the pass.
    const pending = new Set<Promise<void>>();
    try {
      const window = await backend.beginSweep();
      let cursor: string | null = null;
      while (window.upperId !== null) {
        const page = await backend.readPage(window, cursor);
        if (page.length === 0) break;
        report.discovered += page.length;
        for (const row of page) {
          cursor = row.taskId;
          const candidate = Object.freeze({ ...row });
          if (
            /^[1-9][0-9]{0,15}$/.exec(candidate.taskId)?.[0] !==
              candidate.taskId ||
            BigInt(candidate.taskId) > 9007199254740991n ||
            candidate.taskName.length === 0
          ) {
            report.failed++;
            failure ??= {
              stage: "candidate",
              cause: new Error("unsupported task identity"),
              candidate,
            };
            continue;
          }
          const settled = dispatchTask(
            config.dispatcher,
            candidate.taskName,
            candidate.taskId,
          ).then(
            () => {
              report.accepted++;
            },
            (cause: unknown) => {
              report.failed++;
              failure ??= { stage: "dispatch", cause, candidate };
            },
          );
          pending.add(settled);
          void settled.then(() => pending.delete(settled));
        }
      }
    } catch (cause) {
      failure ??= { stage: "discovery", cause };
    }

    let backendCloseError: { readonly cause: unknown } | undefined;
    try {
      await backend.close();
    } catch (cause) {
      if (failure) backendCloseError = Object.freeze({ cause });
      else failure = { stage: "backend-close", cause };
    }
    await Promise.all(pending);
    Object.freeze(report);
    if (failure) {
      throw new PostgresSweeperError(
        failure.stage,
        failure.cause,
        report,
        failure.candidate,
        backendCloseError,
      );
    }
    return report;
  };
  return {
    sweep,
    scheduled: async (_event, env) => {
      try {
        await sweep(env);
      } catch (error) {
        // Never attach raw driver/dispatch causes at the platform logging boundary.
        throw new Error(
          `PostgreSQL sweeper failed at ${
            error instanceof PostgresSweeperError
              ? error.stage
              : "configuration"
          }`,
        );
      }
    },
  };
}
