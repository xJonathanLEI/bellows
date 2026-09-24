import {
  type PostgresBackendOptions,
  PostgresDiscoveryBackend,
  type PostgresDiscoveryCandidate,
} from "../backends/postgres-discovery.js";
import {
  type DispatchTask,
  type DurableObjectNamespaceLike,
  dispatchTasks,
} from "../cloudflare.js";
import type { SingletonTaskDefinition } from "../types.js";
import { dispatchIdentity } from "./protocol.js";

/** The entire selected schema must belong to the workload served by this dispatcher. */
export interface PostgresSweeperConfig extends PostgresBackendOptions {
  /** Obtain from this event's Hyperdrive binding, with query caching disabled. */
  readonly connectionString: string;
  readonly dispatcher: DurableObjectNamespaceLike;
  /** Definitions to bootstrap after discovery; never a discovery filter. */
  readonly singletons?: readonly SingletonTaskDefinition<unknown>[];
}

/**
 * Settled candidate counts, partial on error. Accepted includes duplicate acknowledgements,
 * not completion or durable tracking. Discovered + bootstrapCandidates = accepted + failed.
 */
export interface PostgresSweepReport {
  readonly discovered: number;
  /** Ensure entries submitted, not singleton executions or creations. */
  readonly bootstrapCandidates: number;
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

/** Exact database identity or a name-only bootstrap entry. */
export type PostgresSweepCandidate =
  | (PostgresDiscoveryCandidate & { readonly source: "discovery" })
  | { readonly source: "bootstrap"; readonly taskName: string };

/**
 * First failure with settled counts and any later close error.
 * Only the message is sanitized; do not automatically log causes or candidate identities.
 */
export class PostgresSweeperError extends Error {
  constructor(
    readonly stage: PostgresSweeperStage,
    cause: unknown,
    readonly report: PostgresSweepReport,
    /** At most 100 entries from the failed batch; one for candidate validation failures. */
    readonly candidates: readonly PostgresSweepCandidate[] = [],
    /** Present even when the later close threw `undefined`. */
    readonly backendCloseError?: { readonly cause: unknown },
  ) {
    super(`PostgreSQL sweeper failed at ${stage}`, { cause });
    this.name = "PostgresSweeperError";
  }
}

/**
 * Read-only recovery of both task kinds through `global`, plus optional singleton bootstrap.
 *
 * Each call owns a fresh backend and awaits dispatches and shutdown, including on errors.
 * Fixed database time and an upper ID bound each pass, not a snapshot. Pagination does not limit
 * dispatch concurrency; concurrent changes may await another sweep. After successful discovery,
 * configured singletons not submitted in page batches receive one bulk ensure request.
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
    const report = {
      discovered: 0,
      bootstrapCandidates: 0,
      accepted: 0,
      failed: 0,
    };
    let config: PostgresSweeperConfig;
    let singletons: string[];
    let backend: PostgresDiscoveryBackend;
    let stage: PostgresSweeperStage = "configuration";
    try {
      config = configure(env);
      const names = new Set<string>();
      singletons = (config.singletons ?? []).map((definition) => {
        if (!definition || definition.kind !== "singleton")
          throw new Error("invalid singleton definition");
        const task = dispatchIdentity({
          kind: "singleton",
          taskName: definition.name,
        });
        if (names.has(task.taskName))
          throw new Error("duplicate singleton definition");
        names.add(task.taskName);
        return task.taskName;
      });
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
          candidates?: readonly PostgresSweepCandidate[];
        }
      | undefined;
    // Keep only live work, not one result (or response body) per task in the pass.
    const pending = new Set<Promise<void>>();
    const submitted = new Set<string>();
    const submit = (
      tasks: DispatchTask[],
      candidates: PostgresSweepCandidate[],
    ) => {
      if (tasks.length === 0) return;
      const count = tasks.length;
      const context = Object.freeze(candidates.slice(0, 100));
      const settled = dispatchTasks(config.dispatcher, tasks).then(
        () => {
          report.accepted += count;
        },
        (cause: unknown) => {
          report.failed += count;
          failure ??= { stage: "dispatch", cause, candidates: context };
        },
      );
      pending.add(settled);
      void settled.then(() => pending.delete(settled));
    };
    try {
      const window = await backend.beginSweep();
      let cursor: string | null = null;
      while (window.upperId !== null) {
        const page = await backend.readPage(window, cursor);
        if (page.length === 0) break;
        report.discovered += page.length;
        const tasks: DispatchTask[] = [];
        const candidates: PostgresSweepCandidate[] = [];
        for (const row of page) {
          cursor = row.taskId;
          const candidate = Object.freeze({
            ...row,
            source: "discovery" as const,
          });
          try {
            if (
              !candidate.isSingleton &&
              (/^[1-9][0-9]{0,15}$/.exec(candidate.taskId)?.[0] !==
                candidate.taskId ||
                BigInt(candidate.taskId) > 9007199254740991n)
            )
              throw new Error("unsupported task identity");
            const task = dispatchIdentity(
              candidate.isSingleton
                ? { kind: "singleton", taskName: candidate.taskName }
                : {
                    kind: "published",
                    taskId: candidate.taskId,
                    taskName: candidate.taskName,
                  },
            );
            tasks.push({ task, intent: "run" });
            candidates.push(candidate);
            if (task.kind === "singleton") submitted.add(task.taskName);
          } catch (cause) {
            report.failed++;
            failure ??= {
              stage: "candidate",
              cause,
              candidates: Object.freeze([candidate]),
            };
          }
        }
        submit(tasks, candidates);
      }
      const bootstrap = singletons.filter((name) => !submitted.has(name));
      report.bootstrapCandidates += bootstrap.length;
      submit(
        bootstrap.map((taskName) => ({
          task: { kind: "singleton", taskName },
          intent: "ensure",
        })),
        bootstrap.map((taskName) =>
          Object.freeze({ source: "bootstrap", taskName }),
        ),
      );
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
        failure.candidates,
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
