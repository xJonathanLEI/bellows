import { PostgresExecutionBackend } from "../backends/postgres-execution.js";
import type { PostgresBackendOptions } from "../backends/postgres-operations.js";
import { runTaskOnce, type TaskAttemptOutcome } from "../runtime.js";
import type { PublishTaskDefinition, WorkerFactory } from "../types.js";

export {
  createPostgresPublisher,
  type PostgresPublisherConfig,
  PostgresPublisherError,
  type PostgresPublisherReceipt,
  type PostgresPublisherStage,
} from "./postgres-publisher.js";
export {
  createPostgresSweeper,
  type PostgresSweeperConfig,
  PostgresSweeperError,
  type PostgresSweeperStage,
  type PostgresSweepReport,
} from "./postgres-sweeper.js";

const executeTask = Symbol("executeTask");

/** A typed published factory registered under its definition's exact name. */
export interface PostgresProcessorTask {
  readonly name: string;
  readonly [executeTask]: (
    backend: PostgresExecutionBackend,
    workerId: number,
    taskId: number,
  ) => Promise<TaskAttemptOutcome>;
}

export function createPostgresProcessorTask<
  TTask extends PublishTaskDefinition<unknown, unknown>,
>(factory: WorkerFactory<TTask>): PostgresProcessorTask {
  return {
    name: factory.task.name,
    [executeTask]: (backend, workerId, taskId) =>
      runTaskOnce(backend, factory, workerId, { type: "task", taskId }),
  };
}

/** A fresh scope for one validated processor request. */
export interface PostgresProcessorConfig extends PostgresBackendOptions {
  /** Obtain this URL from the request's Hyperdrive binding. */
  readonly connectionString: string;
  /** Non-empty registrations with unique, non-empty definition names. */
  readonly tasks: readonly PostgresProcessorTask[];
  /**
   * Awaited once whenever configuration returned, including invalid registries and unknown names.
   * Retain and drain any business promise that can outlive lease loss here.
   */
  readonly cleanup?: () => Promise<void>;
}

/**
 * Delegates POST `/process` with `{ taskId, taskName }` to typed published factories.
 *
 * Unknown names return 404 without acquisition; claims still check the persisted definition name.
 * Configuration is synchronous and runs only after validation, once per request.
 * Connections are request-scoped; application cleanup and Bellows shutdown are awaited.
 * HTTP 200 reports `nextAction`: `done` or `retryAt` with absolute Unix `atMs`, not business success.
 * Uncertain runtime outcomes return a sanitized HTTP 500. This does not cancel
 * arbitrary promises, extend request lifetime, or retry tasks.
 * If configuration throws before returning, it owns its partially created resources.
 *
 * Use as a default Worker export or delegate to `fetch` from a router; no `this` is required.
 */
export function createPostgresProcessor<TEnv>(
  configure: (env: TEnv) => PostgresProcessorConfig,
): {
  fetch(request: Request, env: TEnv): Promise<Response>;
} {
  return {
    fetch: async (request, env) => {
      if (new URL(request.url).pathname !== "/process") {
        return errorResponse(404, "not-found");
      }
      if (request.method !== "POST") {
        const response = errorResponse(405, "method-not-allowed");
        response.headers.set("allow", "POST");
        return response;
      }
      if (
        !request.headers
          .get("content-type")
          ?.toLowerCase()
          .includes("application/json")
      ) {
        return errorResponse(415, "content-type must be application/json");
      }
      let body: unknown;
      try {
        body = await request.json();
      } catch {
        return errorResponse(400, "invalid JSON");
      }
      if (
        body === null ||
        typeof body !== "object" ||
        Array.isArray(body) ||
        !("taskId" in body) ||
        typeof body.taskId !== "string" ||
        // `$` also matches before a final newline; require the complete string.
        /^[1-9][0-9]{0,15}$/.exec(body.taskId)?.[0] !== body.taskId
      ) {
        return errorResponse(
          400,
          "taskId must be a canonical positive decimal string",
        );
      }
      const taskId = Number(body.taskId);
      if (!Number.isSafeInteger(taskId) || String(taskId) !== body.taskId) {
        return errorResponse(
          400,
          "taskId must encode a positive safe integer canonically",
        );
      }
      if (
        !("taskName" in body) ||
        typeof body.taskName !== "string" ||
        body.taskName.length === 0
      ) {
        return errorResponse(400, "taskName must be a non-empty string");
      }

      let config: PostgresProcessorConfig | undefined;
      let backend: PostgresExecutionBackend | undefined;
      let unknownName = false;
      let stage = "configuration";
      let failed = false;
      let nextAction:
        | { type: "done" }
        | { type: "retryAt"; atMs: number }
        | undefined;
      const failure = (stage: string) => {
        failed = true;
        // Never log configuration, driver errors, or request-supplied properties.
        console.error(`task processing attempt failed ${taskId} ${stage}`);
      };
      try {
        config = configure(env);
        const tasks = new Map<string, PostgresProcessorTask>();
        if (config.tasks.length === 0) throw new Error("empty task registry");
        for (const task of config.tasks) {
          if (
            typeof task.name !== "string" ||
            task.name.length === 0 ||
            tasks.has(task.name)
          ) {
            throw new Error("invalid task registry");
          }
          tasks.set(task.name, task);
        }
        const task = tasks.get(body.taskName);
        if (!task) {
          unknownName = true;
        } else {
          stage = "worker-id";
          const workerId = randomWorkerId();
          stage = "acquisition";
          backend = await PostgresExecutionBackend.connect(
            config.connectionString,
            { schema: config.schema },
          );
          stage = "attempt";
          const outcome = await task[executeTask](backend, workerId, taskId);
          switch (outcome.type) {
            case "done":
              nextAction = { type: "done" };
              break;
            case "retryAt": {
              const atMs = outcome.availableFromMs;
              if (
                !Number.isSafeInteger(atMs) ||
                atMs < 0 ||
                atMs > 8_640_000_000_000_000
              ) {
                throw new Error("invalid scheduling deadline");
              }
              nextAction = { type: "retryAt", atMs };
              break;
            }
            case "retry":
              failure("attempt");
              break;
          }
        }
      } catch {
        failure(stage);
      } finally {
        try {
          await config?.cleanup?.();
        } catch {
          failure("application-cleanup");
        } finally {
          try {
            await backend?.close();
          } catch {
            failure("backend-close");
          }
        }
      }
      return failed
        ? errorResponse(500, "task processing attempt failed")
        : unknownName
          ? errorResponse(404, "unknown task name")
          : jsonResponse({ taskId: body.taskId, nextAction }, 200);
    },
  };
}

function randomWorkerId(): number {
  const bytes = new Uint8Array(6);
  while (true) {
    crypto.getRandomValues(bytes);
    const value = bytes.reduce((value, byte) => value * 256 + byte, 0);
    if (value !== 0) {
      return value;
    }
  }
}

function errorResponse(status: number, error: string): Response {
  return jsonResponse({ error }, status);
}

function jsonResponse(body: unknown, status: number): Response {
  return Response.json(body, {
    status,
    headers: {
      "cache-control": "no-store",
      "x-content-type-options": "nosniff",
      "content-type": "application/json; charset=utf-8",
    },
  });
}
