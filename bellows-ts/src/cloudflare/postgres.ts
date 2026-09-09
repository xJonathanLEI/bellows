import { PostgresExecutionBackend } from "../backends/postgres-execution.js";
import type { PostgresBackendOptions } from "../backends/postgres-operations.js";
import { runTaskOnce } from "../runtime.js";
import type { PublishTaskDefinition, WorkerFactory } from "../types.js";

/** A fresh scope for one validated processor request. */
export interface PostgresProcessorConfig<
  TTask extends PublishTaskDefinition<unknown, unknown>,
> extends PostgresBackendOptions {
  /** Obtain this URL from the request's Hyperdrive binding. */
  readonly connectionString: string;
  readonly factory: WorkerFactory<TTask>;
  /**
   * Awaited once after the attempt, including acquisition failure and no-claim paths.
   * Retain and drain any business promise that can outlive lease loss here.
   */
  readonly cleanup?: () => Promise<void>;
}

/**
 * Delegates POST `/process` for one published task definition.
 *
 * Configuration is synchronous and runs only after validation, once per request.
 * Connections are request-scoped; application cleanup and Bellows shutdown are awaited.
 * HTTP 200 means the attempt ended, not that the task succeeded. This does not cancel
 * arbitrary promises, extend request lifetime, or retry tasks.
 * If configuration throws before returning, it owns its partially created resources.
 *
 * Use as a default Worker export or delegate to `fetch` from a router; no `this` is required.
 */
export function createPostgresProcessor<
  TEnv,
  TTask extends PublishTaskDefinition<unknown, unknown>,
>(
  configure: (env: TEnv) => PostgresProcessorConfig<TTask>,
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

      let config: PostgresProcessorConfig<TTask> | undefined;
      let backend: PostgresExecutionBackend | undefined;
      let stage = "configuration";
      let failed = false;
      const failure = (stage: string) => {
        failed = true;
        // Never log configuration, driver errors, or request-supplied properties.
        console.error(`task processing attempt failed ${taskId} ${stage}`);
      };
      try {
        config = configure(env);
        stage = "worker-id";
        const workerId = randomWorkerId();
        stage = "acquisition";
        backend = await PostgresExecutionBackend.connect(
          config.connectionString,
          { schema: config.schema },
        );
        stage = "attempt";
        await runTaskOnce(backend, config.factory, workerId, {
          type: "task",
          taskId,
        });
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
        : jsonResponse({ taskId: body.taskId, attemptFinished: true }, 200);
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
