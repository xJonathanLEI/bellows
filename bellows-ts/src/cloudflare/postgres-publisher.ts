import {
  type PostgresBackendOptions,
  PostgresPublishedTaskIdError,
  PostgresPublishingBackend,
} from "../backends/postgres-publishing.js";
import {
  type DurableObjectNamespaceLike,
  dispatchTask,
} from "../cloudflare.js";
import type { PublishTaskDefinition, TaskPayload } from "../types.js";

/** Configuration for one immediate publication, using an existing schema. */
export interface PostgresPublisherConfig<
  TTask extends PublishTaskDefinition<unknown, unknown>,
> extends PostgresBackendOptions {
  /** Obtain this URL from the request's Hyperdrive binding. */
  readonly connectionString: string;
  readonly task: TTask;
  readonly dispatcher: DurableObjectNamespaceLike;
}

/** An exact published ID. Success confirms dispatch acceptance, not processing success. */
export interface PostgresPublisherReceipt {
  readonly taskId: string;
}

export type PostgresPublisherStage =
  | "configuration"
  | "acquisition"
  | "publication"
  | "task-id"
  | "backend-close"
  | "dispatch";

/**
 * The first lifecycle failure, with causes retained for deliberate inspection.
 *
 * A receipt means publication is known; close/dispatch failures can be recovered using that ID
 * without republishing. A `task-id` receipt is unsupported by the processor, not redispatchable.
 * No receipt on a publication failure does not establish rollback. Never retry blindly.
 */
export class PostgresPublisherError extends Error {
  constructor(
    readonly stage: PostgresPublisherStage,
    cause: unknown,
    readonly receipt?: PostgresPublisherReceipt,
    /** Present only when closing also failed, even if its thrown cause was `undefined`. */
    readonly backendCloseError?: { readonly cause: unknown },
  ) {
    super(`PostgreSQL publisher failed at ${stage}`, { cause });
    this.name = "PostgresPublisherError";
  }
}

/**
 * Publishes one task, awaits listener-free backend shutdown, then dispatches its exact ID.
 *
 * Construction performs no I/O. Synchronous configuration runs once per call; connections and
 * failures are never shared between calls. Callback-bearing definitions are plain publication,
 * without callback registration. Only canonical positive IDs up to 9007199254740991 can dispatch.
 * Success confirms acceptance, not completion. Inspect `PostgresPublisherError` before any generic
 * conversion; known publication failures retain their exact receipt.
 *
 * Await `publish` within your request, including when detached from this object. Normal error
 * paths await shutdown, but termination cannot guarantee cleanup. This does not extend request
 * lifetime, own an HTTP endpoint, retry publication, or make publication and dispatch atomic.
 * No future/awaitable publication, callback delivery, application cleanup hooks, transaction
 * participation, or durable recovery is provided.
 */
export function createPostgresPublisher<
  TEnv,
  TTask extends PublishTaskDefinition<unknown, unknown>,
>(
  configure: (env: TEnv) => PostgresPublisherConfig<TTask>,
): {
  publish(
    env: TEnv,
    payload: TaskPayload<TTask>,
  ): Promise<PostgresPublisherReceipt>;
} {
  return {
    publish: async (env, payload) => {
      let config: PostgresPublisherConfig<TTask>;
      let backend: PostgresPublishingBackend;
      let stage: PostgresPublisherStage = "configuration";
      try {
        config = configure(env);
        stage = "acquisition";
        backend = await PostgresPublishingBackend.connect(
          config.connectionString,
          {
            schema: config.schema,
          },
        );
      } catch (cause) {
        throw new PostgresPublisherError(stage, cause);
      }

      let receipt: PostgresPublisherReceipt | undefined;
      let failure:
        | { stage: PostgresPublisherStage; cause: unknown }
        | undefined;
      stage = "publication";
      try {
        const published = await backend.publish(config.task, payload);
        receipt = Object.freeze({ taskId: String(published.taskId) });
        stage = "task-id";
        if (
          !Number.isSafeInteger(published.taskId) ||
          /^[1-9][0-9]{0,15}$/.exec(receipt.taskId)?.[0] !== receipt.taskId
        ) {
          throw new Error("task ID must be a canonical positive safe integer");
        }
      } catch (cause) {
        if (cause instanceof PostgresPublishedTaskIdError) {
          receipt = Object.freeze({ taskId: cause.taskId });
          stage = "task-id";
        }
        failure = { stage, cause };
      }

      let backendCloseError: { readonly cause: unknown } | undefined;
      try {
        await backend.close();
      } catch (cause) {
        if (failure) {
          backendCloseError = Object.freeze({ cause });
        } else {
          failure = { stage: "backend-close", cause };
        }
      }
      if (failure) {
        throw new PostgresPublisherError(
          failure.stage,
          failure.cause,
          receipt,
          backendCloseError,
        );
      }
      // A successful publication always retained its receipt before closing.
      const published = receipt as PostgresPublisherReceipt;
      try {
        await dispatchTask(config.dispatcher, published.taskId);
      } catch (cause) {
        throw new PostgresPublisherError("dispatch", cause, published);
      }
      return published;
    },
  };
}
