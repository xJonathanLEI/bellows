import {
  type DispatchTask,
  dispatchEntry,
  dispatchIdentity,
  sameTask,
  type TaskIdentity,
  taskIdentity,
  trackingKey,
} from "./cloudflare/protocol.js";

export type { DispatchTask, TaskIdentity } from "./cloudflare/protocol.js";

const DISPATCHER_NAME = "global";
const DISPATCH_PATH = "https://dispatcher/dispatch";
const PROCESSOR_PATH = "https://processor/process";
const HEARTBEAT_INTERVAL_MS = 30_000;
const ATTEMPT_WATCHDOG_MS = 60_000;
const MAX_DATE_MS = 8_640_000_000_000_000;
const METADATA_KEY = "scheduler";
const TASK_PREFIX = "task:";
const MAX_ERROR_LENGTH = 500;

const JSON_HEADERS = {
  "cache-control": "no-store",
  "content-type": "application/json; charset=utf-8",
  "x-content-type-options": "nosniff",
};

export interface ProcessorFetcher {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

export interface DurableObjectStubLike extends ProcessorFetcher {}

export interface DurableObjectNamespaceLike {
  getByName(name: string): DurableObjectStubLike;
}

/** The subset shared by DurableObjectStorage and its transaction object. */
export interface DispatcherRecords {
  get<T>(key: string): Promise<T | undefined>;
  list<T>(options: { prefix: string }): Promise<Map<string, T>>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
}

/** Alarm changes on this storage participate in its SQLite transaction. */
export interface DispatcherStorage extends DispatcherRecords {
  transaction<T>(
    closure: (transaction: DispatcherRecords) => Promise<T>,
  ): Promise<T>;
  getAlarm(): Promise<number | null>;
  setAlarm(alarmTime: number): Promise<void>;
}

interface SchedulerMetadata {
  nextHeartbeatAtMs: number;
  nextAttemptId: number;
}

interface TaskRecord {
  task: TaskIdentity;
  nextAttemptAtMs: number;
  infrastructureFailures: number;
  state: { type: "pending" } | { type: "running"; attemptId: number };
}

type NextAction = { type: "done" } | { type: "retryAt"; atMs: number };

function timestamp(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= 0 &&
    value <= MAX_DATE_MS
  );
}

function counter(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("invalid dispatcher state or processor response");
  }
  return value as Record<string, unknown>;
}

function nextAction(text: string, task: TaskIdentity): NextAction {
  try {
    const body = object(JSON.parse(text));
    const action = object(body.nextAction);
    if (
      sameTask(taskIdentity(body.task), task) &&
      Object.keys(body).length === 2
    ) {
      if (action.type === "done" && Object.keys(action).length === 1) {
        return { type: "done" };
      }
      if (
        action.type === "retryAt" &&
        Object.keys(action).length === 2 &&
        timestamp(action.atMs)
      ) {
        return { type: "retryAt", atMs: action.atMs };
      }
    }
  } catch {
    // Do not expose JSON decoder excerpts from malformed successful responses.
  }
  throw new Error("invalid processor next action");
}

function retry(record: TaskRecord, now: number): void {
  record.infrastructureFailures = Math.min(
    record.infrastructureFailures + 1,
    6,
  );
  record.state = { type: "pending" };
  record.nextAttemptAtMs =
    now + Math.min(1_000 * 2 ** (record.infrastructureFailures - 1), 30_000);
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: JSON_HEADERS,
  });
}

function errorMessage(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return truncateText(message);
}

// Keep the UTF-16 length limit without splitting a Unicode surrogate pair.
function truncateText(text: string): string {
  let end = Math.min(text.length, MAX_ERROR_LENGTH);
  const last = text.charCodeAt(end - 1);
  const next = text.charCodeAt(end);
  if (last >= 0xd800 && last <= 0xdbff && next >= 0xdc00 && next <= 0xdfff) {
    end -= 1;
  }
  return text.slice(0, end);
}

async function parseDispatchRequest(request: Request): Promise<DispatchTask[]> {
  const contentType = request.headers.get("content-type") ?? "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new Error("request content-type must be application/json");
  }

  const body = object(await request.json());
  if (Object.keys(body).length !== 1 || !Array.isArray(body.tasks))
    throw new Error("request must contain a tasks array");
  return body.tasks.map(dispatchEntry);
}

async function consumeResponse(response: Response): Promise<string> {
  return await response.text();
}

/** Dispatches the definition's exact name and opaque ID, consuming the full response. */
export async function dispatchTask(
  namespace: DurableObjectNamespaceLike,
  taskName: string,
  taskId: string,
): Promise<void> {
  return dispatchTasks(namespace, [
    { task: { kind: "published", taskId, taskName }, intent: "run" },
  ]);
}

/** Validates the whole batch before contacting `global`; empty batches do no I/O. */
export async function dispatchTasks(
  namespace: DurableObjectNamespaceLike,
  tasks: readonly DispatchTask[],
): Promise<void> {
  const entries = tasks.map(dispatchEntry);
  if (entries.length === 0) return;
  const dispatcher = namespace.getByName(DISPATCHER_NAME);
  const response = await dispatcher.fetch(DISPATCH_PATH, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({ tasks: entries }),
  });
  const responseBody = await consumeResponse(response);

  if (!response.ok) {
    throw new Error(
      `task dispatcher returned HTTP ${response.status}: ${truncateText(responseBody)}`,
    );
  }
}

/**
 * One delegate per SQLite-backed Durable Object, using the named object `global`.
 * Dispatch validates the entire batch, launches in memory, then checks the warming alarm once,
 * without task writes. The alarm is set only when absent or later than now plus thirty seconds.
 * Active attempts deduplicate by published ID or singleton name through full response consumption
 * and result persistence. Pending published IDs can be redispatched with a corrected name.
 * Singleton `ensure` entries are suppressed only after this delegate establishes an invocation
 * chain. The in-memory optimization never suppresses `run` entries or alarms and is cleared on done.
 *
 * Only a successful, fully consumed, matching-identity `done` removes tracking. `retryAt` persists the
 * hint and resets infrastructure backoff; uncertain responses back off from one to thirty seconds.
 * Tasks with persisted hints survive reconstruction; unsaved uncertainty retries only in memory.
 * One alarm selects the earliest pending/60-second scheduled-attempt watchdog
 * deadline or independent 30-second heartbeat, launching all due identities without a Bellows concurrency
 * cap, subject to platform limits. Stale responses cannot overwrite newer attempts.
 *
 * PostgreSQL claims remain authoritative; renewed leases can move hints later. Watchdog supersession
 * only cancels transport on a best-effort basis, not business promises. Alarms and attempts are
 * at-least-once, not exactly-once side effects. Use `createPostgresSweeper` from `cloudflare/postgres`
 * in a scheduled Worker to recover missed invocations. Abrupt termination cannot guarantee cleanup.
 */
export class RetainedTaskDispatcher {
  private readonly inFlight = new Map<
    string,
    { record: TaskRecord; controller: AbortController; promise: Promise<void> }
  >();
  private readonly memoryRetries = new Map<string, TaskRecord>();
  // Bootstrap optimization only. Discovery and alarms never consult this set.
  private readonly singletons = new Set<string>();
  private bookkeeping: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly storage: DispatcherStorage,
    private readonly processor: ProcessorFetcher,
  ) {}

  private serialize<T>(work: () => Promise<T>): Promise<T> {
    const operation = this.bookkeeping.then(work);
    this.bookkeeping = operation.catch(() => undefined);
    return operation;
  }

  private ensureHeartbeat(): Promise<void> {
    return this.serialize(async () => {
      const deadline = Date.now() + HEARTBEAT_INTERVAL_MS;
      const alarm = await this.storage.getAlarm();
      if (alarm === null || alarm > deadline)
        await this.storage.setAlarm(deadline);
    });
  }

  // Only storage work is queued. No fetch or response-body read holds this queue.
  private update<T>(
    change: (
      metadata: SchedulerMetadata,
      records: Map<string, TaskRecord>,
      now: number,
    ) => T,
  ): Promise<T> {
    return this.serialize(() => this.updateStorage(change));
  }

  private updateStorage<T>(
    change: (
      metadata: SchedulerMetadata,
      records: Map<string, TaskRecord>,
      now: number,
    ) => T,
  ): Promise<T> {
    return this.storage.transaction(async (transaction) => {
      const now = Date.now();
      const raw = await transaction.get<unknown>(METADATA_KEY);
      const initialAlarm =
        raw === undefined ? await this.storage.getAlarm() : null;
      const stored = await transaction.list<TaskRecord>({
        prefix: TASK_PREFIX,
      });
      const metadata: SchedulerMetadata =
        raw === undefined
          ? {
              nextHeartbeatAtMs: Math.min(
                initialAlarm !== null && initialAlarm > now
                  ? initialAlarm
                  : now + HEARTBEAT_INTERVAL_MS,
                now + HEARTBEAT_INTERVAL_MS,
              ),
              nextAttemptId: 0,
            }
          : (object(raw) as unknown as SchedulerMetadata);
      if (
        (raw === undefined && stored.size !== 0) ||
        Object.keys(metadata).length !== 2 ||
        !timestamp(metadata.nextHeartbeatAtMs) ||
        !counter(metadata.nextAttemptId)
      ) {
        throw new Error("invalid dispatcher metadata");
      }
      const records = new Map<string, TaskRecord>();
      for (const [key, value] of stored) {
        const record = object(value) as unknown as TaskRecord;
        const state = object(record.state);
        if (
          key !== TASK_PREFIX + trackingKey(dispatchIdentity(record.task)) ||
          Object.keys(record).length !== 4 ||
          !timestamp(record.nextAttemptAtMs) ||
          !counter(record.infrastructureFailures) ||
          record.infrastructureFailures > 6 ||
          !(
            (state.type === "pending" && Object.keys(state).length === 1) ||
            (state.type === "running" &&
              Object.keys(state).length === 2 &&
              counter(state.attemptId) &&
              state.attemptId < metadata.nextAttemptId)
          )
        ) {
          throw new Error("invalid dispatcher task record");
        }
        records.set(trackingKey(record.task), structuredClone(record));
      }
      const result = change(metadata, records, now);
      let alarm = metadata.nextHeartbeatAtMs;
      for (const [id, record] of records) {
        alarm = Math.min(alarm, record.nextAttemptAtMs);
        if (
          JSON.stringify(stored.get(TASK_PREFIX + id)) !==
          JSON.stringify(record)
        ) {
          await transaction.put(TASK_PREFIX + id, record);
        }
      }
      for (const key of stored.keys()) {
        if (!records.has(key.slice(TASK_PREFIX.length))) {
          await transaction.delete(key);
        }
      }
      await transaction.put(METADATA_KEY, metadata);
      await this.storage.setAlarm(alarm);
      return result;
    });
  }

  private running(
    metadata: SchedulerMetadata,
    record: TaskRecord,
    now: number,
  ): TaskRecord {
    if (metadata.nextAttemptId >= Number.MAX_SAFE_INTEGER) {
      throw new Error("dispatcher attempt identifiers exhausted");
    }
    record.state = { type: "running", attemptId: metadata.nextAttemptId++ };
    record.nextAttemptAtMs = now + ATTEMPT_WATCHDOG_MS;
    return structuredClone(record);
  }

  private async runProcessor(
    record: TaskRecord,
    signal: AbortSignal,
  ): Promise<NextAction> {
    const { task } = record;
    const response = await this.processor.fetch(PROCESSOR_PATH, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ task }),
      signal,
    });
    const responseBody = await consumeResponse(response);

    if (!response.ok) {
      throw new Error(
        `task processor returned HTTP ${response.status}: ${truncateText(responseBody)}`,
      );
    }
    return nextAction(responseBody, task);
  }

  private launchProcessor(record: TaskRecord): void {
    const key = trackingKey(record.task);
    if (this.inFlight.has(key)) return;
    this.memoryRetries.delete(key);
    const controller = new AbortController();
    const promise = this.runProcessor(record, controller.signal)
      .then(
        (action) => action,
        (error: unknown) => {
          if (this.inFlight.get(key)?.record === record) {
            console.error(
              "task processor failed",
              record.task.kind,
              errorMessage(error),
            );
          }
          return undefined;
        },
      )
      .then((action) =>
        this.serialize(async () => {
          if (this.inFlight.get(key)?.record !== record) return;
          // A targeted read, only after the response, distinguishes an unsaved task from
          // an explicit redispatch of a schedule retained by an earlier delegate.
          if (
            action?.type !== "retryAt" &&
            (await this.storage.get(TASK_PREFIX + key)) === undefined
          ) {
            if (action === undefined && record.state.type === "pending")
              this.retryInMemory(record);
            if (action?.type === "done") this.forgetSingleton(record);
            return;
          }
          const applied = await this.updateStorage(
            (_metadata, records, now) => {
              const current = records.get(key);
              if (
                record.state.type === "running" &&
                (current?.state.type !== "running" ||
                  current.state.attemptId !== record.state.attemptId)
              )
                return false;
              if (action?.type === "done") {
                records.delete(key);
              } else if (action?.type === "retryAt") {
                records.set(key, {
                  task: record.task,
                  state: { type: "pending" },
                  nextAttemptAtMs: action.atMs,
                  infrastructureFailures: 0,
                });
              } else if (current) {
                retry(current, now);
              }
              return true;
            },
          );
          if (applied && action?.type === "done") this.forgetSingleton(record);
        }),
      )
      .catch((error: unknown) => {
        console.error(
          "task processor failed",
          record.task.kind,
          errorMessage(error),
        );
        if (this.inFlight.get(key)?.record === record)
          this.retryInMemory(record);
      })
      .finally(() => {
        if (this.inFlight.get(key)?.record === record)
          this.inFlight.delete(key);
      });
    this.inFlight.set(key, { record, controller, promise });
    if (record.task.kind === "singleton")
      this.singletons.add(record.task.taskName);
  }

  private forgetSingleton(record: TaskRecord): void {
    if (record.task.kind === "singleton")
      this.singletons.delete(record.task.taskName);
  }

  private retryInMemory(record: TaskRecord): void {
    const pending = structuredClone(record);
    retry(pending, Date.now());
    const key = trackingKey(record.task);
    this.memoryRetries.set(key, pending);
    setTimeout(
      () => {
        if (this.memoryRetries.get(key) === pending)
          this.launchProcessor(pending);
      },
      Math.max(0, pending.nextAttemptAtMs - Date.now()),
    );
  }

  private async dispatch(request: Request): Promise<Response> {
    const tasks = await parseDispatchRequest(request);
    const launched = new Set<string>();
    for (const { task, intent } of tasks) {
      if (intent === "ensure" && this.singletons.has(task.taskName)) continue;
      const key = trackingKey(task);
      if (launched.has(key)) continue;
      this.launchProcessor({
        task,
        nextAttemptAtMs: Date.now(),
        infrastructureFailures:
          this.memoryRetries.get(key)?.infrastructureFailures ?? 0,
        state: { type: "pending" },
      });
      launched.add(key);
    }
    if (tasks.length !== 0) await this.ensureHeartbeat();
    return jsonResponse({ ok: true });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== "POST" || url.pathname !== "/dispatch") {
      return jsonResponse({ error: "not-found", ok: false }, 404);
    }

    try {
      return await this.dispatch(request);
    } catch (error) {
      return jsonResponse({ error: errorMessage(error), ok: false }, 400);
    }
  }

  async alarm(): Promise<void> {
    try {
      const { launches, expired } = await this.update(
        (metadata, records, now) => {
          if (metadata.nextHeartbeatAtMs <= now)
            metadata.nextHeartbeatAtMs = now + HEARTBEAT_INTERVAL_MS;
          const launches: TaskRecord[] = [];
          const expired: { key: string; attemptId: number }[] = [];
          for (const record of records.values()) {
            if (record.nextAttemptAtMs > now) continue;
            const active = this.inFlight.get(trackingKey(record.task))?.record;
            if (active?.state.type === "pending") {
              // External dispatch has no durable attempt allocation. Keep its existing
              // schedule recoverable without racing its live invocation.
              record.nextAttemptAtMs = now + ATTEMPT_WATCHDOG_MS;
              continue;
            }
            if (record.state.type === "running") {
              expired.push({
                key: trackingKey(record.task),
                attemptId: record.state.attemptId,
              });
              retry(record, now);
            } else {
              launches.push(this.running(metadata, record, now));
            }
          }
          return { launches, expired };
        },
      );
      for (const { key, attemptId } of expired) {
        const active = this.inFlight.get(key);
        if (
          active?.record.state.type === "running" &&
          active.record.state.attemptId === attemptId
        ) {
          this.inFlight.delete(key);
          active.controller.abort();
        }
      }
      for (const record of launches) this.launchProcessor(record);
    } catch (error) {
      // Re-read within a transaction; never overwrite a concurrently established earlier alarm.
      await this.update((metadata, _records, now) => {
        metadata.nextHeartbeatAtMs = Math.min(
          metadata.nextHeartbeatAtMs,
          now + 1_000,
        );
      }).catch(() => undefined);
      throw error;
    }
  }
}
