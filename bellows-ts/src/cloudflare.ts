const DISPATCHER_NAME = "global";
const DISPATCH_PATH = "https://dispatcher/dispatch";
const PROCESSOR_PATH = "https://processor/process";
const MAX_TASK_ID_LENGTH = 200;
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
  taskId: string;
  taskName: string;
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

function nextAction(text: string, taskId: string): NextAction {
  try {
    const body = object(JSON.parse(text));
    const action = object(body.nextAction);
    if (body.taskId === taskId && Object.keys(body).length === 2) {
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

interface DispatchRequest {
  readonly taskId?: unknown;
  readonly taskName?: unknown;
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

function parseTaskId(value: unknown): string {
  if (
    typeof value !== "string" ||
    value.length < 1 ||
    value.length > MAX_TASK_ID_LENGTH
  ) {
    throw new Error(
      `taskId must be a non-empty string no longer than ${MAX_TASK_ID_LENGTH} characters`,
    );
  }

  return value;
}

function parseTaskName(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("taskName must be a non-empty string");
  }
  return value;
}

async function parseDispatchRequest(
  request: Request,
): Promise<{ taskId: string; taskName: string }> {
  const contentType = request.headers.get("content-type") ?? "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new Error("request content-type must be application/json");
  }

  const body = (await request.json()) as DispatchRequest;
  if (body === null || typeof body !== "object" || Array.isArray(body)) {
    throw new Error("request body must be a JSON object");
  }

  return {
    taskId: parseTaskId(body.taskId),
    taskName: parseTaskName(body.taskName),
  };
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
  const validTaskId = parseTaskId(taskId);
  const validTaskName = parseTaskName(taskName);
  const dispatcher = namespace.getByName(DISPATCHER_NAME);
  const response = await dispatcher.fetch(DISPATCH_PATH, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({ taskId: validTaskId, taskName: validTaskName }),
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
 * Dispatch launches in memory before checking/repairing the warming alarm, without task writes.
 * The alarm is set only when absent or later than now plus thirty seconds. Active same-ID duplicates cannot
 * change routing and stay deduplicated through full body consumption and result persistence.
 * Pending IDs can be explicitly redispatched immediately with a corrected name.
 *
 * Only a successful, fully consumed, matching-ID `done` removes tracking. `retryAt` persists the
 * hint and resets infrastructure backoff; uncertain responses back off from one to thirty seconds.
 * Tasks with persisted hints survive reconstruction; unsaved uncertainty retries only in memory.
 * One alarm selects the earliest pending/60-second scheduled-attempt watchdog
 * deadline or independent 30-second heartbeat, launching all due IDs without a Bellows concurrency
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
          key !== TASK_PREFIX + parseTaskId(record.taskId) ||
          Object.keys(record).length !== 5 ||
          !parseTaskName(record.taskName) ||
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
        records.set(record.taskId, structuredClone(record));
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
    const { taskName, taskId } = record;
    const response = await this.processor.fetch(PROCESSOR_PATH, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ taskId, taskName }),
      signal,
    });
    const responseBody = await consumeResponse(response);

    if (!response.ok) {
      throw new Error(
        `task processor returned HTTP ${response.status}: ${truncateText(responseBody)}`,
      );
    }
    return nextAction(responseBody, taskId);
  }

  private launchProcessor(record: TaskRecord): void {
    const { taskId } = record;
    if (this.inFlight.has(taskId)) return;
    this.memoryRetries.delete(taskId);
    const controller = new AbortController();
    const promise = this.runProcessor(record, controller.signal)
      .then(
        (action) => action,
        (error: unknown) => {
          if (this.inFlight.get(taskId)?.record === record) {
            console.error("task processor failed", taskId, errorMessage(error));
          }
          return undefined;
        },
      )
      .then((action) =>
        this.serialize(async () => {
          if (this.inFlight.get(taskId)?.record !== record) return;
          // A targeted read, only after the response, distinguishes an unsaved task from
          // an explicit redispatch of a schedule retained by an earlier delegate.
          if (
            action?.type !== "retryAt" &&
            (await this.storage.get(TASK_PREFIX + taskId)) === undefined
          ) {
            if (action === undefined && record.state.type === "pending")
              this.retryInMemory(record);
            return;
          }
          await this.updateStorage((_metadata, records, now) => {
            const current = records.get(taskId);
            if (
              record.state.type === "running" &&
              (current?.state.type !== "running" ||
                current.state.attemptId !== record.state.attemptId)
            )
              return;
            if (action?.type === "done") {
              records.delete(taskId);
            } else if (action?.type === "retryAt") {
              records.set(taskId, {
                taskId,
                taskName: record.taskName,
                state: { type: "pending" },
                nextAttemptAtMs: action.atMs,
                infrastructureFailures: 0,
              });
            } else if (current) {
              retry(current, now);
            }
          });
        }),
      )
      .catch((error: unknown) => {
        console.error("task processor failed", taskId, errorMessage(error));
        if (this.inFlight.get(taskId)?.record === record)
          this.retryInMemory(record);
      })
      .finally(() => {
        if (this.inFlight.get(taskId)?.record === record)
          this.inFlight.delete(taskId);
      });
    this.inFlight.set(taskId, { record, controller, promise });
  }

  private retryInMemory(record: TaskRecord): void {
    const pending = structuredClone(record);
    retry(pending, Date.now());
    this.memoryRetries.set(record.taskId, pending);
    setTimeout(
      () => {
        if (this.memoryRetries.get(record.taskId) === pending)
          this.launchProcessor(pending);
      },
      Math.max(0, pending.nextAttemptAtMs - Date.now()),
    );
  }

  private async dispatch(request: Request): Promise<Response> {
    const { taskId, taskName } = await parseDispatchRequest(request);

    const duplicate = this.inFlight.has(taskId);
    if (!duplicate) {
      this.launchProcessor({
        taskId,
        taskName,
        nextAttemptAtMs: Date.now(),
        infrastructureFailures:
          this.memoryRetries.get(taskId)?.infrastructureFailures ?? 0,
        state: { type: "pending" },
      });
    }
    await this.ensureHeartbeat();
    if (duplicate) {
      return jsonResponse({ duplicate: true, ok: true, taskId });
    }
    return jsonResponse({ ok: true, taskId });
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
          const expired: { taskId: string; attemptId: number }[] = [];
          for (const record of records.values()) {
            if (record.nextAttemptAtMs > now) continue;
            const active = this.inFlight.get(record.taskId)?.record;
            if (active?.state.type === "pending") {
              // External dispatch has no durable attempt allocation. Keep its existing
              // schedule recoverable without racing its live invocation.
              record.nextAttemptAtMs = now + ATTEMPT_WATCHDOG_MS;
              continue;
            }
            if (record.state.type === "running") {
              expired.push({
                taskId: record.taskId,
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
      for (const { taskId, attemptId } of expired) {
        const active = this.inFlight.get(taskId);
        if (
          active?.record.state.type === "running" &&
          active.record.state.attemptId === attemptId
        ) {
          this.inFlight.delete(taskId);
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
