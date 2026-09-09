const DISPATCHER_NAME = "global";
const DISPATCH_PATH = "https://dispatcher/dispatch";
const PROCESSOR_PATH = "https://processor/process";
const MAX_TASK_ID_LENGTH = 200;
const HEARTBEAT_INTERVAL_MS = 30_000;
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

export interface AlarmStorage {
  getAlarm(): Promise<number | null>;
  setAlarm(alarmTime: number): Promise<void>;
}

interface DispatchRequest {
  readonly taskId?: unknown;
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

async function parseDispatchRequest(request: Request): Promise<string> {
  const contentType = request.headers.get("content-type") ?? "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new Error("request content-type must be application/json");
  }

  const body = (await request.json()) as DispatchRequest;
  if (body === null || typeof body !== "object" || Array.isArray(body)) {
    throw new Error("request body must be a JSON object");
  }

  return parseTaskId(body.taskId);
}

async function consumeResponse(response: Response): Promise<string> {
  return await response.text();
}

export async function dispatchTask(
  namespace: DurableObjectNamespaceLike,
  taskId: string,
): Promise<void> {
  const validTaskId = parseTaskId(taskId);
  const dispatcher = namespace.getByName(DISPATCHER_NAME);
  const response = await dispatcher.fetch(DISPATCH_PATH, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({ taskId: validTaskId }),
  });
  const responseBody = await consumeResponse(response);

  if (!response.ok) {
    throw new Error(
      `task dispatcher returned HTTP ${response.status}: ${truncateText(responseBody)}`,
    );
  }
}

export class RetainedTaskDispatcher {
  private readonly inFlight = new Map<string, Promise<void>>();

  constructor(
    private readonly storage: AlarmStorage,
    private readonly processor: ProcessorFetcher,
  ) {}

  private async runProcessor(taskId: string): Promise<void> {
    const response = await this.processor.fetch(PROCESSOR_PATH, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ taskId }),
    });
    const responseBody = await consumeResponse(response);

    if (!response.ok) {
      throw new Error(
        `task processor returned HTTP ${response.status}: ${truncateText(responseBody)}`,
      );
    }
  }

  private launchProcessor(taskId: string): void {
    if (this.inFlight.has(taskId)) {
      return;
    }

    let retainedRuntime!: Promise<void>;
    retainedRuntime = this.runProcessor(taskId)
      .catch((error: unknown) => {
        console.error("task processor failed", taskId, errorMessage(error));
      })
      .finally(() => {
        if (this.inFlight.get(taskId) === retainedRuntime) {
          this.inFlight.delete(taskId);
        }
      });
    this.inFlight.set(taskId, retainedRuntime);
  }

  private async scheduleHeartbeat(): Promise<void> {
    const now = Date.now();
    const nextAlarmAtMs = now + HEARTBEAT_INTERVAL_MS;
    const currentAlarmAtMs = await this.storage.getAlarm();

    if (
      currentAlarmAtMs === null ||
      currentAlarmAtMs <= now ||
      currentAlarmAtMs > nextAlarmAtMs
    ) {
      await this.storage.setAlarm(nextAlarmAtMs);
    }
  }

  private async dispatch(request: Request): Promise<Response> {
    const taskId = await parseDispatchRequest(request);

    if (this.inFlight.has(taskId)) {
      await this.scheduleHeartbeat();
      return jsonResponse({ duplicate: true, ok: true, taskId });
    }

    this.launchProcessor(taskId);
    await this.scheduleHeartbeat();

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
    await this.storage.setAlarm(Date.now() + HEARTBEAT_INTERVAL_MS);
  }
}
