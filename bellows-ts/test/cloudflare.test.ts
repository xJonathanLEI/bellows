import { afterEach, expect, test, vi } from "vitest";
import {
  type DispatcherRecords,
  type DispatcherStorage,
  type DispatchTask,
  type DurableObjectNamespaceLike,
  type DurableObjectStubLike,
  dispatchTask,
  dispatchTasks,
  type ProcessorFetcher,
  RetainedTaskDispatcher,
  type TaskIdentity,
} from "../src/cloudflare.js";

class Deferred<T> {
  readonly promise: Promise<T>;
  private resolvePromise!: (value: T) => void;
  private rejectPromise!: (error: Error) => void;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolvePromise = resolve;
      this.rejectPromise = reject;
    });
  }

  resolve(value: T): void {
    this.resolvePromise(value);
  }

  reject(error: Error): void {
    this.rejectPromise(error);
  }
}

function singleton(
  taskName = "7",
  intent: "run" | "ensure" = "ensure",
): DispatchTask {
  return { task: { kind: "singleton", taskName }, intent };
}

function singletonRecord(
  storage: FakeAlarmStorage,
  name = "7",
): StoredTask | undefined {
  return storage.records.get(`task:singleton:${name}`) as
    | StoredTask
    | undefined;
}

test.each([
  { task: { kind: "published", taskName: "7", taskId: "" }, intent: "run" },
  {
    task: { kind: "published", taskName: "7", taskId: `${"😀".repeat(100)}a` },
    intent: "run",
  },
  { task: { kind: "singleton", taskName: null }, intent: "ensure" },
  { task: { kind: "singleton", taskName: "7", taskId: "7" }, intent: "run" },
  { task: { kind: "singleton", taskName: "7", extra: true }, intent: "run" },
  { task: { kind: "publish", taskName: "7", taskId: "7" }, intent: "run" },
  { task: { kind: "published", taskName: "7" }, intent: "run" },
  { task: { kind: "published", taskName: "7", taskId: "7" }, intent: "ensure" },
  { task: { kind: "singleton", taskName: "" }, intent: "run" },
  { task: { kind: "singleton", taskName: "7" }, intent: "unknown" },
  { task: { kind: "singleton", taskName: "7" } },
  { ...singleton(), extra: true },
  singleton("😀".repeat(509)),
  singleton("\ud800"),
])("validates the entire batch before side effects: %j", async (invalid) => {
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  const alarm = vi.spyOn(storage, "getAlarm");
  const transaction = vi.spyOn(storage, "transaction");
  expect(
    (await dispatcher.fetch(batchRequest([singleton(), invalid]))).status,
  ).toBe(400);
  expect(processor.calls).toHaveLength(0);
  expect(alarm).not.toHaveBeenCalled();
  expect(transaction).not.toHaveBeenCalled();
  expect(storage.records.size).toBe(0);
});

test("immediate completions still dispatch each batch identity only once", async () => {
  const storage = new FakeAlarmStorage();
  const fetch = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) =>
    Response.json({
      task: JSON.parse(String(init?.body)).task,
      nextAction: { type: "done" },
    }),
  );
  const dispatcher = new RetainedTaskDispatcher(storage, { fetch });
  const entries = [
    singleton(),
    {
      task: { kind: "published", taskId: "7", taskName: "published" },
      intent: "run",
    },
  ];
  await dispatcher.fetch(
    batchRequest(Array.from({ length: 1000 }, () => entries).flat()),
  );
  expect(fetch).toHaveBeenCalledTimes(2);
});

test("batch helpers validate before lookup and empty batches do no I/O", async () => {
  const namespace = new RecordingNamespace();
  await dispatchTasks(namespace, []);
  expect(namespace.names).toEqual([]);
  await expect(
    dispatchTasks(namespace, [singleton(), singleton("x".repeat(2034))]),
  ).rejects.toThrow("byte limit");
  expect(namespace.names).toEqual([]);
  const tasks = [
    singleton(`${"😀".repeat(508)}x`),
    singleton(" published:7 雪\n"),
  ];
  await dispatchTasks(namespace, tasks);
  expect(namespace.names).toEqual(["global"]);
  expect(await namespace.stub.requests[0]?.json()).toEqual({ tasks });
  const storage = new FakeAlarmStorage();
  const alarm = vi.spyOn(storage, "getAlarm");
  await expectJson(
    await new RetainedTaskDispatcher(storage, new DeferredProcessor()).fetch(
      batchRequest([]),
    ),
    200,
    { ok: true },
  );
  expect(alarm).not.toHaveBeenCalled();
});

test("all 300 distinct mixed entries launch before a single blocked warming check", async () => {
  const storage = new FakeAlarmStorage();
  storage.getGate = new Deferred<void>();
  const gate = storage.getGate;
  const alarm = vi.spyOn(storage, "getAlarm");
  const transaction = vi.spyOn(storage, "transaction");
  const get = vi.spyOn(storage, "get");
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  const tasks: DispatchTask[] = Array.from({ length: 300 }, (_, i) =>
    i % 2 === 0
      ? singleton(String(i / 2))
      : {
          task: {
            kind: "published",
            taskId: String((i - 1) / 2),
            taskName: "published",
          },
          intent: "run",
        },
  );
  const acceptance = dispatcher.fetch(batchRequest([...tasks, ...tasks]));
  await waitFor(() => processor.calls.length === 300);
  expect(alarm).toHaveBeenCalledTimes(1);
  expect(transaction).not.toHaveBeenCalled();
  expect(get).not.toHaveBeenCalled();
  expect(storage.records.size).toBe(0);
  gate.resolve(undefined);
  await expectJson(await acceptance, 200, { ok: true });
  expect(storage.scheduledAlarms).toHaveLength(1);
  for (let i = 0; i < 300; i++) {
    reply(processor, i, { type: "done" });
    await waitFor(() => get.mock.calls.length === i + 1);
  }
});

test("singleton bootstrap suppression never suppresses run, and current done forgets it", async () => {
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(batchRequest([singleton(), singleton()]));
  expect(processor.calls).toHaveLength(1);
  const body = controlledResponse();
  processor.calls[0]?.response.resolve(body.response);
  await waitFor(() => body.response.bodyUsed);
  await dispatcher.fetch(batchRequest([singleton(), singleton("7", "run")]));
  expect(processor.calls).toHaveLength(1);
  body.finish(
    JSON.stringify({
      task: singleton().task,
      nextAction: { type: "retryAt", atMs: Date.now() + 60_000 },
    }),
  );
  await body.consumed;
  await waitFor(() => singletonRecord(storage)?.state.type === "pending");
  const alarm = vi.spyOn(storage, "getAlarm");
  await dispatcher.fetch(batchRequest([singleton(), singleton()]));
  expect(alarm).toHaveBeenCalledTimes(1);
  expect(processor.calls).toHaveLength(1);
  await dispatcher.fetch(batchRequest([singleton(), singleton("7", "run")]));
  expect(processor.calls).toHaveLength(2);
  reply(processor, 1, { type: "done" });
  await waitFor(() => !singletonRecord(storage));
  for (let i = 0; i < 100 && processor.calls.length < 3; i++)
    await dispatcher.fetch(batchRequest([singleton()]));
  expect(processor.calls).toHaveLength(3);
  await finish(processor, 2);
  // Unsaved done also releases suppression.
  for (let i = 0; i < 100 && processor.calls.length < 4; i++)
    await dispatcher.fetch(batchRequest([singleton()]));
  expect(processor.calls).toHaveLength(4);
  await finish(processor, 3);
});

test.each([
  "run",
  "ensure",
] as const)("singleton %s chains survive reconstruction and alarm launches establish suppression", async (intent) => {
  let now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  let dispatcher = new RetainedTaskDispatcher(storage, processor);
  const name = `singleton: published:7 雪🦀 ${"x".repeat(300)}`;
  await dispatcher.fetch(batchRequest([singleton(name, intent)]));
  reply(processor, 0, { type: "retryAt", atMs: now + 10_000 });
  await waitFor(() => singletonRecord(storage, name)?.state.type === "pending");
  expect(singletonRecord(storage, name)?.task).toEqual(singleton(name).task);
  await dispatcher.fetch(batchRequest([singleton(name)]));
  expect(processor.calls).toHaveLength(1);
  dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(batchRequest([singleton(name)]));
  expect(processor.calls).toHaveLength(2);
  reply(processor, 1, { type: "retryAt", atMs: now + 10_001 });
  await waitFor(
    () => singletonRecord(storage, name)?.nextAttemptAtMs === now + 10_001,
  );
  dispatcher = new RetainedTaskDispatcher(storage, processor);
  now += 10_001;
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(3);
  await dispatcher.fetch(batchRequest([singleton(name)]));
  expect(processor.calls).toHaveLength(3);
  reply(processor, 2, { type: "retryAt", atMs: now + 60_000 });
  await waitFor(() => singletonRecord(storage, name)?.state.type === "pending");
  await dispatcher.fetch(batchRequest([singleton(name)]));
  expect(processor.calls).toHaveLength(3);
});

test("singleton infrastructure retries retain bootstrap suppression and reset on past hints", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(1_700_000_000_000);
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  let dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(batchRequest([singleton()]));
  processor.calls[0]?.response.reject(new Error("network"));
  await vi.advanceTimersByTimeAsync(0);
  await dispatcher.fetch(batchRequest([singleton()]));
  expect(processor.calls).toHaveLength(1);
  expect(storage.records.size).toBe(0);
  await vi.advanceTimersByTimeAsync(1_000);
  expect(processor.calls).toHaveLength(2);
  reply(processor, 1, { type: "retryAt", atMs: Date.now() });
  await waitFor(() => singletonRecord(storage)?.state.type === "pending");
  for (let failures = 1; failures <= 7; failures++) {
    dispatcher = new RetainedTaskDispatcher(storage, processor);
    await dispatcher.alarm();
    processor.calls.at(-1)?.response.reject(new Error("network"));
    await vi.advanceTimersByTimeAsync(0);
    const record = singletonRecord(storage);
    if (!record) throw new Error("missing singleton schedule");
    expect(record.infrastructureFailures).toBe(Math.min(failures, 6));
    const delay = Math.min(1_000 * 2 ** (failures - 1), 30_000);
    expect(record.nextAttemptAtMs).toBe(Date.now() + delay);
    const count = processor.calls.length;
    await dispatcher.fetch(batchRequest([singleton()]));
    expect(processor.calls).toHaveLength(count);
    await vi.advanceTimersByTimeAsync(delay);
  }
  await dispatcher.alarm();
  reply(processor, processor.calls.length - 1, {
    type: "retryAt",
    atMs: Date.now() - 1,
  });
  await waitFor(() => singletonRecord(storage)?.infrastructureFailures === 0);
});

test.each([
  "done",
  "retryAt",
  "error",
])("stale singleton %s cannot replace or forget a newer chain", async (action) => {
  let now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(batchRequest([singleton()]));
  reply(processor, 0, { type: "retryAt", atMs: now });
  await waitFor(() => singletonRecord(storage)?.state.type === "pending");
  await dispatcher.alarm();
  const body = controlledResponse();
  processor.calls[1]?.response.resolve(body.response);
  await waitFor(() => body.response.bodyUsed);
  now += 60_000;
  await dispatcher.alarm();
  now += 1_000;
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(3);
  reply(processor, 2, { type: "retryAt", atMs: now + 50_000 });
  await waitFor(() => singletonRecord(storage)?.state.type === "pending");
  const saved = structuredClone(singletonRecord(storage));
  if (action === "error") body.fail(new Error("late error"));
  else
    body.finish(
      JSON.stringify({
        task: singleton().task,
        nextAction:
          action === "done" ? { type: "done" } : { type: "retryAt", atMs: 0 },
      }),
    );
  await body.consumed;
  await dispatcher.fetch(batchRequest([singleton()]));
  expect(processor.calls).toHaveLength(3);
  expect(singletonRecord(storage)).toEqual(saved);
});

test("a singleton record under another identity's storage key is rejected", async () => {
  const storage = new FakeAlarmStorage();
  seedSchedule(storage);
  const record = storedTask(storage);
  if (!record) throw new Error("missing schedule");
  record.task = singleton().task;
  const processor = new DeferredProcessor();
  await expect(
    new RetainedTaskDispatcher(storage, processor).alarm(),
  ).rejects.toThrow("task record");
  expect(processor.calls).toHaveLength(0);
});

test.each([
  false,
  true,
])("singleton persistence failure preserves a retry chain (uncertain=%s)", async (uncertain) => {
  vi.useFakeTimers();
  vi.setSystemTime(1_700_000_000_000);
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(batchRequest([singleton()]));
  if (uncertain) storage.uncertainCommit = true;
  else storage.setFailures = 1;
  reply(processor, 0, { type: "retryAt", atMs: Date.now() });
  await vi.advanceTimersByTimeAsync(0);
  expect(singletonRecord(storage) !== undefined).toBe(uncertain);
  await dispatcher.fetch(batchRequest([singleton()]));
  expect(processor.calls).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(1_000);
  expect(processor.calls).toHaveLength(2);
  reply(processor, 1, { type: "retryAt", atMs: Date.now() + 60_000 });
  await waitFor(
    () => singletonRecord(storage)?.nextAttemptAtMs === Date.now() + 60_000,
  );
  expect(singletonRecord(storage)?.infrastructureFailures).toBe(0);
  await dispatcher.fetch(batchRequest([singleton()]));
  expect(processor.calls).toHaveLength(2);
});

test.each([
  { kind: "singleton", taskName: "7 " },
  { kind: "published", taskName: "7", taskId: "7" },
  { kind: "singleton", taskName: "7", taskId: "7" },
])("a mismatched singleton response cannot end its chain: %j", async (task) => {
  vi.useFakeTimers();
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(batchRequest([singleton()]));
  reply(processor, 0, { type: "retryAt", atMs: Date.now() });
  await waitFor(() => singletonRecord(storage)?.state.type === "pending");
  await dispatcher.alarm();
  processor.calls[1]?.response.resolve(
    Response.json({ task, nextAction: { type: "done" } }),
  );
  await waitFor(() => singletonRecord(storage)?.infrastructureFailures === 1);
  await dispatcher.fetch(batchRequest([singleton()]));
  expect(processor.calls).toHaveLength(2);
});

function seedSchedule(storage: FakeAlarmStorage, taskName = "contract"): void {
  storage.records.set("scheduler", {
    nextHeartbeatAtMs: Date.now() + 30_000,
    nextAttemptId: 0,
  });
  storage.records.set("task:published:id", {
    task: { kind: "published", taskId: "id", taskName },
    nextAttemptAtMs: Date.now(),
    infrastructureFailures: 0,
    state: { type: "pending" },
  });
}

class FakeAlarmStorage implements DispatcherStorage {
  records = new Map<string, unknown>();
  private queue: Promise<unknown> = Promise.resolve();
  private stagedAlarms: number[] | undefined;
  alarm: number | null = null;
  readonly scheduledAlarms: number[] = [];
  getError: Error | null = null;
  setError: Error | null = null;
  setFailures = 0;
  uncertainCommit = false;
  getGate: Deferred<void> | null = null;

  async get<T>(key: string): Promise<T | undefined> {
    return structuredClone(this.records.get(key)) as T | undefined;
  }

  async list<T>({ prefix }: { prefix: string }): Promise<Map<string, T>> {
    return structuredClone(
      new Map([...this.records].filter(([key]) => key.startsWith(prefix))),
    ) as Map<string, T>;
  }

  async put<T>(key: string, value: T): Promise<void> {
    this.records.set(key, structuredClone(value));
  }

  async delete(key: string): Promise<boolean> {
    return this.records.delete(key);
  }

  transaction<T>(
    closure: (transaction: DispatcherRecords) => Promise<T>,
  ): Promise<T> {
    const result = this.queue.then(async () => {
      await this.getAlarm();
      const draft = new FakeAlarmStorage();
      draft.records = structuredClone(this.records);
      this.stagedAlarms = [];
      try {
        const result = await closure(draft);
        this.records = draft.records;
        for (const alarm of this.stagedAlarms) {
          this.alarm = alarm;
          this.scheduledAlarms.push(alarm);
        }
        if (this.uncertainCommit) {
          this.uncertainCommit = false;
          throw new Error("commit acknowledgement lost");
        }
        return result;
      } finally {
        this.stagedAlarms = undefined;
      }
    });
    this.queue = result.catch(() => undefined);
    return result;
  }

  async getAlarm(): Promise<number | null> {
    const gate = this.getGate;
    this.getGate = null;
    if (gate !== null) {
      await gate.promise;
    }
    if (this.getError !== null) {
      throw this.getError;
    }
    return this.alarm;
  }

  async setAlarm(alarmTime: number): Promise<void> {
    if (this.setFailures > 0) {
      this.setFailures--;
      throw new Error("alarm storage failed");
    }
    if (this.setError !== null) {
      throw this.setError;
    }
    if (this.stagedAlarms) this.stagedAlarms.push(alarmTime);
    else {
      this.alarm = alarmTime;
      this.scheduledAlarms.push(alarmTime);
    }
  }
}

interface ProcessorCall {
  readonly request: Request;
  readonly response: Deferred<Response>;
  readonly taskId: string;
  readonly task: TaskIdentity;
}

class DeferredProcessor implements ProcessorFetcher {
  readonly calls: ProcessorCall[] = [];

  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const response = new Deferred<Response>();
    this.calls.push({
      request: new Request(input, init),
      response,
      taskId: JSON.parse(init?.body as string).task.taskId,
      task: JSON.parse(init?.body as string).task,
    });
    return response.promise;
  }
}

class RecordingStub implements DurableObjectStubLike {
  readonly requests: Request[] = [];
  response: Response = Response.json({ ok: true });
  error: Error | null = null;

  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    this.requests.push(new Request(input, init));
    return this.error === null
      ? Promise.resolve(this.response)
      : Promise.reject(this.error);
  }
}

class RecordingNamespace implements DurableObjectNamespaceLike {
  readonly stub = new RecordingStub();
  names: string[] = [];
  error: Error | null = null;

  getByName(name: string): DurableObjectStubLike {
    this.names.push(name);
    if (this.error !== null) {
      throw this.error;
    }
    return this.stub;
  }
}

function dispatchRequest(taskId: string, taskName = "contract"): Request {
  return batchRequest([
    { task: { kind: "published", taskId, taskName }, intent: "run" },
  ]);
}

function batchRequest(tasks: readonly unknown[]): Request {
  return new Request("https://dispatcher/dispatch", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({ tasks }),
  });
}

async function waitFor(condition: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (condition()) {
      return;
    }

    await Promise.resolve();
  }

  throw new Error("condition was not met");
}

// Headers can resolve while text() is still waiting for the end of a real stream.
function controlledResponse(status = 200) {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  const body = new ReadableStream<Uint8Array>({
    start(value) {
      controller = value;
    },
  });
  const response = new Response(body, { status });
  const consumed = new Deferred<void>();
  const text = response.text.bind(response);
  vi.spyOn(response, "text").mockImplementation(async () => {
    try {
      return await text();
    } finally {
      consumed.resolve(undefined);
    }
  });
  return {
    response,
    consumed: consumed.promise,
    finish(text: string) {
      controller.enqueue(new TextEncoder().encode(text));
      controller.close();
    },
    fail(error: Error) {
      controller.error(error);
    },
  };
}

async function finish(processor: DeferredProcessor, index: number) {
  const control = controlledResponse();
  processor.calls[index]?.response.resolve(control.response);
  const task = processor.calls[index]?.task;
  control.finish(JSON.stringify({ task, nextAction: { type: "done" } }));
  await control.consumed;
}

async function expectJson(response: Response, status: number, body: unknown) {
  expect(response.status).toBe(status);
  expect(Object.fromEntries(response.headers)).toEqual({
    "cache-control": "no-store",
    "content-type": "application/json; charset=utf-8",
    "x-content-type-options": "nosniff",
  });
  expect(await response.json()).toEqual(body);
}

async function expectRedispatchAllowed(
  dispatcher: RetainedTaskDispatcher,
  processor: DeferredProcessor,
  taskId: string,
  taskName = "contract",
) {
  const count = processor.calls.length;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    await expectJson(
      await dispatcher.fetch(dispatchRequest(taskId, taskName)),
      200,
      { ok: true },
    );
    if (processor.calls.length > count) return;
  }
  throw new Error("completed attempt did not permit explicit redispatch");
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

test("healthy dispatch and unsaved completion perform no writes or schedule scans", async () => {
  const storage = new FakeAlarmStorage();
  storage.alarm = Date.now() + 10_000;
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  const transaction = vi.spyOn(storage, "transaction");
  const list = vi.spyOn(storage, "list");
  const get = vi.spyOn(storage, "get");
  const put = vi.spyOn(storage, "put");
  const setAlarm = vi.spyOn(storage, "setAlarm");
  await dispatcher.fetch(dispatchRequest("id"));
  await dispatcher.fetch(dispatchRequest("id", "ignored"));
  expect(processor.calls).toHaveLength(1);
  expect(get).not.toHaveBeenCalled();
  reply(processor, 0, { type: "done" });
  await expectRedispatchAllowed(dispatcher, processor, "id");
  expect(get).toHaveBeenCalledExactlyOnceWith("task:published:id");
  expect(transaction).not.toHaveBeenCalled();
  expect(list).not.toHaveBeenCalled();
  expect(put).not.toHaveBeenCalled();
  expect(setAlarm).not.toHaveBeenCalled();
  expect(storage.records.size).toBe(0);
  await finish(processor, 1);
});

test("unsaved uncertainty retries in memory; only a valid hint starts persistence", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(1_700_000_000_000);
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(dispatchRequest("id"));
  processor.calls[0]?.response.reject(new Error("network"));
  await vi.advanceTimersByTimeAsync(0);
  expect(storage.records.size).toBe(0);
  expect(storage.scheduledAlarms).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(999);
  expect(processor.calls).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(1);
  expect(processor.calls).toHaveLength(2);
  reply(processor, 1, { type: "retryAt", atMs: Date.now() - 1 });
  await waitFor(() => storedTask(storage)?.state.type === "pending");
  expect(storedTask(storage)?.infrastructureFailures).toBe(0);
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(3);
  await finish(processor, 2);
});

test("explicit redispatch reconciles a saved schedule only after its response", async () => {
  const storage = new FakeAlarmStorage();
  seedSchedule(storage, "old");
  storage.alarm = Date.now() - 1;
  const before = structuredClone(storage.records);
  const transaction = vi.spyOn(storage, "transaction");
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(dispatchRequest("id", "corrected"));
  await dispatcher.fetch(dispatchRequest("id", "ignored"));
  expect(transaction).not.toHaveBeenCalled();
  expect(storage.records).toEqual(before);
  expect(storage.scheduledAlarms).toEqual([]);
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(1);
  expect(storedTask(storage)).toBeDefined();
  reply(processor, 0, { type: "retryAt", atMs: Date.now() + 10_000 });
  await waitFor(() => storedTask(storage)?.task.taskName === "corrected");
  await expectRedispatchAllowed(dispatcher, processor, "id");
  reply(processor, 1, { type: "done" });
  await waitFor(() => !storedTask(storage));
});

test("first delayed scheduling response preserves the already armed warming deadline", async () => {
  let now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(dispatchRequest("id"));
  const heartbeat = storage.alarm;
  now += 10_000;
  reply(processor, 0, { type: "retryAt", atMs: now + 60_000 });
  await waitFor(() => storedTask(storage) !== undefined);
  expect(storage.alarm).toBe(heartbeat);
});

test("dispatch launches through blocked outcome bookkeeping without overwriting its earlier alarm", async () => {
  const now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockReturnValue(now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(dispatchRequest("id"));
  const gate = new Deferred<void>();
  storage.getGate = gate;
  reply(processor, 0, { type: "retryAt", atMs: now + 1_000 });
  await waitFor(() => storage.getGate === null);
  const dispatch = dispatcher.fetch(dispatchRequest("other"));
  await waitFor(() => processor.calls.length === 2);
  expect(storage.records.size).toBe(0);
  gate.resolve(undefined);
  expect((await dispatch).status).toBe(200);
  expect(storage.alarm).toBe(now + 1_000);
  expect(storedTask(storage, "other")).toBeUndefined();
  await finish(processor, 1);
});

test("an uncertain scheduling commit retains a recoverable schedule", async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  let now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  let dispatcher = new RetainedTaskDispatcher(storage, processor);
  expect((await dispatcher.fetch(dispatchRequest("id"))).status).toBe(200);
  expect(storage.records.size).toBe(0);
  storage.uncertainCommit = true;
  reply(processor, 0, { type: "retryAt", atMs: now + 1_000 });
  await waitFor(() => vi.mocked(console.error).mock.calls.length === 1);
  expect(storedTask(storage)?.state.type).toBe("pending");
  dispatcher = new RetainedTaskDispatcher(storage, processor);
  now += 1_000;
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(2);
  await finish(processor, 1);
});

test("failed alarm bookkeeping rolls back launches and re-arms from current records", async () => {
  let now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  seedSchedule(storage);
  await dispatcher.alarm();
  reply(processor, 0, { type: "retryAt", atMs: now + 1_000 });
  await waitFor(() => storedTask(storage)?.state.type === "pending");
  now += 1_000;
  storage.setFailures = 1;
  await expect(dispatcher.alarm()).rejects.toThrow("alarm storage failed");
  expect(processor.calls).toHaveLength(1);
  expect(storedTask(storage)?.state.type).toBe("pending");
  expect(storage.alarm).toBe(now);
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(2);
  await finish(processor, 1);
});

test("result persistence is awaited before retiring local tracking", async () => {
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  seedSchedule(storage);
  await dispatcher.alarm();
  const gate = new Deferred<void>();
  storage.getGate = gate;
  reply(processor, 0, { type: "done" });
  await waitFor(() => storage.getGate === null);
  expect(storedTask(storage)?.state.type).toBe("running");
  expect(processor.calls).toHaveLength(1);
  gate.resolve(undefined);
  await waitFor(() => !storedTask(storage));
});

test.each([
  "done",
  "retryAt",
  "body-error",
])("a late %s cannot affect an ID accepted again after completion", async (action) => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const original = new RetainedTaskDispatcher(storage, processor);
  seedSchedule(storage);
  await original.alarm();
  const body = controlledResponse();
  processor.calls[0]?.response.resolve(body.response);
  await waitFor(() => body.response.bodyUsed);
  const successor = new RetainedTaskDispatcher(storage, processor);
  await successor.fetch(dispatchRequest("id"));
  reply(processor, 1, { type: "done" });
  await waitFor(() => !storedTask(storage));
  await expectRedispatchAllowed(successor, processor, "id", "new");
  const current = structuredClone(storedTask(storage));
  if (action === "body-error") body.fail(new Error("late body error"));
  else
    body.finish(
      JSON.stringify({
        task: { kind: "published", taskId: "id", taskName: "contract" },
        nextAction:
          action === "done" ? { type: "done" } : { type: "retryAt", atMs: 0 },
      }),
    );
  await body.consumed;
  await original.alarm();
  expect(storedTask(storage)).toEqual(current);
  await expectJson(await successor.fetch(dispatchRequest("id", "wrong")), 200, {
    ok: true,
  });
  expect(processor.calls).toHaveLength(3);
  await finish(processor, 2);
});

test("later results and duplicate acknowledgements cannot postpone earlier deadlines", async () => {
  const now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockReturnValue(now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  for (const id of ["early", "late", "active"])
    await dispatcher.fetch(dispatchRequest(id));
  reply(processor, 0, { type: "retryAt", atMs: now + 1_000 });
  await waitFor(() => storedTask(storage, "early")?.state.type === "pending");
  reply(processor, 1, { type: "retryAt", atMs: now + 20_000 });
  await waitFor(() => storedTask(storage, "late")?.state.type === "pending");
  await dispatcher.fetch(dispatchRequest("active"));
  await dispatcher.alarm();
  expect(storage.alarm).toBe(now + 1_000);
  expect(processor.calls).toHaveLength(3);
});

interface StoredTask {
  task: TaskIdentity;
  nextAttemptAtMs: number;
  infrastructureFailures: number;
  state: { type: "pending" } | { type: "running"; attemptId: number };
}

function storedTask(
  storage: FakeAlarmStorage,
  id = "id",
): StoredTask | undefined {
  return storage.records.get(`task:published:${id}`) as StoredTask | undefined;
}

function reply(
  processor: DeferredProcessor,
  index: number,
  action: unknown,
): void {
  const call = processor.calls[index];
  if (!call) throw new Error("missing processor call");
  call.response.resolve(Response.json({ task: call.task, nextAction: action }));
}

test("future hints survive reconstruction, preserve heartbeat, and remove only on done", async () => {
  let now = 1_700_000_000_000;
  const clock = vi.spyOn(Date, "now").mockImplementation(() => now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  let dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.fetch(dispatchRequest("id"));
  reply(processor, 0, { type: "retryAt", atMs: now + 10_000 });
  await waitFor(() => storedTask(storage)?.state.type === "pending");
  dispatcher = new RetainedTaskDispatcher(storage, processor);
  await dispatcher.alarm();
  expect(storage.alarm).toBe(now + 10_000);
  now += 9_999;
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(1);
  now += 1;
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(2);
  expect(storage.alarm).toBe(now + 20_000);
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(2);
  reply(processor, 1, { type: "done" });
  await waitFor(() => !storedTask(storage));
  await dispatcher.alarm();
  expect(storage.alarm).toBe(now + 20_000);
  now += 25_000;
  await dispatcher.alarm();
  expect(storage.alarm).toBe(now + 30_000);
  clock.mockRestore();
});

test.each([
  false,
  true,
])("watchdog recovers hung attempts (reconstructed=%s) and rejects stale results", async (reconstruct) => {
  let now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  let dispatcher = new RetainedTaskDispatcher(storage, processor);
  seedSchedule(storage, "old");
  await dispatcher.alarm();
  const first = storedTask(storage)?.state;
  if (reconstruct) dispatcher = new RetainedTaskDispatcher(storage, processor);
  now += 59_999;
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(1);
  now += 1;
  await dispatcher.alarm();
  expect(storedTask(storage)?.nextAttemptAtMs).toBe(now + 1_000);
  now += 1_000;
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(2);
  expect(storedTask(storage)?.state).not.toEqual(first);
  reply(processor, 1, { type: "retryAt", atMs: now + 20_000 });
  await waitFor(() => storedTask(storage)?.state.type === "pending");
  await dispatcher.alarm();
  reply(processor, 0, { type: "done" });
  await dispatcher.alarm();
  expect(storedTask(storage)?.nextAttemptAtMs).toBe(now + 20_000);
  // Explicit redispatch overrides the pending hint and can correct routing.
  await dispatcher.fetch(dispatchRequest("id", "corrected"));
  await dispatcher.fetch(dispatchRequest("id", "must-not-replace"));
  expect(processor.calls).toHaveLength(3);
  expect(await processor.calls[2]?.request.json()).toEqual({
    task: {
      kind: "published",
      taskId: "id",
      taskName: "corrected",
    },
  });
  expect(storedTask(storage)?.task.taskName).toBe("old");
  reply(processor, 2, { type: "done" });
  await waitFor(() => !storedTask(storage));
  await expectRedispatchAllowed(dispatcher, processor, "id");
  expect(storedTask(storage)?.state).not.toEqual(first);
  await finish(processor, 3);
});

test("all 300 due IDs launch while every processor response is gated", async () => {
  let now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  storage.records.set("scheduler", {
    nextHeartbeatAtMs: now + 30_000,
    nextAttemptId: 0,
  });
  for (let id = 0; id < 300; id++) {
    storage.records.set(`task:published:${id}`, {
      task: {
        kind: "published",
        taskId: String(id),
        taskName: id % 2 ? "first" : "second",
      },
      state: { type: "pending" },
      infrastructureFailures: 0,
      nextAttemptAtMs: now + 1_000,
    });
  }
  now += 35_000; // Both heartbeat and every task are overdue.
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(300);
  expect(storage.alarm).toBe(now + 30_000);
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(300);
  for (const record of storage.records.values()) {
    if ("task" in (record as object))
      expect((record as StoredTask).state.type).toBe("running");
  }
});

test.each([
  "",
  "{}",
  "{",
  "null",
  '{"taskId":"id","attemptFinished":true}',
  '{"taskId":"other","nextAction":{"type":"done"}}',
  '{"taskId":"id","nextAction":{"type":"unknown"}}',
  ...[
    { kind: "published", taskId: "other", taskName: "contract" },
    { kind: "published", taskId: "id", taskName: "contract " },
    { kind: "singleton", taskName: "contract" },
    { kind: "published", taskId: "id", taskName: "contract", extra: true },
    { kind: "publish", taskId: "id", taskName: "contract" },
  ].map((task) => JSON.stringify({ task, nextAction: { type: "done" } })),
  ...[-1, 0.5, 8_640_000_000_000_001, 9_007_199_254_740_992, "1", null].map(
    (atMs) =>
      JSON.stringify({
        task: { kind: "published", taskId: "id", taskName: "contract" },
        nextAction: { type: "retryAt", atMs },
      }),
  ),
])("uncertain response %s remains durable with infrastructure backoff", async (body) => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockReturnValue(now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  seedSchedule(storage);
  await dispatcher.alarm();
  processor.calls[0]?.response.resolve(new Response(body));
  await waitFor(() => storedTask(storage)?.state.type === "pending");
  expect(storedTask(storage)?.nextAttemptAtMs).toBe(now + 1_000);
  expect(storedTask(storage)?.infrastructureFailures).toBe(1);
});

test("backoff persists, saturates, and resets on a valid past retry instruction", async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  let now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  let dispatcher = new RetainedTaskDispatcher(storage, processor);
  seedSchedule(storage, "unknown");
  await dispatcher.alarm();
  for (let index = 0; index < 8; index++) {
    processor.calls[index]?.response.resolve(
      new Response("unknown", { status: 404 }),
    );
    await waitFor(() => storedTask(storage)?.state.type === "pending");
    const delay = Math.min(1_000 * 2 ** index, 30_000);
    expect(storedTask(storage)?.nextAttemptAtMs).toBe(now + delay);
    expect(storedTask(storage)?.infrastructureFailures).toBe(
      Math.min(index + 1, 6),
    );
    dispatcher = new RetainedTaskDispatcher(storage, processor);
    now += delay;
    await dispatcher.alarm();
    expect(processor.calls).toHaveLength(index + 2);
  }
  reply(processor, 8, { type: "retryAt", atMs: now - 1 });
  await waitFor(() => storedTask(storage)?.state.type === "pending");
  expect(storedTask(storage)?.infrastructureFailures).toBe(0);
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(10);
  processor.calls[9]?.response.reject(new Error("network"));
  await waitFor(() => storedTask(storage)?.infrastructureFailures === 1);
  expect(storedTask(storage)?.nextAttemptAtMs).toBe(now + 1_000);
});

test("failed result persistence retains a scheduled attempt's watchdog", async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  let now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  let dispatcher = new RetainedTaskDispatcher(storage, processor);
  seedSchedule(storage);
  await dispatcher.alarm();
  storage.setError = new Error("commit failed");
  reply(processor, 0, { type: "done" });
  await waitFor(() => vi.mocked(console.error).mock.calls.length === 1);
  expect(storedTask(storage)?.state.type).toBe("running");
  expect(storage.alarm).toBe(now + 30_000);
  storage.setError = null;
  dispatcher = new RetainedTaskDispatcher(storage, processor);
  now += 60_000;
  await dispatcher.alarm();
  now += 1_000;
  await dispatcher.alarm();
  expect(processor.calls).toHaveLength(2);
  await finish(processor, 1);
});

test("corrupt current records never become an empty queue", async () => {
  for (const [key, value] of [
    ["scheduler", {}],
    ["scheduler", null],
    ["task:published:id", { taskId: "id" }],
  ] as const) {
    const storage = new FakeAlarmStorage();
    storage.records.set(key, value);
    const processor = new DeferredProcessor();
    const dispatcher = new RetainedTaskDispatcher(storage, processor);
    expect((await dispatcher.fetch(dispatchRequest("id"))).status).toBe(200);
    await expect(dispatcher.alarm()).rejects.toThrow();
    expect(processor.calls).toHaveLength(1);
    expect(storage.records.get(key)).toEqual(value);
  }
});

test("dispatchTask targets the global object and consumes the response", async () => {
  const namespace = new RecordingNamespace();
  const control = controlledResponse();
  namespace.stub.response = control.response;
  let accepted = false;
  const dispatch = dispatchTask(namespace, "contract", "123").then(() => {
    accepted = true;
  });
  await waitFor(() => control.response.bodyUsed);
  expect(accepted).toBe(false);

  control.finish(JSON.stringify({ ok: true }));
  await dispatch;
  await control.consumed;

  expect(namespace.names).toEqual(["global"]);
  expect(namespace.stub.requests).toHaveLength(1);
  expect(namespace.stub.requests[0]?.method).toBe("POST");
  expect(namespace.stub.requests[0]?.url).toBe("https://dispatcher/dispatch");
  expect(namespace.stub.requests[0]?.headers.get("content-type")).toBe(
    "application/json",
  );
  expect(await namespace.stub.requests[0]?.json()).toEqual({
    tasks: [
      {
        task: { kind: "published", taskId: "123", taskName: "contract" },
        intent: "run",
      },
    ],
  });
  expect(namespace.stub.response.bodyUsed).toBe(true);
});

test("dispatchTask consumes an error response before rejecting", async () => {
  const namespace = new RecordingNamespace();
  const control = controlledResponse(503);
  namespace.stub.response = control.response;
  let ended = false;
  const dispatch = dispatchTask(namespace, "contract", "123").catch(
    (error: unknown) => {
      ended = true;
      return error;
    },
  );
  await waitFor(() => control.response.bodyUsed);
  expect(ended).toBe(false);
  control.finish("dispatcher unavailable");
  expect(await dispatch).toEqual(
    new Error("task dispatcher returned HTTP 503: dispatcher unavailable"),
  );
  await control.consumed;
  expect(namespace.stub.response.bodyUsed).toBe(true);
});

test("retained dispatch accepts before processor completion and observes its response", async () => {
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);

  const acceptancePromise = dispatcher.fetch(dispatchRequest("task-1"));
  await waitFor(() => processor.calls.length === 1);
  const acceptance = await acceptancePromise;

  await expectJson(acceptance, 200, { ok: true });
  expect(processor.calls[0]?.request.url).toBe("https://processor/process");
  expect(processor.calls[0]?.request.method).toBe("POST");
  expect(processor.calls[0]?.request.headers.get("content-type")).toBe(
    "application/json",
  );
  expect(await processor.calls[0]?.request.json()).toEqual({
    task: {
      kind: "published",
      taskId: "task-1",
      taskName: "contract",
    },
  });

  const duplicate = await dispatcher.fetch(dispatchRequest("task-1", "other"));
  expect(duplicate.status).toBe(200);
  expect(await duplicate.json()).toEqual({ ok: true });
  expect(processor.calls).toHaveLength(1);

  const control = controlledResponse();
  processor.calls[0]?.response.resolve(control.response);
  await waitFor(() => control.response.bodyUsed);
  await expectJson(await dispatcher.fetch(dispatchRequest("task-1")), 200, {
    ok: true,
  });
  expect(processor.calls).toHaveLength(1);
  control.finish(
    JSON.stringify({
      task: { kind: "published", taskId: "task-1", taskName: "contract" },
      nextAction: { type: "done" },
    }),
  );
  await control.consumed;
  await expectRedispatchAllowed(dispatcher, processor, "task-1");
  expect(processor.calls).toHaveLength(2);
  await finish(processor, 1);
});

test("different task IDs can retain concurrent processor requests", async () => {
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);

  const firstAcceptance = dispatcher.fetch(dispatchRequest("task-1"));
  await waitFor(() => processor.calls.length === 1);
  const secondAcceptance = dispatcher.fetch(dispatchRequest("task-2", "other"));
  await waitFor(() => processor.calls.length === 2);

  await Promise.all([firstAcceptance, secondAcceptance]);
  expect(processor.calls.map(({ request }) => request.url)).toEqual([
    "https://processor/process",
    "https://processor/process",
  ]);
  expect(
    await Promise.all(processor.calls.map(({ request }) => request.json())),
  ).toEqual([
    { task: { kind: "published", taskId: "task-1", taskName: "contract" } },
    { task: { kind: "published", taskId: "task-2", taskName: "other" } },
  ]);

  await Promise.all([finish(processor, 0), finish(processor, 1)]);
});

test("dispatch and alarm schedule the 30-second heartbeat", async () => {
  const now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockReturnValue(now);

  const idleStorage = new FakeAlarmStorage();
  const idle = new RetainedTaskDispatcher(idleStorage, new DeferredProcessor());
  await idle.alarm();
  expect(idleStorage.scheduledAlarms).toEqual([now + 30_000]);

  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);

  await dispatcher.fetch(dispatchRequest("task-1"));
  expect(storage.scheduledAlarms).toEqual([now + 30_000]);

  await dispatcher.alarm();
  expect(storage.scheduledAlarms).toEqual([now + 30_000, now + 30_000]);

  await finish(processor, 0);
  // Consuming the last response does not disable heartbeats.
  await dispatcher.alarm();
  expect(storage.scheduledAlarms.every((time) => time === now + 30_000)).toBe(
    true,
  );
});

test.each([
  404, 500,
])("processor HTTP %s failures are retained and permit explicit redispatch", async (status) => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);

  await dispatcher.fetch(dispatchRequest("task-1", "unknown"));
  const control = controlledResponse(status);
  processor.calls[0]?.response.resolve(control.response);
  await waitFor(() => control.response.bodyUsed);
  expect(log).not.toHaveBeenCalled();
  await expectJson(await dispatcher.fetch(dispatchRequest("task-1")), 200, {
    ok: true,
  });
  const message = status === 404 ? '{"error":"unknown task name"}' : "failed";
  control.finish(message);
  await control.consumed;
  await waitFor(() => log.mock.calls.length === 1);
  expect(log).toHaveBeenCalledWith(
    "task processor failed",
    "published",
    `task processor returned HTTP ${status}: ${message}`,
  );

  await expectRedispatchAllowed(dispatcher, processor, "task-1");
  expect(processor.calls).toHaveLength(2);
  await finish(processor, 1);
});

test("generic IDs use UTF-16 limits and round trip without numeric parsing", async () => {
  for (const id of ["", "a".repeat(201), `${"😀".repeat(100)}a`]) {
    const namespace = new RecordingNamespace();
    await expect(dispatchTask(namespace, "contract", id)).rejects.toThrow(
      "taskId must be a non-empty string no longer than 200 characters",
    );
    expect(namespace.names).toEqual([]);
  }
  for (const taskId of [
    "0",
    " ",
    'opaque/"\\\n雪',
    "a".repeat(200),
    "😀".repeat(100),
    `${"a".repeat(198)}😀`,
  ]) {
    const namespace = new RecordingNamespace();
    await dispatchTask(namespace, "contract", taskId);
    const body = await namespace.stub.requests[0]?.text();
    expect(JSON.parse(body ?? "")).toEqual({
      tasks: [
        {
          task: { kind: "published", taskId, taskName: "contract" },
          intent: "run",
        },
      ],
    });
    const processor = new DeferredProcessor();
    const dispatcher = new RetainedTaskDispatcher(
      new FakeAlarmStorage(),
      processor,
    );
    const request = new Request("https://dispatcher/dispatch?ignored=true", {
      method: "POST",
      headers: { "content-type": "Application/JSON; charset=utf-8" },
      body,
    });
    await expectJson(await dispatcher.fetch(request), 200, { ok: true });
    await finish(processor, 0);
  }
});

test("invalid routes and inputs do not launch work or schedule alarms", async () => {
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  for (const [method, path] of [
    ["GET", "/dispatch"],
    ["POST", "/dispatch/"],
    ["POST", "/process"],
  ]) {
    const request = new Request(`https://dispatcher${path}`, { method });
    const read = vi.spyOn(request, "json");
    await expectJson(await dispatcher.fetch(request), 404, {
      error: "not-found",
      ok: false,
    });
    expect(read).not.toHaveBeenCalled();
  }
  for (const contentType of [null, "text/plain"]) {
    const headers = new Headers();
    if (contentType !== null) {
      headers.set("content-type", contentType);
    }
    const request = new Request("https://dispatcher/dispatch", {
      method: "POST",
      headers,
    });
    const read = vi.spyOn(request, "json");
    await expectJson(await dispatcher.fetch(request), 400, {
      error: "request content-type must be application/json",
      ok: false,
    });
    expect(read).not.toHaveBeenCalled();
  }
  for (const body of [
    null,
    [],
    [{ taskId: "id" }],
    true,
    1,
    "id",
    {},
    { taskId: null },
    { taskId: 1 },
    { taskId: false },
    { taskId: [] },
    { taskId: {} },
    { taskId: "" },
    { taskId: "a".repeat(201) },
    { taskId: `${"😀".repeat(100)}a` },
  ]) {
    const error =
      body !== null && typeof body === "object" && !Array.isArray(body)
        ? "request must contain a tasks array"
        : "invalid dispatcher state or processor response";
    const request = new Request("https://dispatcher/dispatch", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    await expectJson(await dispatcher.fetch(request), 400, {
      error,
      ok: false,
    });
  }
  const malformed = new Request("https://dispatcher/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{",
  });
  const result = await dispatcher.fetch(malformed);
  expect(result.status).toBe(400);
  expect(await result.json()).toEqual({
    ok: false,
    error: expect.stringMatching(/.+/),
  });
  expect(processor.calls).toHaveLength(0);
  expect(storage.scheduledAlarms).toEqual([]);
});

test("dispatch only repairs a missing or too-late heartbeat alarm", async () => {
  const now = 1_700_000_000_000;
  vi.spyOn(Date, "now").mockReturnValue(now);
  for (const alarm of [
    null,
    now - 1,
    now,
    now + 1,
    now + 30_000,
    now + 30_001,
  ]) {
    const storage = new FakeAlarmStorage();
    const processor = new DeferredProcessor();
    const dispatcher = new RetainedTaskDispatcher(storage, processor);
    const expected =
      alarm === null || alarm > now + 30_000 ? [now + 30_000] : [];
    for (const _duplicate of [false, true]) {
      storage.alarm = alarm;
      storage.scheduledAlarms.length = 0;
      await expectJson(await dispatcher.fetch(dispatchRequest("task-1")), 200, {
        ok: true,
      });
      expect(storage.scheduledAlarms).toEqual(expected);
    }
    await finish(processor, 0);
  }
});

test.each([
  undefined,
  "",
  null,
  17,
  [],
  {},
  false,
])("rejects task name %j before lookup, launch, or alarm", async (taskName) => {
  const namespace = new RecordingNamespace();
  await expect(
    dispatchTask(namespace, taskName as string, "17"),
  ).rejects.toThrow("taskName must be a non-empty string");
  await expect(dispatchTask(namespace, taskName as string, "")).rejects.toThrow(
    "taskName must be a non-empty string",
  );
  expect(namespace.names).toEqual([]);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  const input = new Request("https://dispatcher/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      tasks: [
        { task: { kind: "published", taskId: "17", taskName }, intent: "run" },
      ],
    }),
  });
  await expectJson(await dispatcher.fetch(input), 400, {
    error: "taskName must be a non-empty string",
    ok: false,
  });
  expect(processor.calls).toHaveLength(0);
  expect(storage.scheduledAlarms).toEqual([]);
});

test.each([
  "contract",
  " ",
  'Name/"\\\n雪🦀',
  "x".repeat(1000),
])("forwards exact name %s through both hops without a payload", async (taskName) => {
  const namespace = new RecordingNamespace();
  await dispatchTask(namespace, taskName, "opaque");
  const body = await namespace.stub.requests[0]?.json();
  expect(body).toEqual({
    tasks: [
      {
        task: { kind: "published", taskId: "opaque", taskName },
        intent: "run",
      },
    ],
  });
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(
    new FakeAlarmStorage(),
    processor,
  );
  await expectJson(
    await dispatcher.fetch(
      new Request("https://dispatcher/dispatch", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
    ),
    200,
    { ok: true },
  );
  expect(await processor.calls[0]?.request.json()).toEqual({
    task: body.tasks[0].task,
  });
  await finish(processor, 0);
});

test("rejected processor fetches and body reads are retained and permit explicit redispatch", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  for (const status of [null, 200, 503]) {
    log.mockClear();
    const processor = new DeferredProcessor();
    const dispatcher = new RetainedTaskDispatcher(
      new FakeAlarmStorage(),
      processor,
    );
    await dispatcher.fetch(dispatchRequest("task-1"));
    if (status === null) {
      processor.calls[0]?.response.reject(new Error("fetch rejected"));
    } else {
      const control = controlledResponse(status);
      processor.calls[0]?.response.resolve(control.response);
      await waitFor(() => control.response.bodyUsed);
      control.fail(new Error("body read rejected"));
      await control.consumed;
    }
    await waitFor(() => log.mock.calls.length === 1);
    expect(log).toHaveBeenCalledWith(
      "task processor failed",
      "published",
      status === null ? "fetch rejected" : "body read rejected",
    );
    await expectRedispatchAllowed(dispatcher, processor, "task-1");
    expect(processor.calls).toHaveLength(2);
    await finish(processor, 1);
  }
});

test("dispatchTask propagates namespace, fetch, and body-read errors", async () => {
  const namespace = new RecordingNamespace();
  namespace.error = new Error("namespace unavailable");
  await expect(dispatchTask(namespace, "contract", "id")).rejects.toBe(
    namespace.error,
  );
  for (const status of [null, 200, 503]) {
    const namespace = new RecordingNamespace();
    const error = new Error("transport or body failed");
    if (status === null) {
      namespace.stub.error = error;
    } else {
      const control = controlledResponse(status);
      namespace.stub.response = control.response;
      control.fail(error);
    }
    await expect(dispatchTask(namespace, "contract", "id")).rejects.toBe(error);
  }
});

test("diagnostic truncation is Unicode-safe and never limits body consumption", async () => {
  for (const [text, expected] of [
    ["a".repeat(600), "a".repeat(500)],
    ["雪".repeat(600), "雪".repeat(500)],
    [`${"a".repeat(499)}😀tail`, "a".repeat(499)],
    [`${"😀".repeat(250)}tail`, "😀".repeat(250)],
  ]) {
    const namespace = new RecordingNamespace();
    const control = controlledResponse(503);
    namespace.stub.response = control.response;
    const dispatch = dispatchTask(namespace, "contract", "id");
    control.finish(text);
    await expect(dispatch).rejects.toEqual(
      new Error(`task dispatcher returned HTTP 503: ${expected}`),
    );
    await control.consumed;
  }
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  for (const status of [null, 500]) {
    log.mockClear();
    const processor = new DeferredProcessor();
    const dispatcher = new RetainedTaskDispatcher(
      new FakeAlarmStorage(),
      processor,
    );
    await dispatcher.fetch(dispatchRequest("id"));
    const prefix = status === null ? "" : "task processor returned HTTP 500: ";
    const text = `${"a".repeat(499 - prefix.length)}😀tail`;
    if (status === null) {
      processor.calls[0]?.response.reject(new Error(text));
    } else {
      processor.calls[0]?.response.resolve(new Response(text, { status }));
    }
    await waitFor(() => log.mock.calls.length === 1);
    expect(log).toHaveBeenCalledWith(
      "task processor failed",
      "published",
      `${prefix}${"a".repeat(499 - prefix.length)}`,
    );
  }
});

test("request-read and storage errors use the existing error envelope", async () => {
  const processor = new DeferredProcessor();
  const storage = new FakeAlarmStorage();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  const request = dispatchRequest("id");
  vi.spyOn(request, "json").mockRejectedValue(
    new Error(`${"a".repeat(499)}😀tail`),
  );
  await expectJson(await dispatcher.fetch(request), 400, {
    error: "a".repeat(499),
    ok: false,
  });
  expect(processor.calls).toHaveLength(0);
  expect(storage.scheduledAlarms).toEqual([]);
  for (const get of [true, false]) {
    storage.getError = get ? new Error("storage failed") : null;
    storage.setError = get ? null : new Error("storage failed");
    await expectJson(await dispatcher.fetch(dispatchRequest("id")), 400, {
      error: "storage failed",
      ok: false,
    });
  }
  await expect(dispatcher.alarm()).rejects.toThrow("storage failed");
  storage.getError = null;
  storage.setError = null;
  await expectJson(await dispatcher.fetch(dispatchRequest("id")), 200, {
    ok: true,
  });
  expect(processor.calls).toHaveLength(1);
  await finish(processor, 0);
});

test("launch precedes heartbeat storage and never persists acceptance", async () => {
  const storage = new FakeAlarmStorage();
  const gate = new Deferred<void>();
  storage.getGate = gate;
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);
  let accepted = false;
  const acceptance = dispatcher
    .fetch(dispatchRequest("id"))
    .then((response) => {
      accepted = true;
      return response;
    });
  await waitFor(() => storage.getGate === null);
  expect(accepted).toBe(false);
  expect(processor.calls).toHaveLength(1);
  const duplicate = dispatcher.fetch(dispatchRequest("id"));
  const other = dispatcher.fetch(dispatchRequest("other"));
  await waitFor(() => processor.calls.length === 2);
  expect(storage.records.size).toBe(0);
  gate.resolve(undefined);
  await expectJson(await acceptance, 200, { ok: true });
  await expectJson(await duplicate, 200, { ok: true });
  await expectJson(await other, 200, { ok: true });
  await finish(processor, 0);
  await finish(processor, 1);
});

test("synchronously thrown processor errors are logged and permit explicit redispatch", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const dispatcher = new RetainedTaskDispatcher(new FakeAlarmStorage(), {
    fetch() {
      throw new Error("fetch threw");
    },
  });
  for (const count of [1, 2]) {
    await waitFor(() => count === 1 || log.mock.calls.length === count - 1);
    await dispatcher.fetch(dispatchRequest("id"));
    await waitFor(() => log.mock.calls.length === count);
    expect(log).toHaveBeenLastCalledWith(
      "task processor failed",
      "published",
      "fetch threw",
    );
  }
});
