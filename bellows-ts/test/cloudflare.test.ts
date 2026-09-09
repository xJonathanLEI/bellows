import { afterEach, expect, test, vi } from "vitest";
import {
  type AlarmStorage,
  type DurableObjectNamespaceLike,
  type DurableObjectStubLike,
  dispatchTask,
  type ProcessorFetcher,
  RetainedTaskDispatcher,
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

class FakeAlarmStorage implements AlarmStorage {
  alarm: number | null = null;
  readonly scheduledAlarms: number[] = [];
  getError: Error | null = null;
  setError: Error | null = null;
  getGate: Deferred<void> | null = null;

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
    if (this.setError !== null) {
      throw this.setError;
    }
    this.alarm = alarmTime;
    this.scheduledAlarms.push(alarmTime);
  }
}

interface ProcessorCall {
  readonly request: Request;
  readonly response: Deferred<Response>;
}

class DeferredProcessor implements ProcessorFetcher {
  readonly calls: ProcessorCall[] = [];

  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const response = new Deferred<Response>();
    this.calls.push({
      request: new Request(input, init),
      response,
    });
    return response.promise;
  }
}

class RecordingStub implements DurableObjectStubLike {
  readonly requests: Request[] = [];
  response: Response = new Response("accepted");
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

function dispatchRequest(taskId: string): Request {
  return new Request("https://dispatcher/dispatch", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({ taskId }),
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
  control.finish("finished");
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

async function expectReleased(
  dispatcher: RetainedTaskDispatcher,
  taskId: string,
) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const response = await dispatcher.fetch(dispatchRequest(taskId));
    expect(response.status).toBe(200);
    const body = (await response.json()) as { duplicate?: boolean };
    if (body.duplicate === undefined) {
      expect(body).toEqual({ ok: true, taskId });
      return;
    }
    expect(body).toEqual({ duplicate: true, ok: true, taskId });
  }
  throw new Error("completed attempt did not release its ID");
}

afterEach(() => {
  vi.restoreAllMocks();
});

test("dispatchTask targets the global object and consumes the response", async () => {
  const namespace = new RecordingNamespace();
  const control = controlledResponse();
  namespace.stub.response = control.response;
  let accepted = false;
  const dispatch = dispatchTask(namespace, "123").then(() => {
    accepted = true;
  });
  await waitFor(() => control.response.bodyUsed);
  expect(accepted).toBe(false);

  control.finish("accepted");
  await dispatch;
  await control.consumed;

  expect(namespace.names).toEqual(["global"]);
  expect(namespace.stub.requests).toHaveLength(1);
  expect(namespace.stub.requests[0]?.method).toBe("POST");
  expect(namespace.stub.requests[0]?.url).toBe("https://dispatcher/dispatch");
  expect(namespace.stub.requests[0]?.headers.get("content-type")).toBe(
    "application/json",
  );
  expect(await namespace.stub.requests[0]?.json()).toEqual({ taskId: "123" });
  expect(namespace.stub.response.bodyUsed).toBe(true);
});

test("dispatchTask consumes an error response before rejecting", async () => {
  const namespace = new RecordingNamespace();
  const control = controlledResponse(503);
  namespace.stub.response = control.response;
  let ended = false;
  const dispatch = dispatchTask(namespace, "123").catch((error: unknown) => {
    ended = true;
    return error;
  });
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

  await expectJson(acceptance, 200, {
    ok: true,
    taskId: "task-1",
  });
  expect(processor.calls[0]?.request.url).toBe("https://processor/process");
  expect(processor.calls[0]?.request.method).toBe("POST");
  expect(processor.calls[0]?.request.headers.get("content-type")).toBe(
    "application/json",
  );
  expect(await processor.calls[0]?.request.json()).toEqual({
    taskId: "task-1",
  });

  const duplicate = await dispatcher.fetch(dispatchRequest("task-1"));
  expect(duplicate.status).toBe(200);
  expect(await duplicate.json()).toEqual({
    duplicate: true,
    ok: true,
    taskId: "task-1",
  });
  expect(processor.calls).toHaveLength(1);

  const control = controlledResponse();
  processor.calls[0]?.response.resolve(control.response);
  await waitFor(() => control.response.bodyUsed);
  await expectJson(await dispatcher.fetch(dispatchRequest("task-1")), 200, {
    duplicate: true,
    ok: true,
    taskId: "task-1",
  });
  expect(processor.calls).toHaveLength(1);
  control.finish("finished");
  await control.consumed;
  await expectReleased(dispatcher, "task-1");
  expect(processor.calls).toHaveLength(2);
  await finish(processor, 1);
});

test("different task IDs can retain concurrent processor requests", async () => {
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);

  const firstAcceptance = dispatcher.fetch(dispatchRequest("task-1"));
  await waitFor(() => processor.calls.length === 1);
  const secondAcceptance = dispatcher.fetch(dispatchRequest("task-2"));
  await waitFor(() => processor.calls.length === 2);

  await Promise.all([firstAcceptance, secondAcceptance]);
  expect(processor.calls.map(({ request }) => request.url)).toEqual([
    "https://processor/process",
    "https://processor/process",
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
  expect(storage.scheduledAlarms).toEqual([
    now + 30_000,
    now + 30_000,
    now + 30_000,
  ]);
});

test("processor failures are observed and release the task ID", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const storage = new FakeAlarmStorage();
  const processor = new DeferredProcessor();
  const dispatcher = new RetainedTaskDispatcher(storage, processor);

  await dispatcher.fetch(dispatchRequest("task-1"));
  const control = controlledResponse(500);
  processor.calls[0]?.response.resolve(control.response);
  await waitFor(() => control.response.bodyUsed);
  expect(log).not.toHaveBeenCalled();
  await expectJson(await dispatcher.fetch(dispatchRequest("task-1")), 200, {
    duplicate: true,
    ok: true,
    taskId: "task-1",
  });
  control.finish("failed");
  await control.consumed;
  await waitFor(() => log.mock.calls.length === 1);
  expect(log).toHaveBeenCalledWith(
    "task processor failed",
    "task-1",
    "task processor returned HTTP 500: failed",
  );

  await expectReleased(dispatcher, "task-1");
  expect(processor.calls).toHaveLength(2);
  await finish(processor, 1);
});

test("generic IDs use UTF-16 limits and round trip without numeric parsing", async () => {
  for (const id of ["", "a".repeat(201), `${"😀".repeat(100)}a`]) {
    const namespace = new RecordingNamespace();
    await expect(dispatchTask(namespace, id)).rejects.toThrow(
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
    await dispatchTask(namespace, taskId);
    const body = await namespace.stub.requests[0]?.text();
    expect(JSON.parse(body ?? "")).toEqual({ taskId });
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
    await expectJson(await dispatcher.fetch(request), 200, {
      ok: true,
      taskId,
    });
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
        ? "taskId must be a non-empty string no longer than 200 characters"
        : "request body must be a JSON object";
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

test("heartbeat preserves only future alarms no later than the next heartbeat", async () => {
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
      alarm !== null && alarm > now && alarm <= now + 30_000
        ? []
        : [now + 30_000];
    for (const duplicate of [false, true]) {
      storage.alarm = alarm;
      storage.scheduledAlarms.length = 0;
      await expectJson(
        await dispatcher.fetch(dispatchRequest("task-1")),
        200,
        duplicate
          ? { duplicate: true, ok: true, taskId: "task-1" }
          : { ok: true, taskId: "task-1" },
      );
      expect(storage.scheduledAlarms).toEqual(expected);
    }
    await finish(processor, 0);
  }
});

test("rejected processor fetches and body reads are observed and release the ID", async () => {
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
      "task-1",
      status === null ? "fetch rejected" : "body read rejected",
    );
    await expectReleased(dispatcher, "task-1");
    expect(processor.calls).toHaveLength(2);
    await finish(processor, 1);
  }
});

test("dispatchTask propagates namespace, fetch, and body-read errors", async () => {
  const namespace = new RecordingNamespace();
  namespace.error = new Error("namespace unavailable");
  await expect(dispatchTask(namespace, "id")).rejects.toBe(namespace.error);
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
    await expect(dispatchTask(namespace, "id")).rejects.toBe(error);
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
    const dispatch = dispatchTask(namespace, "id");
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
      "id",
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
    duplicate: true,
    ok: true,
    taskId: "id",
  });
  expect(processor.calls).toHaveLength(1);
  await finish(processor, 0);
});

test("pending storage I/O does not block the registry or processor progress", async () => {
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
  await waitFor(() => processor.calls.length === 1);
  expect(accepted).toBe(false);
  await expectJson(await dispatcher.fetch(dispatchRequest("id")), 200, {
    duplicate: true,
    ok: true,
    taskId: "id",
  });
  await expectJson(await dispatcher.fetch(dispatchRequest("other")), 200, {
    ok: true,
    taskId: "other",
  });
  await finish(processor, 0);
  await finish(processor, 1);
  gate.resolve(undefined);
  await expectJson(await acceptance, 200, { ok: true, taskId: "id" });
});

test("synchronously thrown processor errors are logged and release the ID", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const dispatcher = new RetainedTaskDispatcher(new FakeAlarmStorage(), {
    fetch() {
      throw new Error("fetch threw");
    },
  });
  for (const count of [1, 2]) {
    await expectReleased(dispatcher, "id");
    await waitFor(() => log.mock.calls.length === count);
    expect(log).toHaveBeenLastCalledWith(
      "task processor failed",
      "id",
      "fetch threw",
    );
  }
});
