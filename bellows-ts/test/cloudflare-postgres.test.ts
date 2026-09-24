import { Pool } from "pg";
import { afterEach, beforeEach, expect, expectTypeOf, test, vi } from "vitest";
import { InMemoryBackend } from "../src/backends/in-memory.js";
import { PostgresExecutionBackend } from "../src/backends/postgres-execution.js";
import {
  PostgresSingletonTaskIdError,
  PostgresTaskOperations,
} from "../src/backends/postgres-operations.js";
import {
  createPostgresProcessor,
  createPostgresProcessorTask,
  type PostgresProcessorConfig,
  type PostgresProcessorTask,
} from "../src/cloudflare/postgres.js";
import {
  type ClaimedTask,
  definePublishTask,
  defineSingletonTask,
  LeaseLostError,
  type PublishTaskDefinition,
  type SingletonTaskDefinition,
  type TaskCallback,
  type TaskDefinition,
  type TaskExecutionBackend,
  TaskFailure,
  TaskLeasedError,
  TaskNotFoundError,
  type TaskResult,
  TaskSuccess,
  TaskUnavailableError,
  type WorkerFactory,
} from "../src/index.js";
import { Gate } from "./helpers.js";

const task = definePublishTask<{ name: string }, { greeting: string }>(
  "processor_contract",
);
const countTask = definePublishTask<{ count: number }, number[]>(
  'Count/"\\\n雪🦀',
);
const secret = new Error("postgres://user:secret@private/database");
const invalidIdentity = "invalid task identity";
const safe = "taskId must encode a positive safe integer canonically";

class Execution implements TaskExecutionBackend {
  persistedName = task.name;
  persistedPayload = '{"name":"claimed"}';
  claimError: Error | undefined;
  finalizationError: Error | undefined;
  renewalDue = false;
  readonly recording = new Gate();
  recordingGate: Gate | undefined;
  readonly renewing = new Gate();
  renewalGate = new Gate();
  readonly closing = new Gate();
  closeGate: Gate | undefined;

  constructor() {
    vi.spyOn(this as Execution, "claimPublished");
  }

  async claimPublished<TPayload, TCallback>(
    definition: PublishTaskDefinition<TPayload, TCallback>,
    _workerId: number,
    taskId: number,
    leaseExpirationMs: number,
  ): Promise<ClaimedTask<TPayload>> {
    if (definition.name !== this.persistedName) throw new TaskNotFoundError();
    if (this.claimError) throw this.claimError;
    return {
      taskId,
      taskPayload: definition.codec.decode(this.persistedPayload),
      leaseExpirationMs: this.renewalDue ? Date.now() : leaseExpirationMs,
    };
  }
  claimEarliestPublished = vi.fn(async (): Promise<never> => {
    throw new Error("must claim the explicit ID");
  });
  claimSingleton = vi.fn(
    async (
      _task: SingletonTaskDefinition<unknown>,
      _workerId: number,
      leaseExpirationMs: number,
    ): Promise<ClaimedTask<undefined>> => {
      if (this.claimError) throw this.claimError;
      return {
        taskId: 73,
        taskPayload: undefined,
        leaseExpirationMs: this.renewalDue ? Date.now() : leaseExpirationMs,
      };
    },
  );
  renew = vi.fn(async (): Promise<never> => {
    this.renewing.release();
    await this.renewalGate.wait();
    throw new LeaseLostError();
  });
  fail = vi.fn(async (_workerId: number, taskId: number) => {
    await this.record();
    return { taskId };
  });
  finish = vi.fn(
    async <TTask extends TaskDefinition>(
      _task: TTask,
      _workerId: number,
      taskId: number,
      _callback: TaskCallback<TTask>,
      _availableFrom: number | null,
    ) => {
      await this.record();
      return { taskId };
    },
  );
  close = vi.fn(async () => {
    this.closing.release();
    await this.closeGate?.wait();
  });

  private async record() {
    this.recording.release();
    await this.recordingGate?.wait();
    if (this.finalizationError) throw this.finalizationError;
  }
}

const singletonTask = defineSingletonTask<number[]>(" singleton/雪🦀 ");

test.each([
  "",
  "0",
  "-1",
  "+1",
  "01",
  " 1",
  "1 ",
  "1\n",
  "1.0",
  "1e1",
  "١",
  "１",
  "10000000000000000",
])("published identity preserves canonical ID validation for %j", async (taskId) => {
  const f = fixture();
  await envelope(
    await f.processor.fetch(request({ taskId, taskName: task.name }), env),
    400,
    { error: "taskId must be a canonical positive decimal string" },
  );
  expect(f.configure).not.toHaveBeenCalled();
});

function singletonRequest(taskName = singletonTask.name): Request {
  return new Request("https://processor/process", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ task: { kind: "singleton", taskName } }),
  });
}

function singletonFixture() {
  const f = fixture();
  const process = vi.fn(
    async (_id: number, _payload: undefined): Promise<TaskResult<number[]>> =>
      TaskSuccess.done([73]),
  );
  const factory = { task: singletonTask, build: vi.fn(() => ({ process })) };
  const processor = createPostgresProcessor(() => ({
    connectionString: env.url,
    tasks: [...f.tasks, createPostgresProcessorTask(factory)],
    cleanup: f.cleanup,
  }));
  return { ...f, process, factory, processor };
}

test.each([
  "done",
  "scheduled",
  "failure",
  "scheduled-failure",
] as const)("singleton %s uses the claimed row ID and always returns a name-based retry", async (mode) => {
  const f = singletonFixture();
  random();
  const now = Date.now();
  const atMs = mode.includes("scheduled") ? now + 60_000 : now;
  f.process.mockResolvedValue(
    mode.includes("failure")
      ? mode === "failure"
        ? TaskFailure.retryImmediately()
        : TaskFailure.retryAt(atMs)
      : mode === "done"
        ? TaskSuccess.done([73])
        : TaskSuccess.scheduleNextRun([73], atMs),
  );
  const close = new Gate();
  f.backend.closeGate = close;
  const response = f.processor.fetch(singletonRequest(), env);
  const unsettled = pending(response);
  await f.backend.closing.wait();
  unsettled();
  close.release();
  await envelope(await response, 200, {
    task: { kind: "singleton", taskName: singletonTask.name },
    nextAction: { type: "retryAt", atMs },
  });
  expect(f.backend.claimSingleton).toHaveBeenCalledExactlyOnceWith(
    singletonTask,
    23,
    now + 20_000,
  );
  expect(f.backend.claimPublished).not.toHaveBeenCalled();
  expect(f.factory.build).toHaveBeenCalledExactlyOnceWith(23);
  expect(f.process).toHaveBeenCalledExactlyOnceWith(73, undefined);
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  if (mode.includes("failure"))
    expect(f.backend.fail).toHaveBeenCalledWith(
      23,
      73,
      mode === "failure" ? null : atMs,
    );
  else
    expect(f.backend.finish).toHaveBeenCalledWith(
      singletonTask,
      23,
      73,
      [73],
      mode === "done" ? null : atMs,
    );
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  "future",
  "leased",
  "claim",
  "finalization",
  "cleanup",
  "close",
] as const)("singleton %s preserves hints or reports sanitized uncertainty after cleanup", async (stage) => {
  const f = singletonFixture();
  random();
  const atMs = Date.now() + 60_000;
  if (stage === "future") f.backend.claimError = new TaskUnavailableError(atMs);
  if (stage === "leased") f.backend.claimError = new TaskLeasedError(atMs);
  if (stage === "claim") f.backend.claimError = secret;
  if (stage === "finalization") f.backend.finalizationError = secret;
  if (stage === "cleanup") f.cleanup.mockRejectedValue(secret);
  if (stage === "close") f.backend.close.mockRejectedValue(secret);
  const hinted = stage === "future" || stage === "leased";
  await envelope(
    await f.processor.fetch(singletonRequest(), env),
    hinted ? 200 : 500,
    hinted
      ? {
          task: { kind: "singleton", taskName: singletonTask.name },
          nextAction: { type: "retryAt", atMs },
        }
      : { error: "task processing attempt failed" },
  );
  if (hinted || stage === "claim")
    expect(f.factory.build).not.toHaveBeenCalled();
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(JSON.stringify(vi.mocked(console.error).mock.calls)).not.toContain(
    singletonTask.name,
  );
});

test("singleton lease loss does not authorize a hint and drains application work", async () => {
  const f = singletonFixture();
  random();
  f.backend.renewalDue = true;
  const business = new Gate();
  const work = business.wait();
  const started = new Gate();
  const cleaning = new Gate();
  f.process.mockImplementation(async () => {
    started.release();
    await work;
    return TaskSuccess.done([73]);
  });
  f.cleanup.mockImplementation(async () => {
    cleaning.release();
    await work;
  });
  const response = f.processor.fetch(singletonRequest(), env);
  const unsettled = pending(response);
  await started.wait();
  await vi.advanceTimersByTimeAsync(0);
  await f.backend.renewing.wait();
  f.backend.renewalGate.release();
  await cleaning.wait();
  unsettled();
  expect(f.backend.close).not.toHaveBeenCalled();
  business.release();
  await envelope(await response, 500, {
    error: "task processing attempt failed",
  });
  expect(f.backend.finish).not.toHaveBeenCalled();
  expect(f.backend.fail).not.toHaveBeenCalled();
  expect(f.backend.close).toHaveBeenCalledTimes(1);
});

test("singleton requests create and reuse one backend-managed row without publication", async () => {
  const backend = new InMemoryBackend();
  const close = vi.fn(async () => {});
  vi.spyOn(PostgresExecutionBackend, "connect").mockResolvedValue(
    Object.assign(backend, { close }) as unknown as PostgresExecutionBackend,
  );
  random([
    [0, 0, 0, 0, 0, 1],
    [0, 0, 0, 0, 0, 2],
  ]);
  const process = vi.fn(async (_id: number, _payload: undefined) =>
    TaskSuccess.done([73]),
  );
  const processor = createPostgresProcessor(() => ({
    connectionString: env.url,
    tasks: [
      createPostgresProcessorTask({
        task: singletonTask,
        build: () => ({ process }),
      }),
    ],
  }));
  for (let i = 0; i < 2; i++) {
    await envelope(await processor.fetch(singletonRequest(), env), 200, {
      task: { kind: "singleton", taskName: singletonTask.name },
      nextAction: { type: "retryAt", atMs: Date.now() },
    });
  }
  const claimed = await backend.claimSingleton(
    singletonTask,
    3,
    Date.now() + 20_000,
  );
  expect(process.mock.calls).toEqual([
    [claimed.taskId, undefined],
    [claimed.taskId, undefined],
  ]);
  expect(close).toHaveBeenCalledTimes(2);
});

test.each([
  "9007199254740992",
  "9007199254740993",
  "9223372036854775807",
])("unsafe singleton row %s cannot reach business execution or rounded finalization", async (taskId) => {
  const f = singletonFixture();
  random();
  const pool = new Pool();
  const query = vi
    .spyOn(pool, "query")
    .mockResolvedValue({ rows: [{ task_id: taskId }] } as never);
  const operations = new PostgresTaskOperations(pool);
  f.backend.claimSingleton.mockImplementation(
    operations.claimSingleton.bind(operations),
  );
  await expect(
    operations.claimSingleton(singletonTask, 23, Date.now() + 20_000),
  ).rejects.toMatchObject({ name: "PostgresSingletonTaskIdError", taskId });
  await envelope(await f.processor.fetch(singletonRequest(), env), 500, {
    error: "task processing attempt failed",
  });
  expect(query).toHaveBeenCalledTimes(2);
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(f.backend.renew).not.toHaveBeenCalled();
  expect(f.backend.finish).not.toHaveBeenCalled();
  expect(f.backend.fail).not.toHaveBeenCalled();
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(new PostgresSingletonTaskIdError(taskId).taskId).toBe(taskId);
  await pool.end();
});

test.each([
  { taskId: "17", taskName: task.name },
  { task: { kind: "singleton", taskName: singletonTask.name, taskId: "17" } },
  { task: { kind: "singleton", taskName: singletonTask.name, extra: true } },
  { task: { kind: "publish", taskName: task.name, taskId: "17" } },
  { task: { kind: "unknown", taskName: task.name } },
  { task: { kind: "published", taskName: task.name } },
  {
    task: { kind: "published", taskName: task.name, taskId: "17", extra: true },
  },
])("rejects malformed identities and old envelopes %j before configuration", async (body) => {
  const f = fixture();
  const input = new Request("https://processor/process", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  await envelope(await f.processor.fetch(input, env), 400, {
    error: invalidIdentity,
  });
  expect(f.configure).not.toHaveBeenCalled();
  expect(f.connect).not.toHaveBeenCalled();
});

test.each([
  "published",
  "singleton",
  "duplicate",
] as const)("wrong-kind or duplicate %s registration cannot acquire a backend", async (kind) => {
  const f = singletonFixture();
  const processor = createPostgresProcessor(() => ({
    connectionString: env.url,
    tasks:
      kind === "duplicate"
        ? [
            createPostgresProcessorTask(f.factory),
            createPostgresProcessorTask({
              ...f.countFactory,
              task: { ...countTask, name: singletonTask.name },
            }),
          ]
        : [createPostgresProcessorTask(f.factory), ...f.tasks],
    cleanup: f.cleanup,
  }));
  const input =
    kind === "published"
      ? request({ taskId: "17", taskName: singletonTask.name })
      : singletonRequest(kind === "singleton" ? task.name : singletonTask.name);
  await envelope(
    await processor.fetch(input, env),
    kind === "duplicate" ? 500 : 404,
    {
      error:
        kind === "duplicate"
          ? "task processing attempt failed"
          : "unknown task name",
    },
  );
  expect(f.connect).not.toHaveBeenCalled();
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(f.countFactory.build).not.toHaveBeenCalled();
  expect(f.cleanup).toHaveBeenCalledTimes(1);
});

interface Env {
  url: string;
  schema?: string;
}
const env: Env = { url: "hyperdrive-url", schema: "request_schema" };

function scope() {
  const process = vi.fn(async (_id: number, payload: { name: string }) =>
    TaskSuccess.done({ greeting: payload.name }),
  );
  const factory = { task, build: vi.fn(() => ({ process })) };
  const countProcess = vi.fn(async (_id: number, payload: { count: number }) =>
    TaskSuccess.done([payload.count]),
  );
  const countFactory = {
    task: countTask,
    build: vi.fn(() => ({ process: countProcess })),
  };
  const cleanup = vi.fn(async () => {});
  return {
    factory,
    cleanup,
    process,
    countFactory,
    countProcess,
    tasks: [
      createPostgresProcessorTask(factory),
      createPostgresProcessorTask(countFactory),
    ],
  };
}

function fixture(single = false) {
  const backend = new Execution();
  const connect = vi
    .spyOn(PostgresExecutionBackend, "connect")
    .mockResolvedValue(backend as unknown as PostgresExecutionBackend);
  const application = scope();
  const configure = vi.fn((env: Env) => ({
    connectionString: env.url,
    schema: env.schema,
    ...application,
    tasks: single ? application.tasks.slice(0, 1) : application.tasks,
  }));
  return {
    backend,
    connect,
    ...application,
    configure,
    processor: createPostgresProcessor(configure),
  };
}

function request(
  body: unknown = { taskId: "17", taskName: task.name },
): Request {
  return new Request("https://processor/process", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      task:
        body !== null && typeof body === "object" && !Array.isArray(body)
          ? { kind: "published", ...body }
          : body,
    }),
  });
}

async function envelope(
  response: Response,
  status: number,
  body: unknown,
): Promise<void> {
  expect(response.status).toBe(status);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(response.headers.get("x-content-type-options")).toBe("nosniff");
  expect(response.headers.get("content-type")).toBe(
    "application/json; charset=utf-8",
  );
  expect(await response.json()).toEqual(body);
}

function pending(response: Promise<Response>) {
  let settled = false;
  void response.then(() => {
    settled = true;
  });
  return () => expect(settled).toBe(false);
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(console, "error").mockImplementation(() => {});
});

test("application cleanup is optional", async () => {
  const f = fixture();
  const processor = createPostgresProcessor((env: Env) => ({
    connectionString: env.url,
    tasks: [createPostgresProcessorTask(f.factory)],
  }));
  await envelope(await processor.fetch(request(), env), 200, {
    task: { kind: "published", taskId: "17", taskName: task.name },
    nextAction: { type: "done" },
  });
  expect(f.cleanup).not.toHaveBeenCalled();
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).not.toHaveBeenCalled();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function random(samples = [[0, 0, 0, 0, 0, 23]]) {
  const getRandomValues = vi.fn((bytes: Uint8Array) => {
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(bytes.length).toBe(6);
    const sample = samples.shift();
    if (!sample) throw secret;
    bytes.set(sample);
    return bytes;
  });
  vi.stubGlobal("crypto", { getRandomValues });
  return getRandomValues;
}

test.each([
  ["/other", "GET", null, 404, "not-found"],
  ["/process/", "POST", "application/json", 404, "not-found"],
  ["/process", "GET", null, 405, "method-not-allowed"],
  ["/process", "PUT", "application/json", 405, "method-not-allowed"],
  ["/process", "POST", null, 415, "content-type must be application/json"],
  [
    "/process",
    "POST",
    "text/plain",
    415,
    "content-type must be application/json",
  ],
  [
    "/process",
    "POST",
    "application/problem+json",
    415,
    "content-type must be application/json",
  ],
] as const)("rejects %s %s %s without reading the body", async (path, method, mediaType, status, error) => {
  const f = fixture();
  const rng = random();
  const input = new Request(`https://processor${path}`, {
    method,
    headers: mediaType ? { "content-type": mediaType } : {},
  });
  const read = vi.spyOn(input, "json").mockRejectedValue(secret);
  const response = await f.processor.fetch(input, env);
  expect(response.headers.get("allow")).toBe(status === 405 ? "POST" : null);
  await envelope(response, status, { error });
  expect(read).not.toHaveBeenCalled();
  expect(f.configure).not.toHaveBeenCalled();
  expect(rng).not.toHaveBeenCalled();
  expect(f.connect).not.toHaveBeenCalled();
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  "malformed",
  "unreadable",
])("rejects %s JSON before configuration", async (kind) => {
  const f = fixture();
  const rng = random();
  const input = request();
  if (kind === "malformed") {
    // A real JSON parse failure rather than an injected parser result.
    await input.text();
  } else {
    vi.spyOn(input, "json").mockRejectedValue(secret);
  }
  const malformed =
    kind === "malformed"
      ? new Request(input.url, {
          method: "POST",
          headers: input.headers,
          body: "{",
        })
      : input;
  await envelope(await f.processor.fetch(malformed, env), 400, {
    error: "invalid JSON",
  });
  expect(f.configure).not.toHaveBeenCalled();
  expect(rng).not.toHaveBeenCalled();
  expect(f.connect).not.toHaveBeenCalled();
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  null,
  [],
  true,
  1,
  "17",
  {},
  { taskId: null },
  { taskId: 17 },
  ...[
    "",
    "0",
    "-1",
    "+1",
    "01",
    " 1",
    "1 ",
    "1\n",
    "1.0",
    "1.5",
    "1e1",
    "１",
    "١",
    "90071992547409910",
    "10000000000000000",
  ].map((taskId) => ({ taskId })),
])("rejects invalid body %j without side effects", async (body) => {
  const f = fixture();
  const rng = random();
  await envelope(await f.processor.fetch(request(body), env), 400, {
    error: invalidIdentity,
  });
  expect(f.configure).not.toHaveBeenCalled();
  expect(rng).not.toHaveBeenCalled();
  expect(f.connect).not.toHaveBeenCalled();
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  "9007199254740992",
  "9999999999999999",
])("rejects unsafe ID %s", async (taskId) => {
  const f = fixture();
  const rng = random();
  await envelope(
    await f.processor.fetch(request({ taskName: task.name, taskId }), env),
    400,
    {
      error: safe,
    },
  );
  expect(f.configure).not.toHaveBeenCalled();
  expect(rng).not.toHaveBeenCalled();
  expect(f.connect).not.toHaveBeenCalled();
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  "1",
  "9007199254740991",
])("executes boundary ID %s using only the claimed payload", async (taskId) => {
  const f = fixture();
  const rng = random([
    [0, 0, 0, 0, 0, 0],
    [255, 255, 255, 255, 255, 255],
  ]);
  const input = request({
    taskName: task.name,
    taskId,
  });
  input.headers.set("content-type", "Application/JSON; charset=utf-8");
  const { fetch } = f.processor;
  await envelope(await fetch(input, env), 200, {
    task: { kind: "published", taskId, taskName: task.name },
    nextAction: { type: "done" },
  });
  expect(rng).toHaveBeenCalledTimes(2);
  expect(f.configure).toHaveBeenCalledExactlyOnceWith(env);
  expect(f.connect).toHaveBeenCalledExactlyOnceWith(env.url, {
    schema: env.schema,
  });
  const workerId = 2 ** 48 - 1;
  expect(f.backend.claimPublished).toHaveBeenCalledExactlyOnceWith(
    task,
    workerId,
    Number(taskId),
    Date.now() + 20_000,
  );
  expect(f.factory.build).toHaveBeenCalledExactlyOnceWith(workerId);
  expect(f.process).toHaveBeenCalledExactlyOnceWith(Number(taskId), {
    name: "claimed",
  });
  expect(f.backend.finish).toHaveBeenCalledExactlyOnceWith(
    task,
    workerId,
    Number(taskId),
    { greeting: "claimed" },
    null,
  );
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(f.backend.claimEarliestPublished).not.toHaveBeenCalled();
  expect(f.backend.claimSingleton).not.toHaveBeenCalled();
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  undefined,
  "",
  null,
  17,
  [],
  {},
  false,
])("rejects task name %j before configuration even with one registration", async (taskName) => {
  const f = fixture(true);
  const rng = random();
  await envelope(
    await f.processor.fetch(request({ taskId: "17", taskName }), env),
    400,
    { error: invalidIdentity },
  );
  expect(f.configure).not.toHaveBeenCalled();
  expect(rng).not.toHaveBeenCalled();
  expect(f.connect).not.toHaveBeenCalled();
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(f.countFactory.build).not.toHaveBeenCalled();
  expect(f.cleanup).not.toHaveBeenCalled();
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  "empty",
  "empty-name",
  "duplicate",
  "unrelated-duplicate",
  "duplicate-unknown",
  "unknown",
  "PROCESSOR_CONTRACT",
  " processor_contract ",
  "__proto__",
  "constructor",
])("cleans up registry resolution %s without acquisition", async (kind) => {
  for (const cleanupFails of [false, true]) {
    vi.mocked(console.error).mockClear();
    const f = fixture();
    const rng = random();
    const first = createPostgresProcessorTask(f.factory);
    const second = createPostgresProcessorTask(f.countFactory);
    const invalid = [
      "empty",
      "empty-name",
      "duplicate",
      "unrelated-duplicate",
      "duplicate-unknown",
    ].includes(kind);
    let tasks = [first, second];
    if (kind === "empty") tasks = [];
    if (kind === "empty-name") {
      tasks = [
        first,
        createPostgresProcessorTask({
          ...f.countFactory,
          task: { ...countTask, name: "" },
        }),
      ];
    }
    if (kind === "duplicate") tasks = [first, first];
    if (kind === "unrelated-duplicate" || kind === "duplicate-unknown") {
      tasks = [first, second, second];
    }
    const configure = vi.fn(() => ({
      connectionString: env.url,
      tasks,
      cleanup: f.cleanup,
    }));
    if (cleanupFails) f.cleanup.mockRejectedValue(secret);
    const name = invalid && kind !== "duplicate-unknown" ? task.name : kind;
    const failed = invalid || cleanupFails;
    await envelope(
      await createPostgresProcessor(configure).fetch(
        request({ taskId: "17", taskName: name }),
        env,
      ),
      failed ? 500 : 404,
      {
        error: failed ? "task processing attempt failed" : "unknown task name",
      },
    );
    expect(configure).toHaveBeenCalledTimes(1);
    expect(rng).not.toHaveBeenCalled();
    expect(f.connect).not.toHaveBeenCalled();
    expect(f.factory.build).not.toHaveBeenCalled();
    expect(f.countFactory.build).not.toHaveBeenCalled();
    expect(f.backend.claimPublished).not.toHaveBeenCalled();
    expect(f.cleanup).toHaveBeenCalledTimes(1);
    expect(f.backend.close).not.toHaveBeenCalled();
    expect(vi.mocked(console.error).mock.calls).toEqual([
      ...(invalid
        ? [["task processing attempt failed published configuration"]]
        : []),
      ...(cleanupFails
        ? [["task processing attempt failed published application-cleanup"]]
        : []),
    ]);
  }
});

test.each([
  "greeting",
  "count",
])("routes %s using its own codec, claimed payload and callback", async (kind) => {
  const f = fixture();
  random();
  const count = kind === "count";
  const definition = count ? countTask : task;
  const claimed = count ? { count: 42 } : { name: "claimed" };
  f.backend.persistedName = definition.name;
  f.backend.persistedPayload = JSON.stringify(claimed);
  const decode = vi.spyOn(definition.codec, "decode");
  const otherDecode = vi.spyOn((count ? task : countTask).codec, "decode");
  await envelope(
    await f.processor.fetch(
      request({
        taskId: "17",
        taskName: definition.name,
      }),
      env,
    ),
    200,
    {
      task: { kind: "published", taskId: "17", taskName: definition.name },
      nextAction: { type: "done" },
    },
  );
  expect(f.backend.claimPublished).toHaveBeenCalledExactlyOnceWith(
    definition,
    23,
    17,
    Date.now() + 20_000,
  );
  expect(decode).toHaveBeenCalledExactlyOnceWith(JSON.stringify(claimed));
  expect(otherDecode).not.toHaveBeenCalled();
  expect(
    (count ? f.countFactory : f.factory).build,
  ).toHaveBeenCalledExactlyOnceWith(23);
  expect((count ? f.factory : f.countFactory).build).not.toHaveBeenCalled();
  expect(count ? f.countProcess : f.process).toHaveBeenCalledExactlyOnceWith(
    17,
    claimed,
  );
  expect(count ? f.process : f.countProcess).not.toHaveBeenCalled();
  expect(f.backend.finish).toHaveBeenCalledExactlyOnceWith(
    definition,
    23,
    17,
    count ? [42] : { greeting: "claimed" },
    null,
  );
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  false,
  true,
])("awaits cleanup before a registry rejection (invalid=%s)", async (invalid) => {
  const f = fixture();
  const rng = random();
  const started = new Gate();
  const cleaning = new Gate();
  f.cleanup.mockImplementation(async () => {
    started.release();
    await cleaning.wait();
  });
  const task = createPostgresProcessorTask(f.factory);
  const processor = createPostgresProcessor(() => ({
    connectionString: env.url,
    tasks: invalid ? [task, task] : [task],
    cleanup: f.cleanup,
  }));
  const response = processor.fetch(
    request({ taskId: "17", taskName: "unknown" }),
    env,
  );
  const stillPending = pending(response);
  await started.wait();
  stillPending();
  expect(rng).not.toHaveBeenCalled();
  expect(f.connect).not.toHaveBeenCalled();
  cleaning.release();
  await envelope(await response, invalid ? 500 : 404, {
    error: invalid ? "task processing attempt failed" : "unknown task name",
  });
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).not.toHaveBeenCalled();
});

test("a persisted-name mismatch cannot decode or build either worker", async () => {
  const f = fixture();
  random();
  f.backend.persistedName = countTask.name;
  f.backend.persistedPayload = '{"count":42}';
  const decode = vi.spyOn(task.codec, "decode");
  const otherDecode = vi.spyOn(countTask.codec, "decode");
  await envelope(await f.processor.fetch(request(), env), 200, {
    task: { kind: "published", taskId: "17", taskName: task.name },
    nextAction: { type: "done" },
  });
  expect(f.backend.claimPublished).toHaveBeenCalledExactlyOnceWith(
    task,
    23,
    17,
    Date.now() + 20_000,
  );
  expect(decode).not.toHaveBeenCalled();
  expect(otherDecode).not.toHaveBeenCalled();
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(f.countFactory.build).not.toHaveBeenCalled();
  expect(f.backend.finish).not.toHaveBeenCalled();
  expect(f.backend.fail).not.toHaveBeenCalled();
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  "__proto__",
  "constructor",
  " ",
  "雪".repeat(300),
])("matches the exact registered name %s safely", async (name) => {
  const f = fixture();
  random();
  const definition = { ...task, name };
  f.backend.persistedName = name;
  const processor = createPostgresProcessor(() => ({
    connectionString: env.url,
    tasks: [createPostgresProcessorTask({ ...f.factory, task: definition })],
    cleanup: f.cleanup,
  }));
  await envelope(
    await processor.fetch(request({ taskId: "17", taskName: name }), env),
    200,
    {
      task: { kind: "published", taskId: "17", taskName: definition.name },
      nextAction: { type: "done" },
    },
  );
  expect(f.factory.build).toHaveBeenCalledExactlyOnceWith(23);
  expect(f.backend.claimPublished).toHaveBeenCalledExactlyOnceWith(
    definition,
    23,
    17,
    Date.now() + 20_000,
  );
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).not.toHaveBeenCalled();
});

test("annotated environments infer task payload and callback types", () => {
  const factory: WorkerFactory<typeof task> = scope().factory;
  const countFactory: WorkerFactory<typeof countTask> = scope().countFactory;
  const tasks = [
    createPostgresProcessorTask(factory),
    createPostgresProcessorTask(countFactory),
  ] as const satisfies readonly PostgresProcessorTask[];
  const processor = createPostgresProcessor((env: Env) => ({
    connectionString: env.url,
    tasks,
  }));
  expectTypeOf(processor.fetch).parameters.toEqualTypeOf<[Request, Env]>();
  expectTypeOf(factory.build(1).process).returns.resolves.toEqualTypeOf<
    TaskResult<{ greeting: string }>
  >();
  expectTypeOf(countFactory.build(1).process).returns.resolves.toEqualTypeOf<
    TaskResult<number[]>
  >();
  createPostgresProcessor((env: Env) => ({
    connectionString: env.url,
    tasks: [
      createPostgresProcessorTask({
        task,
        build() {
          return {
            async process(id, payload) {
              expectTypeOf(id).toEqualTypeOf<number>();
              expectTypeOf(payload).toEqualTypeOf<{ name: string }>();
              return TaskSuccess.done({ greeting: payload.name });
            },
          };
        },
      }),
      createPostgresProcessorTask({
        task: countTask,
        build: () => ({
          async process(id, payload) {
            expectTypeOf(id).toEqualTypeOf<number>();
            expectTypeOf(payload).toEqualTypeOf<{ count: number }>();
            return TaskSuccess.done([payload.count]);
          },
        }),
      }),
    ],
  }));
  createPostgresProcessorTask({
    task: defineSingletonTask<number[]>("singleton"),
    build: () => ({
      async process(id, payload) {
        expectTypeOf(id).toEqualTypeOf<number>();
        expectTypeOf(payload).toEqualTypeOf<undefined>();
        return TaskSuccess.done([id]);
      },
    }),
  });
  createPostgresProcessorTask({
    task,
    build: () => ({
      // @ts-expect-error The worker payload must match the task definition.
      process: async (_id: number, _payload: { wrong: boolean }) =>
        TaskSuccess.done({ greeting: "done" }),
    }),
  });
  const oldConfig: PostgresProcessorConfig = {
    connectionString: env.url,
    // @ts-expect-error Single-factory configuration is not supported.
    factory,
  };
  expect(oldConfig).toBeDefined();
});

test.each([
  [new TaskNotFoundError(), { type: "done" }],
  [
    new TaskLeasedError(1_700_000_000_000),
    { type: "retryAt", atMs: 1_700_000_000_000 },
  ],
  [
    new TaskUnavailableError(1_900_000_000_000),
    { type: "retryAt", atMs: 1_900_000_000_000 },
  ],
  [new TaskUnavailableError(null), null],
  [secret, null],
] as const)("no claim (%s) reports its disposition after cleanup", async (error, nextAction) => {
  const f = fixture();
  random();
  f.backend.claimError = error;
  await envelope(
    await f.processor.fetch(request(), env),
    nextAction ? 200 : 500,
    nextAction
      ? {
          task: { kind: "published", taskId: "17", taskName: task.name },
          nextAction,
        }
      : { error: "task processing attempt failed" },
  );
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).toHaveBeenCalledTimes(nextAction ? 0 : 1);
});

test.each([
  "success",
  "failure",
  "rejection",
] as const)("awaits %s finalization, application cleanup, and shutdown", async (outcome) => {
  const f = fixture();
  random();
  const started = new Gate();
  const processing = new Gate();
  const cleaning = new Gate();
  const cleanupGate = new Gate();
  f.backend.recordingGate = new Gate();
  f.backend.closeGate = new Gate();
  const factory: WorkerFactory<typeof task> = {
    task,
    build: () => ({
      async process(): Promise<TaskResult<{ greeting: string }>> {
        started.release();
        await processing.wait();
        if (outcome === "rejection") throw secret;
        return outcome === "failure"
          ? TaskFailure.retryImmediately()
          : TaskSuccess.done({ greeting: "done" });
      },
    }),
  };
  const cleanup = vi.fn(async () => {
    cleaning.release();
    await cleanupGate.wait();
  });
  const response = createPostgresProcessor((env: Env) => ({
    connectionString: env.url,
    tasks: [createPostgresProcessorTask(factory)],
    cleanup,
  })).fetch(request(), env);
  const stillPending = pending(response);
  await started.wait();
  stillPending();
  processing.release();
  await f.backend.recording.wait();
  stillPending();
  expect(cleanup).not.toHaveBeenCalled();
  f.backend.recordingGate.release();
  await cleaning.wait();
  stillPending();
  expect(f.backend.close).not.toHaveBeenCalled();
  cleanupGate.release();
  await f.backend.closing.wait();
  stillPending();
  f.backend.closeGate.release();
  await envelope(await response, 200, {
    task: { kind: "published", taskId: "17", taskName: task.name },
    nextAction:
      outcome === "success"
        ? { type: "done" }
        : { type: "retryAt", atMs: Date.now() },
  });
  expect(
    outcome === "success" ? f.backend.finish : f.backend.fail,
  ).toHaveBeenCalledTimes(1);
  expect(cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  "success",
  "failure",
  "reschedule",
  "scheduled-failure",
] as const)("finalization errors return uncertainty (%s)", async (outcome) => {
  for (const error of [secret, new LeaseLostError()]) {
    const f = fixture();
    const atMs = Date.now() + 60_000;
    f.backend.finalizationError = error;
    f.backend.recordingGate = new Gate();
    const factory: WorkerFactory<typeof task> = {
      task,
      build: () => ({
        process: async () => {
          switch (outcome) {
            case "success":
              return TaskSuccess.done({ greeting: "done" });
            case "failure":
              return TaskFailure.retryImmediately();
            case "reschedule":
              return TaskSuccess.scheduleNextRun({ greeting: "done" }, atMs);
            case "scheduled-failure":
              return TaskFailure.retryAt(atMs);
          }
        },
      }),
    };
    const response = createPostgresProcessor((env: Env) => ({
      connectionString: env.url,
      tasks: [createPostgresProcessorTask(factory)],
      cleanup: f.cleanup,
    })).fetch(request(), env);
    const stillPending = pending(response);
    await f.backend.recording.wait();
    stillPending();
    expect(f.cleanup).not.toHaveBeenCalled();
    f.backend.recordingGate.release();
    await envelope(await response, 500, {
      error: "task processing attempt failed",
    });
    expect(f.cleanup).toHaveBeenCalledTimes(1);
    expect(f.backend.close).toHaveBeenCalledTimes(1);
  }
  expect(vi.mocked(console.error).mock.calls).toEqual([
    ["task processing attempt failed published attempt"],
    ["task processing attempt failed published attempt"],
  ]);
});

test.each([
  "future",
  "leased",
  "failure",
  "reschedule",
] as const)("preserves %s deadlines through delayed query, cleanup and close", async (kind) => {
  for (const offset of [-60_000, 60_000]) {
    const f = fixture();
    const atMs = Date.now() + offset;
    const query = new Gate();
    const querying = new Gate();
    const cleaning = new Gate();
    const cleanupGate = new Gate();
    f.backend.closeGate = new Gate();
    if (kind === "future" || kind === "leased") {
      vi.mocked(f.backend.claimPublished).mockImplementation(async () => {
        querying.release();
        await query.wait();
        throw kind === "future"
          ? new TaskUnavailableError(atMs)
          : new TaskLeasedError(atMs);
      });
    }
    const factory: WorkerFactory<typeof task> = {
      task,
      build: () => ({
        process: async () =>
          kind === "failure"
            ? TaskFailure.retryAt(atMs)
            : TaskSuccess.scheduleNextRun({ greeting: "done" }, atMs),
      }),
    };
    f.cleanup.mockImplementation(async () => {
      cleaning.release();
      await cleanupGate.wait();
    });
    const response = createPostgresProcessor(() => ({
      connectionString: env.url,
      tasks: [createPostgresProcessorTask(factory)],
      cleanup: f.cleanup,
    })).fetch(request(), env);
    const stillPending = pending(response);
    if (kind === "future" || kind === "leased") {
      await querying.wait();
      stillPending();
      await vi.advanceTimersByTimeAsync(321);
      query.release();
    }
    await cleaning.wait();
    stillPending();
    await vi.advanceTimersByTimeAsync(321);
    cleanupGate.release();
    await f.backend.closing.wait();
    stillPending();
    await vi.advanceTimersByTimeAsync(321);
    f.backend.closeGate.release();
    await envelope(await response, 200, {
      task: { kind: "published", taskId: "17", taskName: task.name },
      nextAction: { type: "retryAt", atMs },
    });
    expect(f.backend.finish).toHaveBeenCalledTimes(
      kind === "reschedule" ? 1 : 0,
    );
    expect(f.backend.fail).toHaveBeenCalledTimes(kind === "failure" ? 1 : 0);
  }
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  NaN,
  Infinity,
  -Infinity,
  -1,
  1.5,
  8_640_000_000_000_001,
  Number.MAX_SAFE_INTEGER,
])("rejects an unrepresentable deadline %s after owned cleanup", async (atMs) => {
  const f = fixture();
  f.process.mockResolvedValue(
    TaskSuccess.scheduleNextRun({ greeting: "done" }, atMs),
  );
  await envelope(await f.processor.fetch(request(), env), 500, {
    error: "task processing attempt failed",
  });
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).toHaveBeenCalledExactlyOnceWith(
    "task processing attempt failed published attempt",
  );
});

test.each([
  0, 8_640_000_000_000_000,
])("accepts the Date boundary %s", async (atMs) => {
  const f = fixture();
  f.backend.claimError = new TaskUnavailableError(atMs);
  await envelope(await f.processor.fetch(request(), env), 200, {
    task: { kind: "published", taskId: "17", taskName: task.name },
    nextAction: { type: "retryAt", atMs },
  });
});

test.each([
  "configuration",
  "worker-id",
  "acquisition",
  "attempt",
  "application-cleanup",
  "backend-close",
] as const)("sanitizes %s failure and cleans up acquired resources", async (stage) => {
  const f = fixture();
  random();
  if (stage === "configuration")
    f.configure.mockImplementation(() => {
      throw secret;
    });
  if (stage === "worker-id") random([]);
  if (stage === "acquisition") f.connect.mockRejectedValue(secret);
  if (stage === "attempt")
    f.factory.build.mockImplementation(() => {
      throw secret;
    });
  if (stage === "application-cleanup") f.cleanup.mockRejectedValue(secret);
  if (stage === "backend-close") f.backend.close.mockRejectedValue(secret);
  await envelope(await f.processor.fetch(request(), env), 500, {
    error: "task processing attempt failed",
  });
  expect(console.error).toHaveBeenCalledExactlyOnceWith(
    `task processing attempt failed published ${stage}`,
  );
  expect(f.cleanup).toHaveBeenCalledTimes(stage === "configuration" ? 0 : 1);
  expect(f.backend.close).toHaveBeenCalledTimes(
    ["configuration", "worker-id", "acquisition"].includes(stage) ? 0 : 1,
  );
});

test("reports every failure without skipping shutdown", async () => {
  const f = fixture();
  f.factory.build.mockImplementation(() => {
    throw secret;
  });
  f.cleanup.mockRejectedValue(secret);
  f.backend.close.mockRejectedValue(secret);
  await envelope(await f.processor.fetch(request(), env), 500, {
    error: "task processing attempt failed",
  });
  expect(vi.mocked(console.error).mock.calls).toEqual(
    ["attempt", "application-cleanup", "backend-close"].map((stage) => [
      `task processing attempt failed published ${stage}`,
    ]),
  );
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
});

test("fresh concurrent scopes and backends do not share cleanup", async () => {
  const backends = [new Execution(), new Execution()];
  backends[1].persistedName = countTask.name;
  backends[1].persistedPayload = '{"count":42}';
  const scopes = [scope(), scope()];
  const gates = [new Gate(), new Gate()];
  for (const [i, backend] of backends.entries()) backend.closeGate = gates[i];
  const connect = vi.spyOn(PostgresExecutionBackend, "connect");
  for (const backend of backends)
    connect.mockResolvedValueOnce(
      backend as unknown as PostgresExecutionBackend,
    );
  const configure = vi.fn((env: Env) => ({
    connectionString: env.url,
    ...scopes[env.url === "first" ? 0 : 1],
  }));
  const processor = createPostgresProcessor(configure);
  expect(configure).not.toHaveBeenCalled();
  random([
    [0, 0, 0, 0, 0, 1],
    [0, 0, 0, 0, 0, 2],
  ]);
  const first = processor.fetch(request(), { url: "first" });
  const second = processor.fetch(
    request({ taskName: countTask.name, taskId: "18" }),
    { url: "second" },
  );
  const firstPending = pending(first);
  const secondPending = pending(second);
  await Promise.all(backends.map((backend) => backend.closing.wait()));
  firstPending();
  secondPending();
  gates[1].release();
  await envelope(await second, 200, {
    task: { kind: "published", taskId: "18", taskName: countTask.name },
    nextAction: { type: "done" },
  });
  firstPending();
  gates[0].release();
  await envelope(await first, 200, {
    task: { kind: "published", taskId: "17", taskName: task.name },
    nextAction: { type: "done" },
  });
  expect(configure).toHaveBeenCalledTimes(2);
  expect(connect.mock.calls).toEqual([
    ["first", { schema: undefined }],
    ["second", { schema: undefined }],
  ]);
  for (const [i, application] of scopes.entries()) {
    expect(
      (i === 0 ? application.factory : application.countFactory).build,
    ).toHaveBeenCalledExactlyOnceWith(i + 1);
    expect(
      (i === 0 ? application.countFactory : application.factory).build,
    ).not.toHaveBeenCalled();
    expect(backends[i].claimPublished).toHaveBeenCalledExactlyOnceWith(
      i === 0 ? task : countTask,
      i + 1,
      17 + i,
      Date.now() + 20_000,
    );
    expect(application.cleanup).toHaveBeenCalledTimes(1);
    expect(backends[i].close).toHaveBeenCalledTimes(1);
  }
});

test("renewal loss does not cancel business work; registered cleanup drains it", async () => {
  const f = fixture();
  f.backend.renewalDue = true;
  const started = new Gate();
  const businessGate = new Gate();
  const cleanupStarted = new Gate();
  let businessFinished = false;
  let operation: Promise<TaskSuccess<{ greeting: string }>> | undefined;
  const cleanup = vi.fn(async () => {
    cleanupStarted.release();
    await operation;
  });
  const processor = createPostgresProcessor((env: Env) => ({
    connectionString: env.url,
    tasks: [
      createPostgresProcessorTask({
        task,
        build: () => ({
          process: () => {
            operation = (async () => {
              started.release();
              try {
                await businessGate.wait();
                return TaskSuccess.done({ greeting: "drained" });
              } finally {
                businessFinished = true;
              }
            })();
            return operation;
          },
        }),
      }),
    ],
    cleanup,
  }));
  const response = processor.fetch(request(), env);
  const stillPending = pending(response);
  await started.wait();
  await vi.advanceTimersByTimeAsync(0);
  await f.backend.renewing.wait();
  f.backend.renewalGate.release();
  await cleanupStarted.wait();
  stillPending();
  expect(businessFinished).toBe(false);
  expect(f.backend.close).not.toHaveBeenCalled();
  businessGate.release();
  await envelope(await response, 500, {
    error: "task processing attempt failed",
  });
  expect(businessFinished).toBe(true);
  expect(cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(f.backend.finish).not.toHaveBeenCalled();
  expect(f.backend.fail).not.toHaveBeenCalled();
  expect(console.error).toHaveBeenCalledExactlyOnceWith(
    "task processing attempt failed published attempt",
  );
});
