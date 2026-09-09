import { afterEach, beforeEach, expect, expectTypeOf, test, vi } from "vitest";
import { PostgresExecutionBackend } from "../src/backends/postgres-execution.js";
import { createPostgresProcessor } from "../src/cloudflare/postgres.js";
import {
  type ClaimedTask,
  definePublishTask,
  defineSingletonTask,
  LeaseLostError,
  type PublishTaskDefinition,
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
const secret = new Error("postgres://user:secret@private/database");
const canonical = "taskId must be a canonical positive decimal string";
const safe = "taskId must encode a positive safe integer canonically";

class Execution implements TaskExecutionBackend {
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
    if (this.claimError) throw this.claimError;
    return {
      taskId,
      taskPayload: definition.codec.decode('{"name":"claimed"}'),
      leaseExpirationMs: this.renewalDue ? Date.now() : leaseExpirationMs,
    };
  }
  claimEarliestPublished = vi.fn(async (): Promise<never> => {
    throw new Error("must claim the explicit ID");
  });
  claimSingleton = vi.fn(async (): Promise<never> => {
    throw new Error("must claim a published task");
  });
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
  const cleanup = vi.fn(async () => {});
  return { factory, cleanup, process };
}

function fixture() {
  const backend = new Execution();
  const connect = vi
    .spyOn(PostgresExecutionBackend, "connect")
    .mockResolvedValue(backend as unknown as PostgresExecutionBackend);
  const application = scope();
  const configure = vi.fn((env: Env) => ({
    connectionString: env.url,
    schema: env.schema,
    ...application,
  }));
  return {
    backend,
    connect,
    ...application,
    configure,
    processor: createPostgresProcessor(configure),
  };
}

function request(body: unknown = { taskId: "17" }): Request {
  return new Request("https://processor/process", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
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
    factory: f.factory,
  }));
  await envelope(await processor.fetch(request(), env), 200, {
    taskId: "17",
    attemptFinished: true,
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
    error: canonical,
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
  await envelope(await f.processor.fetch(request({ taskId }), env), 400, {
    error: safe,
  });
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
  const input = request({ taskId, payload: { name: "untrusted" } });
  input.headers.set("content-type", "Application/JSON; charset=utf-8");
  const { fetch } = f.processor;
  await envelope(await fetch(input, env), 200, {
    taskId,
    attemptFinished: true,
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

test("annotated environments infer task payload and callback types", () => {
  const factory: WorkerFactory<typeof task> = scope().factory;
  const processor = createPostgresProcessor((env: Env) => ({
    connectionString: env.url,
    factory,
  }));
  expectTypeOf(processor.fetch).parameters.toEqualTypeOf<[Request, Env]>();
  expectTypeOf(factory.build(1).process).returns.resolves.toEqualTypeOf<
    TaskResult<{ greeting: string }>
  >();
  createPostgresProcessor((env: Env) => ({
    connectionString: env.url,
    factory: {
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
    },
  }));
  const singleton = { task: defineSingletonTask("singleton"), build: vi.fn() };
  // @ts-expect-error A wire ID must never be interpreted as singleton activation.
  createPostgresProcessor((env: Env) => ({
    connectionString: env.url,
    factory: singleton,
  }));
  // @ts-expect-error The worker payload must match the task definition.
  createPostgresProcessor((env: Env) => ({
    connectionString: env.url,
    factory: {
      task,
      build: () => ({
        process: async (_id: number, _payload: { wrong: boolean }) =>
          TaskSuccess.done({ greeting: "done" }),
      }),
    },
  }));
});

test.each([
  new TaskNotFoundError(),
  new TaskLeasedError(Date.now() + 60_000),
  new TaskUnavailableError(null),
  secret,
])("no claim (%s) is a cleaned-up normal attempt", async (error) => {
  const f = fixture();
  random();
  f.backend.claimError = error;
  await envelope(await f.processor.fetch(request(), env), 200, {
    taskId: "17",
    attemptFinished: true,
  });
  expect(f.factory.build).not.toHaveBeenCalled();
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).not.toHaveBeenCalled();
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
    factory,
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
  await envelope(await response, 200, { taskId: "17", attemptFinished: true });
  expect(
    outcome === "success" ? f.backend.finish : f.backend.fail,
  ).toHaveBeenCalledTimes(1);
  expect(cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).not.toHaveBeenCalled();
});

test.each([
  false,
  true,
])("runtime-swallowed finalization errors remain normal (failure=%s)", async (failed) => {
  const f = fixture();
  f.backend.finalizationError = secret;
  const factory: WorkerFactory<typeof task> = {
    task,
    build: () => ({
      process: async () =>
        failed
          ? TaskFailure.retryImmediately()
          : TaskSuccess.done({ greeting: "done" }),
    }),
  };
  await envelope(
    await createPostgresProcessor((env: Env) => ({
      connectionString: env.url,
      factory,
      cleanup: f.cleanup,
    })).fetch(request(), env),
    200,
    { taskId: "17", attemptFinished: true },
  );
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(console.error).not.toHaveBeenCalled();
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
    `task processing attempt failed 17 ${stage}`,
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
      `task processing attempt failed 17 ${stage}`,
    ]),
  );
  expect(f.cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
});

test("fresh concurrent scopes and backends do not share cleanup", async () => {
  const backends = [new Execution(), new Execution()];
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
  const second = processor.fetch(request({ taskId: "18" }), { url: "second" });
  const firstPending = pending(first);
  const secondPending = pending(second);
  await Promise.all(backends.map((backend) => backend.closing.wait()));
  firstPending();
  secondPending();
  gates[1].release();
  await envelope(await second, 200, { taskId: "18", attemptFinished: true });
  firstPending();
  gates[0].release();
  await envelope(await first, 200, { taskId: "17", attemptFinished: true });
  expect(configure).toHaveBeenCalledTimes(2);
  expect(connect.mock.calls).toEqual([
    ["first", { schema: undefined }],
    ["second", { schema: undefined }],
  ]);
  for (const [i, application] of scopes.entries()) {
    expect(application.factory.build).toHaveBeenCalledExactlyOnceWith(i + 1);
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
    factory: {
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
    },
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
  await envelope(await response, 200, { taskId: "17", attemptFinished: true });
  expect(businessFinished).toBe(true);
  expect(cleanup).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(f.backend.finish).not.toHaveBeenCalled();
  expect(f.backend.fail).not.toHaveBeenCalled();
  expect(console.error).not.toHaveBeenCalled();
});
