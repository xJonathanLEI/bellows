import { afterEach, beforeEach, expect, expectTypeOf, test, vi } from "vitest";
import {
  PostgresPublishedTaskIdError,
  PostgresPublishingBackend,
} from "../src/backends/postgres-publishing.js";
import {
  createPostgresPublisher,
  type PostgresPublisherConfig,
  PostgresPublisherError,
  type PostgresPublisherReceipt,
  type PostgresPublisherStage,
} from "../src/cloudflare/postgres.js";
import {
  definePublishTask,
  defineSingletonTask,
  type PublishedTask,
  type PublishTaskDefinition,
  type TaskPublishingBackend,
} from "../src/types.js";
import { Gate } from "./helpers.js";

const task = definePublishTask<{ name: string }, { greeting: string }>(
  "publisher_contract",
);
const secret = new Error("postgres://user:secret@private/database");
const closeSecret = new Error("private shutdown failure");

class Publishing implements TaskPublishingBackend {
  taskId = 17;
  readonly publishing = new Gate();
  publishGate: Gate | undefined;
  readonly closing = new Gate();
  closeGate: Gate | undefined;
  readonly encoded: string[] = [];
  readonly events: string[] = [];

  constructor() {
    vi.spyOn(this as Publishing, "publish");
  }

  async publish<TPayload, TCallback>(
    definition: PublishTaskDefinition<TPayload, TCallback>,
    payload: TPayload,
  ): Promise<PublishedTask> {
    this.events.push("publish");
    this.publishing.release();
    await this.publishGate?.wait();
    this.encoded.push(definition.codec.encode(payload));
    return { taskId: this.taskId };
  }

  publishFuture = vi.fn(async (): Promise<never> => {
    throw new Error("must publish immediately");
  });

  close = vi.fn(async () => {
    this.events.push("close");
    this.closing.release();
    await this.closeGate?.wait();
  });
}

function receiver(events: string[] = []) {
  const dispatched = new Gate();
  const fetch = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit) => {
      events.push("dispatch");
      dispatched.release();
      return new Response("accepted");
    },
  );
  const getByName = vi.fn((_name: string) => {
    events.push("lookup");
    return { fetch };
  });
  return { dispatcher: { getByName }, fetch, dispatched };
}

interface Env {
  url: string;
  schema?: string;
}
const env: Env = { url: "hyperdrive-url", schema: "request_schema" };
const payload = { name: "Ada" };

function fixture() {
  const backend = new Publishing();
  const connect = vi
    .spyOn(PostgresPublishingBackend, "connect")
    .mockImplementation(async () => {
      backend.events.push("acquire");
      return backend as unknown as PostgresPublishingBackend;
    });
  const dispatch = receiver(backend.events);
  const configure = vi.fn((env: Env) => {
    backend.events.push("configure");
    return {
      connectionString: env.url,
      schema: env.schema,
      task,
      dispatcher: dispatch.dispatcher,
    };
  });
  return {
    backend,
    connect,
    configure,
    ...dispatch,
    publisher: createPostgresPublisher(configure),
  };
}

function pending<T>(promise: Promise<T>) {
  let settled = false;
  void promise.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  return () => expect(settled).toBe(false);
}

async function failure(
  promise: Promise<PostgresPublisherReceipt>,
  stage: PostgresPublisherStage,
  taskId?: string,
): Promise<PostgresPublisherError> {
  const result = await promise.catch((error: unknown) => error);
  expect(result).toBeInstanceOf(PostgresPublisherError);
  const error = result as PostgresPublisherError;
  expect(error.stage).toBe(stage);
  expect(error.message).toBe(`PostgreSQL publisher failed at ${stage}`);
  expect(String(error)).not.toContain(secret.message);
  expect(error.receipt).toEqual(taskId === undefined ? undefined : { taskId });
  if (error.receipt) expect(Object.isFrozen(error.receipt)).toBe(true);
  return error;
}

beforeEach(() => {
  for (const method of ["error", "warn", "log", "info", "debug"] as const) {
    vi.spyOn(console, method).mockImplementation(() => {});
  }
});

afterEach(() => {
  for (const method of ["error", "warn", "log", "info", "debug"] as const) {
    expect(console[method]).not.toHaveBeenCalled();
  }
  vi.restoreAllMocks();
});

test("construction is inert; detached publication forwards configuration, task and payload", async () => {
  const f = fixture();
  expect(f.configure).not.toHaveBeenCalled();
  expect(f.connect).not.toHaveBeenCalled();
  expect(Object.keys(f.publisher)).toEqual(["publish"]);
  const encode = vi.spyOn(task.codec, "encode");
  const callback = vi.spyOn(task.callbackCodec, "encode");
  const { publish } = f.publisher;
  const receipt = await publish(env, payload);
  expect(receipt).toEqual({ taskId: "17" });
  expect(Object.isFrozen(receipt)).toBe(true);
  expect(f.configure).toHaveBeenCalledExactlyOnceWith(env);
  expect(f.connect).toHaveBeenCalledExactlyOnceWith(env.url, {
    schema: env.schema,
  });
  expect(f.backend.publish).toHaveBeenCalledExactlyOnceWith(task, payload);
  expect(encode).toHaveBeenCalledExactlyOnceWith(payload);
  expect(callback).not.toHaveBeenCalled();
  expect(f.backend.encoded).toEqual(['{"name":"Ada"}']);
  expect(f.backend.publishFuture).not.toHaveBeenCalled();
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(f.dispatcher.getByName).toHaveBeenCalledExactlyOnceWith("global");
  expect(f.fetch).toHaveBeenCalledExactlyOnceWith(
    "https://dispatcher/dispatch",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"taskId":"17"}',
    },
  );
  expect(f.backend.events).toEqual([
    "configure",
    "acquire",
    "publish",
    "close",
    "lookup",
    "dispatch",
  ]);
});

test("void payloads and custom codecs use plain publication", async () => {
  const f = fixture();
  const voidTask = definePublishTask<void>("void");
  const publisher = createPostgresPublisher((env: Env) => ({
    connectionString: env.url,
    task: voidTask,
    dispatcher: f.dispatcher,
  }));
  await publisher.publish(env, undefined);
  const custom = definePublishTask<{ name: string }>("custom", {
    encode: vi.fn(({ name }) => name.toUpperCase()),
    decode: vi.fn((name) => ({ name })),
  });
  await createPostgresPublisher((env: Env) => ({
    connectionString: env.url,
    task: custom,
    dispatcher: f.dispatcher,
  })).publish(env, payload);
  expect(f.backend.encoded).toEqual(["null", "ADA"]);
  expect(custom.codec.encode).toHaveBeenCalledExactlyOnceWith(payload);
  expect(custom.codec.decode).not.toHaveBeenCalled();
});

test("environment, payload, callback-bearing task and error types remain precise", () => {
  const f = fixture();
  expectTypeOf(f.publisher.publish).parameters.toEqualTypeOf<
    [Env, { name: string }]
  >();
  expectTypeOf(
    f.publisher.publish,
  ).returns.resolves.toEqualTypeOf<PostgresPublisherReceipt>();
  const config: PostgresPublisherConfig<typeof task> = f.configure(env);
  expectTypeOf(config.task).toEqualTypeOf<typeof task>();
  function unsupported(
    error: PostgresPublisherError,
    receipt: PostgresPublisherReceipt,
  ) {
    expectTypeOf(error.stage).toEqualTypeOf<PostgresPublisherStage>();
    expectTypeOf(error.cause).toEqualTypeOf<unknown>();
    expectTypeOf(error.receipt).toEqualTypeOf<
      PostgresPublisherReceipt | undefined
    >();
    // @ts-expect-error The payload must match the bound definition.
    void f.publisher.publish(env, { name: 1 });
    // @ts-expect-error The environment is inferred from the callback.
    void f.publisher.publish({ other: true }, payload);
    // @ts-expect-error Only immediate publication is supported.
    void f.publisher.publishFuture(env, payload, 1);
    // @ts-expect-error No callback registration or awaitable result.
    void f.publisher.publishAwaitable(env, payload);
    // @ts-expect-error Recovery uses the separate dispatch helper.
    void f.publisher.redispatch(env, "17");
    // @ts-expect-error No application cleanup or lifecycle methods.
    void f.publisher.close();
    // @ts-expect-error Receipts are readonly.
    receipt.taskId = "18";
  }
  expect(unsupported).toBeTypeOf("function");
  // @ts-expect-error Singleton activation cannot be published.
  createPostgresPublisher((env: Env) => ({
    connectionString: env.url,
    task: defineSingletonTask("singleton"),
    dispatcher: f.dispatcher,
  }));
  // @ts-expect-error Configuration must be synchronous.
  createPostgresPublisher(async (_env: Env) => config);
});

test.each([
  1,
  Number.MAX_SAFE_INTEGER,
])("dispatches safe ID %s exactly", async (taskId) => {
  const f = fixture();
  f.backend.taskId = taskId;
  expect(await f.publisher.publish(env, payload)).toEqual({
    taskId: String(taskId),
  });
  expect(f.fetch.mock.calls[0][1]?.body).toBe(
    JSON.stringify({ taskId: String(taskId) }),
  );
});

test.each([
  0,
  -1,
  1.5,
  NaN,
  Infinity,
  9007199254740992,
])("rejects invalid numeric backend receipt %s after retaining and closing", async (taskId) => {
  const f = fixture();
  f.backend.taskId = taskId;
  const error = await failure(
    f.publisher.publish(env, payload),
    "task-id",
    String(taskId),
  );
  expect(error.cause).toBeInstanceOf(Error);
  expect(error.backendCloseError).toBeUndefined();
  expect(f.backend.publish).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(f.dispatcher.getByName).not.toHaveBeenCalled();
});

test.each([
  "9007199254740992",
  "9007199254740993",
  "9223372036854775807",
])("preserves exact known publication error ID %s", async (taskId) => {
  const f = fixture();
  const cause = new PostgresPublishedTaskIdError(taskId);
  vi.mocked(f.backend.publish).mockRejectedValue(cause);
  const error = await failure(
    f.publisher.publish(env, payload),
    "task-id",
    taskId,
  );
  expect(error.cause).toBe(cause);
  expect(error.backendCloseError).toBeUndefined();
  expect(f.backend.publish).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(f.dispatcher.getByName).not.toHaveBeenCalled();
});

test.each([
  200, 503,
])("awaits publication, close, and the entire HTTP %s body", async (status) => {
  const f = fixture();
  f.backend.publishGate = new Gate();
  f.backend.closeGate = new Gate();
  const bodyGate = new Gate();
  const reading = new Gate();
  let consumed = false;
  const body = `${secret.message}${"x".repeat(2000)}tail`;
  const response = new Response(
    new ReadableStream<Uint8Array>({
      async start(controller) {
        controller.enqueue(new TextEncoder().encode(body.slice(0, 1000)));
        await bodyGate.wait();
        controller.enqueue(new TextEncoder().encode(body.slice(1000)));
        controller.close();
      },
    }),
    { status },
  );
  const text = response.text.bind(response);
  const read = vi.spyOn(response, "text").mockImplementation(async () => {
    reading.release();
    const result = await text();
    expect(result).toBe(body);
    consumed = true;
    return result;
  });
  f.fetch.mockResolvedValue(response);
  const operation = f.publisher.publish(env, payload);
  const assertPending = pending(operation);
  try {
    await f.backend.publishing.wait();
    assertPending();
    expect(f.backend.close).not.toHaveBeenCalled();
    expect(f.fetch).not.toHaveBeenCalled();
    f.backend.publishGate.release();
    await f.backend.closing.wait();
    assertPending();
    expect(f.fetch).not.toHaveBeenCalled();
    f.backend.closeGate.release();
    await reading.wait();
    assertPending();
    expect(f.backend.close).toHaveBeenCalledTimes(1);
    expect(consumed).toBe(false);
    bodyGate.release();
    if (status === 200) {
      expect(await operation).toEqual({ taskId: "17" });
    } else {
      const error = await failure(operation, "dispatch", "17");
      expect((error.cause as Error).message).toBe(
        `task dispatcher returned HTTP 503: ${body.slice(0, 500)}`,
      );
      expect(error.backendCloseError).toBeUndefined();
    }
    expect(consumed).toBe(true);
    expect(read).toHaveBeenCalledTimes(1);
    expect(f.fetch).toHaveBeenCalledTimes(1);
    expect(f.backend.publish).toHaveBeenCalledTimes(1);
  } finally {
    f.backend.publishGate.release();
    f.backend.closeGate.release();
    bodyGate.release();
    await operation.catch(() => {});
  }
});

test.each([
  "configuration",
  "acquisition",
  "publication",
  "backend-close",
  "lookup",
  "fetch",
  "body",
] as const)("retains %s causes, including arbitrary falsy thrown values", async (point) => {
  for (const cause of [
    secret,
    undefined,
    null,
    false,
    0,
    "",
    Symbol("failure"),
  ]) {
    const f = fixture();
    if (point === "configuration")
      f.configure.mockImplementation(() => {
        throw cause;
      });
    if (point === "acquisition") f.connect.mockRejectedValue(cause);
    if (point === "publication")
      vi.mocked(f.backend.publish).mockRejectedValue(cause);
    if (point === "backend-close") f.backend.close.mockRejectedValue(cause);
    if (point === "lookup")
      f.dispatcher.getByName.mockImplementation(() => {
        throw cause;
      });
    if (point === "fetch") f.fetch.mockRejectedValue(cause);
    if (point === "body") {
      const response = new Response("ignored");
      vi.spyOn(response, "text").mockRejectedValue(cause);
      f.fetch.mockResolvedValue(response);
    }
    const dispatch = ["lookup", "fetch", "body"].includes(point);
    const known = dispatch || point === "backend-close";
    const acquired = !["configuration", "acquisition"].includes(point);
    const error = await failure(
      f.publisher.publish(env, payload),
      dispatch ? "dispatch" : (point as PostgresPublisherStage),
      known ? "17" : undefined,
    );
    expect(error.cause).toBe(cause);
    expect(error.backendCloseError).toBeUndefined();
    expect(f.backend.publish).toHaveBeenCalledTimes(acquired ? 1 : 0);
    expect(f.backend.close).toHaveBeenCalledTimes(acquired ? 1 : 0);
    expect(f.dispatcher.getByName).toHaveBeenCalledTimes(dispatch ? 1 : 0);
  }
});

test("encoding errors remain unknown publication outcomes and await shutdown", async () => {
  const f = fixture();
  vi.spyOn(task.codec, "encode").mockImplementation(() => {
    throw secret;
  });
  const error = await failure(f.publisher.publish(env, payload), "publication");
  expect(error.cause).toBe(secret);
  expect(f.backend.publish).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(f.fetch).not.toHaveBeenCalled();
});

test.each([
  "publication",
  "task-id",
  "numeric-task-id",
] as const)("keeps %s primary while awaiting a secondary close failure", async (point) => {
  for (const closeCause of [closeSecret, undefined]) {
    const f = fixture();
    const cause =
      point === "publication"
        ? secret
        : new PostgresPublishedTaskIdError("9007199254740993");
    if (point === "numeric-task-id") f.backend.taskId = 0;
    else vi.mocked(f.backend.publish).mockRejectedValue(cause);
    f.backend.closeGate = new Gate();
    f.backend.close.mockImplementation(async () => {
      f.backend.closing.release();
      await f.backend.closeGate?.wait();
      throw closeCause;
    });
    const operation = f.publisher.publish(env, payload);
    const assertPending = pending(operation);
    try {
      await f.backend.closing.wait();
      assertPending();
      f.backend.closeGate.release();
      const error = await failure(
        operation,
        point === "publication" ? point : "task-id",
        point === "publication"
          ? undefined
          : point === "task-id"
            ? "9007199254740993"
            : "0",
      );
      if (point !== "numeric-task-id") expect(error.cause).toBe(cause);
      expect(error.backendCloseError).toEqual({ cause: closeCause });
      expect(f.backend.publish).toHaveBeenCalledTimes(1);
      expect(f.backend.close).toHaveBeenCalledTimes(1);
      expect(f.fetch).not.toHaveBeenCalled();
    } finally {
      f.backend.closeGate.release();
      await operation.catch(() => {});
    }
  }
});

test("concurrent and sequential calls have independent configuration, backends, receipts and failures", async () => {
  const backends = [new Publishing(), new Publishing(), new Publishing()];
  const receivers = backends.map(() => receiver());
  const connect = vi.spyOn(PostgresPublishingBackend, "connect");
  for (const [i, backend] of backends.entries()) {
    backend.taskId = i + 1;
    backend.closeGate = new Gate();
    connect.mockResolvedValueOnce(
      backend as unknown as PostgresPublishingBackend,
    );
  }
  const configure = vi.fn((env: Env & { index: number }) => ({
    connectionString: env.url,
    schema: env.schema,
    task,
    dispatcher: receivers[env.index].dispatcher,
  }));
  const publisher = createPostgresPublisher(configure);
  backends[0].close.mockImplementation(async () => {
    backends[0].closing.release();
    await backends[0].closeGate?.wait();
    throw closeSecret;
  });
  const first = publisher.publish(
    { url: "first", schema: "a", index: 0 },
    { name: "first" },
  );
  const second = publisher.publish(
    { url: "second", schema: "b", index: 1 },
    { name: "second" },
  );
  const assertFirstPending = pending(first);
  try {
    await Promise.all(
      backends.slice(0, 2).map((backend) => backend.closing.wait()),
    );
    backends[1].closeGate?.release();
    expect(await second).toEqual({ taskId: "2" });
    assertFirstPending();
    backends[0].closeGate?.release();
    expect((await failure(first, "backend-close", "1")).cause).toBe(
      closeSecret,
    );
    backends[2].closeGate?.release();
    expect(
      await publisher.publish({ url: "third", index: 2 }, { name: "third" }),
    ).toEqual({ taskId: "3" });
    expect(configure).toHaveBeenCalledTimes(3);
    expect(connect.mock.calls).toEqual([
      ["first", { schema: "a" }],
      ["second", { schema: "b" }],
      ["third", { schema: undefined }],
    ]);
    for (const [i, name] of ["first", "second", "third"].entries()) {
      expect(backends[i].publish).toHaveBeenCalledExactlyOnceWith(task, {
        name,
      });
      expect(backends[i].close).toHaveBeenCalledTimes(1);
      expect(receivers[i].fetch).toHaveBeenCalledTimes(i === 0 ? 0 : 1);
    }
  } finally {
    for (const backend of backends) backend.closeGate?.release();
    await Promise.allSettled([first, second]);
  }
});
