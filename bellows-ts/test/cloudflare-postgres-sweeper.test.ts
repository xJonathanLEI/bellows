import {
  afterEach,
  beforeEach,
  describe,
  expect,
  expectTypeOf,
  test,
  vi,
} from "vitest";
import {
  PostgresDiscoveryBackend,
  type PostgresDiscoveryCandidate,
  type PostgresSweepWindow,
} from "../src/backends/postgres-discovery.js";
import {
  createPostgresSweeper,
  type PostgresSweeperConfig,
  PostgresSweeperError,
  type PostgresSweeperStage,
  type PostgresSweepReport,
} from "../src/cloudflare/postgres.js";
import { Gate } from "./helpers.js";

const secret = new Error("postgres://user:secret@private/database");
const closeSecret = new Error("private shutdown failure");
const exactName = 'unregistered/"\\\n雪🦀';
const row = (
  taskId: string,
  taskName = exactName,
): PostgresDiscoveryCandidate => ({ taskId, taskName });
const window: PostgresSweepWindow = {
  cutoffUnixMs: "1234",
  upperId: "9223372036854775807",
};

class Discovery {
  pages: PostgresDiscoveryCandidate[][] = [[row("1")], []];
  beginSweep = vi.fn(async () => window);
  readPage = vi.fn(
    async (_window: PostgresSweepWindow, _cursor: string | null) =>
      this.pages.shift() ?? [],
  );
  closing = new Gate();
  closeGate: Gate | undefined;
  close = vi.fn(async () => {
    this.closing.release();
    await this.closeGate?.wait();
  });
}

interface Env {
  url: string;
  schema?: string;
}
const env: Env = { url: "hyperdrive-url", schema: "sweep_schema" };

function fixture() {
  const backend = new Discovery();
  const connect = vi
    .spyOn(PostgresDiscoveryBackend, "connect")
    .mockResolvedValue(backend as unknown as PostgresDiscoveryBackend);
  const fetch = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response("accepted"),
  );
  const getByName = vi.fn((_name: string) => ({ fetch }));
  const configure = vi.fn(
    (env: Env): PostgresSweeperConfig => ({
      connectionString: env.url,
      schema: env.schema,
      dispatcher: { getByName },
    }),
  );
  return {
    backend,
    connect,
    fetch,
    getByName,
    configure,
    sweeper: createPostgresSweeper(configure),
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
  promise: Promise<unknown>,
  stage: PostgresSweeperStage,
  report: PostgresSweepReport,
) {
  const result: unknown = await promise.catch((error: unknown) => error);
  expect(result).toBeInstanceOf(PostgresSweeperError);
  const error = result as PostgresSweeperError;
  expect(error.stage).toBe(stage);
  expect(error.message).toBe(`PostgreSQL sweeper failed at ${stage}`);
  expect(error.report).toEqual(report);
  expect(error.report.discovered).toBe(
    error.report.accepted + error.report.failed,
  );
  expect(Object.isFrozen(error.report)).toBe(true);
  expect(String(error)).not.toContain(secret.message);
  return error;
}

beforeEach(() => {
  for (const method of ["error", "warn", "log", "info", "debug"] as const)
    vi.spyOn(console, method).mockImplementation(() => {});
});
afterEach(() => {
  for (const method of ["error", "warn", "log", "info", "debug"] as const)
    expect(console[method]).not.toHaveBeenCalled();
  vi.restoreAllMocks();
});

test("inert construction, detached methods, precise environment and forwarding", async () => {
  const f = fixture();
  expect(f.configure).not.toHaveBeenCalled();
  expect(f.connect).not.toHaveBeenCalled();
  const { sweep, scheduled } = f.sweeper;
  expectTypeOf(sweep).parameter(0).toEqualTypeOf<Env>();
  expectTypeOf(scheduled).returns.toEqualTypeOf<Promise<void>>();
  expectTypeOf(sweep).returns.toEqualTypeOf<Promise<PostgresSweepReport>>();
  const report = await sweep(env);
  expect(report).toEqual({ discovered: 1, accepted: 1, failed: 0 });
  expect(Object.isFrozen(report)).toBe(true);
  expect(f.configure).toHaveBeenCalledExactlyOnceWith(env);
  expect(f.connect).toHaveBeenCalledExactlyOnceWith(env.url, {
    schema: env.schema,
  });
  expect(f.backend.beginSweep).toHaveBeenCalledTimes(1);
  expect(f.backend.readPage.mock.calls).toEqual([
    [window, null],
    [window, "1"],
  ]);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(f.getByName).toHaveBeenCalledExactlyOnceWith("global");
  expect(JSON.parse(String(f.fetch.mock.calls[0]?.[1]?.body))).toEqual(
    row("1"),
  );
  const empty = new Discovery();
  empty.beginSweep.mockResolvedValue({ ...window, upperId: null });
  f.connect.mockResolvedValueOnce(empty as unknown as PostgresDiscoveryBackend);
  expect(
    await scheduled({ scheduledTime: 0, cron: "* * * * *" }, env),
  ).toBeUndefined();
  expect(empty.readPage).not.toHaveBeenCalled();
  expect(empty.close).toHaveBeenCalledTimes(1);
  expect(f.configure).toHaveBeenCalledTimes(2);
});

test("300 responses launch across pages, during gated discovery, and settle during shutdown", async () => {
  const f = fixture();
  f.backend.closeGate = new Gate();
  const laterQuery = new Gate();
  const releaseQuery = new Gate();
  const bodies = new Gate();
  const started = new Gate();
  const consumed = new Gate();
  let count = 0;
  f.backend.readPage.mockImplementation(async (bound, cursor) => {
    expect(bound).toBe(window);
    const start = cursor === null ? 0 : Number(cursor);
    if (start === 100) {
      laterQuery.release();
      await releaseQuery.wait();
    }
    return start === 300
      ? []
      : Array.from({ length: 100 }, (_, i) =>
          row(
            String(start + i + 1),
            i % 2 ? exactName : "another unregistered name",
          ),
        );
  });
  f.fetch.mockImplementation(async () => {
    count++;
    started.release();
    const response = new Response();
    vi.spyOn(response, "text").mockImplementation(async () => {
      await bodies.wait();
      consumed.release();
      return "duplicate or accepted";
    });
    return response;
  });
  const call = f.sweeper.sweep(env);
  const assertPending = pending(call);
  await laterQuery.wait();
  for (let i = 0; i < 100; i++) await started.wait();
  expect(count).toBe(100);
  assertPending();
  releaseQuery.release();
  await f.backend.closing.wait();
  for (let i = 100; i < 300; i++) await started.wait();
  expect(count).toBe(300);
  expect(f.backend.readPage).toHaveBeenCalledTimes(4);
  expect(
    new Set(
      f.fetch.mock.calls.map(
        ([, init]) => JSON.parse(String(init?.body)).taskId,
      ),
    ).size,
  ).toBe(300);
  assertPending();
  for (let i = 0; i < 300; i++) bodies.release();
  for (let i = 0; i < 300; i++) await consumed.wait();
  assertPending();
  f.backend.closeGate.release();
  expect(await call).toEqual({ discovered: 300, accepted: 300, failed: 0 });
});

test.each([
  "-9223372036854775808",
  "-1",
  "0",
  "01",
  "+1",
  "1\n",
  "1.0",
  "9007199254740992",
  "9007199254740993",
  "9223372036854775807",
  "",
])("unsupported exact ID %j retains identity and advances the cursor", async (id) => {
  const f = fixture();
  f.backend.pages = [[row(id)], [row("9007199254740991", "other")], []];
  const error = await failure(f.sweeper.sweep(env), "candidate", {
    discovered: 2,
    accepted: 1,
    failed: 1,
  });
  expect(error.candidate).toEqual(row(id));
  expect(Object.isFrozen(error.candidate)).toBe(true);
  expect(f.backend.readPage.mock.calls.map(([, cursor]) => cursor)).toEqual([
    null,
    id,
    "9007199254740991",
  ]);
  expect(f.fetch).toHaveBeenCalledTimes(1);
  expect(JSON.parse(String(f.fetch.mock.calls[0]?.[1]?.body))).toEqual(
    row("9007199254740991", "other"),
  );
});

test("empty names fail, whitespace and exact Unicode names are never normalized", async () => {
  const f = fixture();
  f.backend.pages = [[row("1", ""), row("2", " "), row("3")], []];
  const error = await failure(f.sweeper.sweep(env), "candidate", {
    discovered: 3,
    accepted: 2,
    failed: 1,
  });
  expect(error.candidate).toEqual(row("1", ""));
  expect(
    f.fetch.mock.calls.map(([, init]) => JSON.parse(String(init?.body))),
  ).toEqual([row("2", " "), row("3")]);
});

describe.each([
  "lookup",
  "transport",
  "status",
  "body",
] as const)("%s failure", (fault) => {
  test("does not starve later candidates/pages and drains response bodies", async () => {
    const f = fixture();
    const text = vi.fn(async () => {
      if (fault === "body") throw secret;
      return `${secret.message}${"x".repeat(5000)}tail`;
    });
    if (fault === "lookup")
      f.getByName.mockImplementationOnce(() => {
        throw secret;
      });
    else if (fault === "transport") f.fetch.mockRejectedValueOnce(secret);
    else
      f.fetch.mockImplementationOnce(async () => {
        const response = new Response(null, {
          status: fault === "status" ? 503 : 200,
        });
        vi.spyOn(response, "text").mockImplementation(text);
        return response;
      });
    f.backend.pages = [[row("1"), row("2")], [row("3")], []];
    const error = await failure(f.sweeper.sweep(env), "dispatch", {
      discovered: 3,
      accepted: 2,
      failed: 1,
    });
    expect(error.candidate).toEqual(row("1"));
    if (fault !== "status") expect(error.cause).toBe(secret);
    if (fault === "body" || fault === "status")
      expect(text).toHaveBeenCalledTimes(1);
    expect(f.backend.close).toHaveBeenCalledTimes(1);
    expect(f.backend.readPage).toHaveBeenCalledTimes(3);
    expect(f.getByName).toHaveBeenCalledTimes(3);
  });
});

test.each([
  "configuration",
  "acquisition",
  "discovery",
] as const)("%s errors retain even undefined causes", async (stage) => {
  const f = fixture();
  if (stage === "configuration")
    f.configure.mockImplementation(() => {
      throw undefined;
    });
  if (stage === "acquisition") f.connect.mockRejectedValue(undefined);
  if (stage === "discovery") f.backend.beginSweep.mockRejectedValue(undefined);
  const error = await failure(f.sweeper.sweep(env), stage, {
    discovered: 0,
    accepted: 0,
    failed: 0,
  });
  expect(Object.hasOwn(error, "cause")).toBe(true);
  expect(error.cause).toBeUndefined();
  expect(f.backend.close).toHaveBeenCalledTimes(stage === "discovery" ? 1 : 0);
  expect(f.fetch).not.toHaveBeenCalled();
});

test("query failure settles prior work, closes before slow responses and retains later close failure", async () => {
  const f = fixture();
  const bodyGate = new Gate();
  const bodyStarted = new Gate();
  f.backend.readPage
    .mockResolvedValueOnce([row("1"), row("2")])
    .mockRejectedValueOnce(secret);
  f.backend.close.mockImplementation(async () => {
    f.backend.closing.release();
    throw undefined;
  });
  f.fetch.mockImplementation(async () => {
    const response = new Response();
    vi.spyOn(response, "text").mockImplementation(async () => {
      bodyStarted.release();
      await bodyGate.wait();
      throw closeSecret;
    });
    return response;
  });
  const call = f.sweeper.sweep(env);
  const assertPending = pending(call);
  await f.backend.closing.wait();
  await bodyStarted.wait();
  await bodyStarted.wait();
  assertPending();
  bodyGate.release();
  bodyGate.release();
  const error = await failure(call, "discovery", {
    discovered: 2,
    accepted: 0,
    failed: 2,
  });
  expect(error.cause).toBe(secret);
  expect(error.candidate).toBeUndefined();
  expect(error.backendCloseError).toEqual({ cause: undefined });
  expect(f.backend.readPage).toHaveBeenCalledTimes(2);
});

test.each([
  "candidate",
  "dispatch",
  "backend-close",
] as const)("close failure after %s preserves first observed stage", async (stage) => {
  const f = fixture();
  if (stage === "candidate") f.backend.pages = [[row("0")], []];
  if (stage === "dispatch") {
    f.fetch.mockRejectedValueOnce(secret);
    // Wait for dispatch rejection to be observed before the final query completes.
    f.backend.readPage
      .mockResolvedValueOnce([row("1")])
      .mockImplementationOnce(async () => {
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        return [];
      });
  }
  f.backend.close.mockRejectedValue(closeSecret);
  const error = await failure(f.sweeper.sweep(env), stage, {
    discovered: 1,
    accepted: stage === "backend-close" ? 1 : 0,
    failed: stage === "backend-close" ? 0 : 1,
  });
  expect(
    stage === "backend-close" ? error.cause : error.backendCloseError?.cause,
  ).toBe(closeSecret);
});

test("close failure observed before dispatch failure stays primary, but all bodies settle", async () => {
  const f = fixture();
  const body = new Gate();
  const closed = new Gate();
  f.fetch.mockImplementation(async () => {
    const response = new Response();
    vi.spyOn(response, "text").mockImplementation(async () => {
      await body.wait();
      throw secret;
    });
    return response;
  });
  f.backend.close.mockImplementation(async () => {
    closed.release();
    throw closeSecret;
  });
  const call = f.sweeper.sweep(env);
  const assertPending = pending(call);
  await closed.wait();
  await Promise.resolve();
  assertPending();
  body.release();
  const error = await failure(call, "backend-close", {
    discovered: 1,
    accepted: 0,
    failed: 1,
  });
  expect(error.cause).toBe(closeSecret);
  expect(error.backendCloseError).toBeUndefined();
});

test("overlapping calls have independent backend, cursor, report and failure state", async () => {
  const f = fixture();
  const second = new Discovery();
  second.pages = [[row("3")], []];
  f.backend.pages = [[row("0")], []];
  f.backend.closeGate = new Gate();
  f.connect
    .mockResolvedValueOnce(f.backend as unknown as PostgresDiscoveryBackend)
    .mockResolvedValueOnce(second as unknown as PostgresDiscoveryBackend);
  const first = f.sweeper.sweep(env);
  const assertPending = pending(first);
  await f.backend.closing.wait();
  expect(await f.sweeper.sweep({ url: "second" })).toEqual({
    discovered: 1,
    accepted: 1,
    failed: 0,
  });
  assertPending();
  expect(second.readPage.mock.calls.map(([, cursor]) => cursor)).toEqual([
    null,
    "3",
  ]);
  f.backend.closeGate.release();
  await failure(first, "candidate", { discovered: 1, accepted: 0, failed: 1 });
  expect(f.configure).toHaveBeenCalledTimes(2);
  expect(f.connect.mock.calls[1]).toEqual(["second", { schema: undefined }]);
});

test("schema validation uses the owned backend rules without querying or initializing", async () => {
  const f = fixture();
  f.connect.mockRestore();
  const error = await failure(
    f.sweeper.sweep({ url: "not-a-database-url", schema: "invalid-schema" }),
    "acquisition",
    { discovered: 0, accepted: 0, failed: 0 },
  );
  expect(error.cause).toBeInstanceOf(Error);
  expect(f.fetch).not.toHaveBeenCalled();
});

test("undefined dispatch failures are observed during discovery without unhandled rejections", async () => {
  const f = fixture();
  const unhandled: unknown[] = [];
  const onUnhandled = (cause: unknown) => unhandled.push(cause);
  process.on("unhandledRejection", onUnhandled);
  try {
    f.backend.readPage
      .mockResolvedValueOnce([row("1"), row("2")])
      .mockImplementationOnce(async () => {
        // Cross an event-loop turn while discovery is outstanding and dispatches reject.
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        return [row("3")];
      })
      .mockResolvedValueOnce([]);
    f.fetch.mockRejectedValueOnce(undefined).mockRejectedValueOnce(secret);
    const error = await failure(f.sweeper.sweep(env), "dispatch", {
      discovered: 3,
      accepted: 1,
      failed: 2,
    });
    expect(error.cause).toBeUndefined();
    expect(Object.hasOwn(error, "cause")).toBe(true);
    expect(error.candidate).toEqual(row("1"));
    expect(unhandled).toEqual([]);
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
});

test("detached scheduled awaits cleanup and rejects only sanitized stage information", async () => {
  const f = fixture();
  f.backend.beginSweep.mockRejectedValue(secret);
  f.backend.closeGate = new Gate();
  const { scheduled } = f.sweeper;
  const call = scheduled({ scheduledTime: 0 }, env);
  const assertPending = pending(call);
  await f.backend.closing.wait();
  assertPending();
  f.backend.closeGate.release();
  const error: unknown = await call.catch((cause: unknown) => cause);
  expect(error).toBeInstanceOf(Error);
  expect(error).not.toBeInstanceOf(PostgresSweeperError);
  expect(String(error)).toBe("Error: PostgreSQL sweeper failed at discovery");
  expect(Object.hasOwn(error as Error, "cause")).toBe(false);
});
