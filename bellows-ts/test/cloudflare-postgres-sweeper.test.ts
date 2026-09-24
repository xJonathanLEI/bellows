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
import { definePublishTask, defineSingletonTask } from "../src/types.js";
import { Gate } from "./helpers.js";

const secret = new Error("postgres://user:secret@private/database");
const closeSecret = new Error("private shutdown failure");
const exactName = 'unregistered/"\\\n雪🦀';
const row = (
  taskId: string,
  taskName = exactName,
  isSingleton = false,
): PostgresDiscoveryCandidate => ({ taskId, taskName, isSingleton });
const context = (...rows: PostgresDiscoveryCandidate[]) =>
  rows.map((row) => ({ ...row, source: "discovery" }));
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

function fixture(singletons?: PostgresSweeperConfig["singletons"]) {
  const backend = new Discovery();
  const connect = vi
    .spyOn(PostgresDiscoveryBackend, "connect")
    .mockResolvedValue(backend as unknown as PostgresDiscoveryBackend);
  const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
    Response.json({ ok: true }),
  );
  const getByName = vi.fn((_name: string) => ({ fetch }));
  const configure = vi.fn(
    (env: Env): PostgresSweeperConfig => ({
      connectionString: env.url,
      schema: env.schema,
      dispatcher: { getByName },
      singletons,
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
  report: Omit<PostgresSweepReport, "bootstrapCandidates"> & {
    bootstrapCandidates?: number;
  },
) {
  const result: unknown = await promise.catch((error: unknown) => error);
  expect(result).toBeInstanceOf(PostgresSweeperError);
  const error = result as PostgresSweeperError;
  expect(error.stage).toBe(stage);
  expect(error.message).toBe(`PostgreSQL sweeper failed at ${stage}`);
  expect(error.report).toEqual({ bootstrapCandidates: 0, ...report });
  expect(error.report.discovered + error.report.bootstrapCandidates).toBe(
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
  expect(report).toEqual({
    discovered: 1,
    bootstrapCandidates: 0,
    accepted: 1,
    failed: 0,
  });
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
    batch(row("1")),
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

test("three page batches launch during gated discovery and settle during shutdown", async () => {
  const f = fixture([
    defineSingletonTask(`${exactName}2`),
    defineSingletonTask("bootstrap"),
  ]);
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
            i % 2
              ? `${exactName}${start + i + 1}`
              : "another unregistered name",
            i % 2 === 1,
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
      return JSON.stringify({ ok: true });
    });
    return response;
  });
  const call = f.sweeper.sweep(env);
  const assertPending = pending(call);
  await laterQuery.wait();
  await started.wait();
  expect(count).toBe(1);
  assertPending();
  releaseQuery.release();
  await f.backend.closing.wait();
  for (let i = 1; i < 4; i++) await started.wait();
  expect(count).toBe(4);
  expect(f.backend.readPage).toHaveBeenCalledTimes(4);
  expect(
    new Set(
      f.fetch.mock.calls.flatMap(([, init]) =>
        JSON.parse(String(init?.body)).tasks.map((entry: { task: unknown }) =>
          JSON.stringify(entry.task),
        ),
      ),
    ).size,
  ).toBe(301);
  expect(JSON.parse(String(f.fetch.mock.calls[3]?.[1]?.body))).toEqual({
    tasks: [
      { task: { kind: "singleton", taskName: "bootstrap" }, intent: "ensure" },
    ],
  });
  assertPending();
  for (let i = 0; i < 4; i++) bodies.release();
  for (let i = 0; i < 4; i++) await consumed.wait();
  assertPending();
  f.backend.closeGate.release();
  expect(await call).toEqual({
    discovered: 300,
    bootstrapCandidates: 1,
    accepted: 301,
    failed: 0,
  });
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
  expect(error.candidates).toEqual(context(row(id)));
  expect(Object.isFrozen(error.candidates)).toBe(true);
  expect(Object.isFrozen(error.candidates[0])).toBe(true);
  expect(f.backend.readPage.mock.calls.map(([, cursor]) => cursor)).toEqual([
    null,
    id,
    "9007199254740991",
  ]);
  expect(f.fetch).toHaveBeenCalledTimes(1);
  expect(JSON.parse(String(f.fetch.mock.calls[0]?.[1]?.body))).toEqual(
    batch(row("9007199254740991", "other")),
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
  expect(error.candidates).toEqual(context(row("1", "")));
  expect(
    f.fetch.mock.calls.map(([, init]) => JSON.parse(String(init?.body))),
  ).toEqual([batch(row("2", " "), row("3"))]);
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
      accepted: 1,
      failed: 2,
    });
    expect(error.candidates).toEqual(context(row("1"), row("2")));
    if (fault !== "status") expect(error.cause).toBe(secret);
    if (fault === "body" || fault === "status")
      expect(text).toHaveBeenCalledTimes(1);
    expect(f.backend.close).toHaveBeenCalledTimes(1);
    expect(f.backend.readPage).toHaveBeenCalledTimes(3);
    expect(f.getByName).toHaveBeenCalledTimes(2);
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
  assertPending();
  bodyGate.release();
  bodyGate.release();
  const error = await failure(call, "discovery", {
    discovered: 2,
    accepted: 0,
    failed: 2,
  });
  expect(error.cause).toBe(secret);
  expect(error.candidates).toEqual([]);
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
  f.configure.mockImplementation((env) => ({
    connectionString: env.url,
    schema: env.schema,
    dispatcher: { getByName: f.getByName },
    singletons: [defineSingletonTask(env.url)],
  }));
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
    bootstrapCandidates: 1,
    accepted: 2,
    failed: 0,
  });
  assertPending();
  expect(second.readPage.mock.calls.map(([, cursor]) => cursor)).toEqual([
    null,
    "3",
  ]);
  f.backend.closeGate.release();
  await failure(first, "candidate", {
    discovered: 1,
    bootstrapCandidates: 1,
    accepted: 1,
    failed: 1,
  });
  expect(
    f.fetch.mock.calls
      .map(([, init]) => JSON.parse(String(init?.body)))
      .filter((body) => body.tasks[0].intent === "ensure")
      .map((body) => body.tasks[0].task.taskName),
  ).toEqual([env.url, "second"]);
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
    f.fetch.mockRejectedValueOnce(undefined);
    const error = await failure(f.sweeper.sweep(env), "dispatch", {
      discovered: 3,
      accepted: 1,
      failed: 2,
    });
    expect(error.cause).toBeUndefined();
    expect(Object.hasOwn(error, "cause")).toBe(true);
    expect(error.candidates).toEqual(context(row("1"), row("2")));
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

test.each([
  { singletons: undefined },
  { singletons: [] },
])("absent or empty bootstrap configuration performs no dispatch for an empty window", async ({
  singletons,
}) => {
  const f = fixture(singletons);
  f.backend.beginSweep.mockResolvedValue({ ...window, upperId: null });
  expect(await f.sweeper.sweep(env)).toEqual({
    discovered: 0,
    bootstrapCandidates: 0,
    accepted: 0,
    failed: 0,
  });
  expect(f.getByName).not.toHaveBeenCalled();
  expect(f.backend.close).toHaveBeenCalledTimes(1);
});

test.each([
  null,
  "99",
])("empty discovery window %s bootstraps typed heterogeneous definitions", async (upperId) => {
  const f = fixture([
    defineSingletonTask(exactName),
    defineSingletonTask<number[]>(" count "),
  ]);
  f.backend.beginSweep.mockResolvedValue({ ...window, upperId });
  f.backend.pages = [];
  expect(await f.sweeper.sweep(env)).toEqual({
    discovered: 0,
    bootstrapCandidates: 2,
    accepted: 2,
    failed: 0,
  });
  expect(JSON.parse(String(f.fetch.mock.calls[0]?.[1]?.body))).toEqual({
    tasks: [exactName, " count "].map((taskName) => ({
      task: { kind: "singleton", taskName },
      intent: "ensure",
    })),
  });
  expect(f.backend.close).toHaveBeenCalledTimes(1);
  expect(f.backend.readPage).toHaveBeenCalledTimes(upperId === null ? 0 : 1);
});

test.each(
  [
    [defineSingletonTask("")],
    [defineSingletonTask("雪".repeat(679))],
    [defineSingletonTask("\ud800")],
    [
      defineSingletonTask("duplicate"),
      defineSingletonTask<number>("duplicate"),
    ],
    [definePublishTask("published")],
    [null],
  ].map((singletons) => ({ singletons })),
)("invalid bootstrap configuration rejects before acquisition", async ({
  singletons,
}) => {
  // Runtime validation also guards untyped application configuration.
  const f = fixture(singletons as PostgresSweeperConfig["singletons"]);
  await failure(f.sweeper.sweep(env), "configuration", {
    discovered: 0,
    accepted: 0,
    failed: 0,
  });
  expect(f.connect).not.toHaveBeenCalled();
  expect(f.fetch).not.toHaveBeenCalled();
});

test("definition names are snapshotted before acquisition and accept the exact byte boundary", async () => {
  const definition = { ...defineSingletonTask(`${"雪".repeat(677)}ab`) };
  const f = fixture([definition]);
  f.backend.pages = [];
  f.connect.mockImplementation(async () => {
    definition.name = "changed";
    return f.backend as unknown as PostgresDiscoveryBackend;
  });
  await f.sweeper.sweep(env);
  expect(
    JSON.parse(String(f.fetch.mock.calls[0]?.[1]?.body)).tasks[0].task.taskName,
  ).toBe(`${"雪".repeat(677)}ab`);
});

test.each([
  "window",
  "page",
])("failed %s discovery skips bootstrap and closes", async (stage) => {
  const f = fixture([defineSingletonTask("bootstrap")]);
  if (stage === "window") f.backend.beginSweep.mockRejectedValue(secret);
  else
    f.backend.readPage
      .mockResolvedValueOnce([row("1")])
      .mockRejectedValueOnce(secret);
  await failure(f.sweeper.sweep(env), "discovery", {
    discovered: stage === "page" ? 1 : 0,
    accepted: stage === "page" ? 1 : 0,
    failed: 0,
  });
  expect(f.fetch).toHaveBeenCalledTimes(stage === "page" ? 1 : 0);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
});

test("failed recovery batches do not bootstrap submitted singleton names again", async () => {
  const f = fixture([
    defineSingletonTask(exactName),
    defineSingletonTask("bootstrap"),
  ]);
  const singleton = row("9223372036854775807", exactName, true);
  const unconfigured = row("9007199254740993", "unconfigured", true);
  f.backend.pages = [[row("1"), singleton], [unconfigured], []];
  const body = new Gate();
  f.fetch.mockImplementationOnce(async () => {
    const response = new Response();
    vi.spyOn(response, "text").mockImplementation(async () => {
      await body.wait();
      throw secret;
    });
    return response;
  });
  const call = f.sweeper.sweep(env);
  const assertPending = pending(call);
  await f.backend.closing.wait();
  assertPending();
  expect(f.fetch).toHaveBeenCalledTimes(3);
  body.release();
  const error = await failure(call, "dispatch", {
    discovered: 3,
    bootstrapCandidates: 1,
    accepted: 2,
    failed: 2,
  });
  expect(error.candidates).toEqual(context(row("1"), singleton));
  expect(
    f.fetch.mock.calls.map(([, init]) => JSON.parse(String(init?.body))),
  ).toEqual([
    batch(row("1"), singleton),
    batch(unconfigured),
    {
      tasks: [
        {
          task: { kind: "singleton", taskName: "bootstrap" },
          intent: "ensure",
        },
      ],
    },
  ]);
  expect(f.backend.readPage.mock.calls.map(([, cursor]) => cursor)).toEqual([
    null,
    singleton.taskId,
    unconfigured.taskId,
  ]);
});

test("invalid singleton candidates do not poison valid page entries or bootstrap", async () => {
  const f = fixture([defineSingletonTask("bootstrap")]);
  const invalid = row("9223372036854775807", "雪".repeat(679), true);
  const valid = row("-9223372036854775808", exactName, true);
  f.backend.pages = [[invalid, valid, row("9007199254740992")], []];
  const error = await failure(f.sweeper.sweep(env), "candidate", {
    discovered: 3,
    bootstrapCandidates: 1,
    accepted: 2,
    failed: 2,
  });
  expect(error.candidates).toEqual(context(invalid));
  expect(JSON.parse(String(f.fetch.mock.calls[0]?.[1]?.body))).toEqual(
    batch(valid),
  );
});

test("bootstrap failure context is bounded and name-only, with full candidate accounting", async () => {
  const f = fixture(
    Array.from({ length: 150 }, (_, i) => defineSingletonTask(`task${i}`)),
  );
  f.backend.pages = [];
  f.fetch.mockRejectedValueOnce(undefined);
  const error = await failure(f.sweeper.sweep(env), "dispatch", {
    discovered: 0,
    bootstrapCandidates: 150,
    accepted: 0,
    failed: 150,
  });
  expect(error.cause).toBeUndefined();
  expect(error.candidates).toEqual(
    Array.from({ length: 100 }, (_, i) => ({
      source: "bootstrap",
      taskName: `task${i}`,
    })),
  );
  expect(f.fetch).toHaveBeenCalledTimes(1);
  expect(f.backend.close).toHaveBeenCalledTimes(1);
});

test("published definitions cannot be configured as typed singletons", () => {
  const config: PostgresSweeperConfig = {
    connectionString: env.url,
    dispatcher: { getByName: () => ({ fetch }) },
    // @ts-expect-error Bootstrap configuration accepts singleton definitions only.
    singletons: [definePublishTask("published")],
  };
  expect(config).toBeDefined();
});

function batch(...candidates: PostgresDiscoveryCandidate[]) {
  return {
    tasks: candidates.map((candidate) => ({
      task: candidate.isSingleton
        ? { kind: "singleton", taskName: candidate.taskName }
        : {
            kind: "published",
            taskId: candidate.taskId,
            taskName: candidate.taskName,
          },
      intent: "run",
    })),
  };
}
