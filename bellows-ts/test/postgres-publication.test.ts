import { Client, Pool } from "pg";
import { expect, expectTypeOf, test, vi } from "vitest";
import {
  type PostgresPublicationFragment,
  PostgresPublishedTaskIdError,
  preparePostgresPublicationFromEncodedRows,
} from "../src/backends/postgres-publishing.js";
import {
  definePublishTask,
  defineSingletonTask,
  type PublishedTask,
} from "../src/index.js";

const task = definePublishTask<{ title: string }, string>("post_created");
const source = {
  relation: "delivery_inputs",
  payloadColumn: "encoded_payload",
};

test("encoded-row publication is pure, binds names, and preserves schema semantics", () => {
  const codec = {
    encode: vi.fn(() => {
      throw new Error("SQL rows must not be encoded in JavaScript");
    }),
    decode: vi.fn(() => {
      throw new Error("Preparation must not decode payloads");
    }),
  };
  const definition = definePublishTask("post'; SELECT '🦀", codec, codec);
  const spies = [
    vi.spyOn(Pool.prototype, "connect"),
    vi.spyOn(Pool.prototype, "query"),
    vi.spyOn(Pool.prototype, "end"),
    vi.spyOn(Client.prototype, "connect"),
    vi.spyOn(Client.prototype, "query"),
    vi.spyOn(Client.prototype, "end"),
  ];
  try {
    for (const schema of [undefined, "app_tasks"]) {
      const fragment = preparePostgresPublicationFromEncodedRows(definition, {
        schema,
        source,
        parameterOffset: 3,
      });
      expect(fragment.sql).toContain(
        `INSERT INTO ${schema ? '"app_tasks".' : ""}bellows_tasks`,
      );
      expect(fragment.sql).toContain(
        'SELECT $4, NULL, "delivery_inputs"."encoded_payload", NULL, NULL, NULL FROM "delivery_inputs"',
      );
      expect(fragment.sql.trim()).toMatch(
        /RETURNING task_id::text AS task_id$/,
      );
      expect(fragment.sql).not.toContain(definition.name);
      expect(fragment.sql).not.toContain(";");
      expect(fragment.parameters).toEqual([definition.name]);
      expect(Object.isFrozen(fragment)).toBe(true);
      expect(Object.isFrozen(fragment.parameters)).toBe(true);
      expect(fragment.readReceipts([])).toEqual([]);
      expect(
        fragment
          .readReceipts([{ task_id: "2" }, { task_id: "1" }])
          .map(({ taskId }) => taskId)
          .sort(),
      ).toEqual([1, 2]);
    }
    expect(codec.encode).not.toHaveBeenCalled();
    expect(codec.decode).not.toHaveBeenCalled();
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  } finally {
    for (const spy of spies) spy.mockRestore();
  }
});

test("encoded-row source identifiers and offsets are bounded before execution", () => {
  for (const identifier of [
    "",
    "Uppercase",
    "a.b",
    "a b",
    "a;b",
    'a"b',
    "1a",
    "é",
    "a\n",
    "a\r",
    "a\u2028",
    "a\u2029",
    "a\0",
    "a".repeat(64),
  ]) {
    for (const field of ["relation", "payloadColumn"]) {
      expect(() =>
        preparePostgresPublicationFromEncodedRows(task, {
          source: { ...source, [field]: identifier },
        }),
      ).toThrow(TypeError);
    }
  }
  for (const parameterOffset of [
    -1,
    0.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    65_535,
    Number.MAX_SAFE_INTEGER,
  ]) {
    expect(() =>
      preparePostgresPublicationFromEncodedRows(task, {
        source,
        parameterOffset,
      }),
    ).toThrow(RangeError);
  }
  expect(
    preparePostgresPublicationFromEncodedRows(task, { source }).sql,
  ).toContain("SELECT $1,");
  const boundary = preparePostgresPublicationFromEncodedRows(task, {
    source: { relation: "a".repeat(63), payloadColumn: "_payload" },
    parameterOffset: 65_534,
  });
  expect(boundary.sql).toContain("SELECT $65535,");
  expect(boundary.sql).toContain(`"${"a".repeat(63)}"."_payload"`);
  expect(() =>
    preparePostgresPublicationFromEncodedRows(task, { source, schema: "a.b" }),
  ).toThrow("Database schema names");
});

test("encoded-row receipts reject noncanonical IDs and preserve unsafe exact text", () => {
  const fragment = preparePostgresPublicationFromEncodedRows(task, { source });
  expect(
    fragment.readReceipts([{ task_id: "1" }, { task_id: "9007199254740991" }]),
  ).toEqual([{ taskId: 1 }, { taskId: Number.MAX_SAFE_INTEGER }]);
  for (const task_id of [
    "",
    "0",
    "-1",
    "-0",
    "+1",
    "01",
    "1.0",
    "1e0",
    " 1",
    "1 ",
    "1\n",
    "1\r",
    "1\u2028",
    "NaN",
    "Infinity",
  ]) {
    // A later invalid ID prevents returning even the preceding valid receipts.
    expect(() =>
      fragment.readReceipts([{ task_id: "1" }, { task_id }]),
    ).toThrow(TypeError);
  }
  for (const task_id of [
    "9007199254740992",
    "9007199254740993",
    "9223372036854775807",
    "9223372036854775808",
  ]) {
    expect(() => fragment.readReceipts([{ task_id }])).toThrow(
      PostgresPublishedTaskIdError,
    );
    expect(() => fragment.readReceipts([{ task_id }])).toThrow(
      expect.objectContaining({ taskId: task_id }),
    );
  }
  // Driver adapters must preserve text, even if an ID happens to fit a number.
  expect(() =>
    fragment.readReceipts([
      // @ts-expect-error A driver returning numeric IDs violates the boundary.
      { task_id: 1 },
    ]),
  ).toThrow(TypeError);
});

test("encoded-row fragment types are readonly and exclude singleton activation", () => {
  expectTypeOf(
    preparePostgresPublicationFromEncodedRows(task, { source }),
  ).toEqualTypeOf<PostgresPublicationFragment>();
  expectTypeOf((fragment: PostgresPublicationFragment) => {
    // Typechecked only: consumers cannot alter the description.
    // @ts-expect-error SQL is readonly.
    fragment.sql = "";
    // @ts-expect-error The bind tuple is readonly.
    fragment.parameters[0] = "";
    // @ts-expect-error The validation operation is readonly.
    fragment.readReceipts = () => [];
    preparePostgresPublicationFromEncodedRows(
      // @ts-expect-error Singleton activation is not publishable.
      defineSingletonTask("singleton"),
      { source },
    );
    const receipts = fragment.readReceipts([{ task_id: "1" }]);
    // @ts-expect-error Receipts are readonly.
    receipts.push({ taskId: 2 });
    return receipts;
  }).returns.toEqualTypeOf<readonly PublishedTask[]>();
  expectTypeOf(
    preparePostgresPublicationFromEncodedRows(
      definePublishTask<string>("without_callback"),
      { source },
    ),
  ).toEqualTypeOf<PostgresPublicationFragment>();
});
