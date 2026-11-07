import type { PublishedTask, PublishTaskDefinition } from "../types.js";
import {
  type PostgresBackendOptions,
  parseSchemaOption,
  postgresPublicationSql,
  qualifiedTasksTable,
  readPostgresPublishedTaskId,
} from "./postgres-operations.js";

/** A named relation (normally a preceding CTE), not a SQL expression or qualified path. */
export interface PostgresEncodedRowsSource {
  /** ASCII lowercase identifier: `[a-z_][a-z0-9_]*`, at most 63 bytes. */
  readonly relation: string;
  /** Each row must supply non-null PostgreSQL text already compatible with the task codec. */
  readonly payloadColumn: string;
}

export interface PostgresEncodedRowsPublicationOptions
  extends PostgresBackendOptions {
  readonly source: PostgresEncodedRowsSource;
  /** Number of preceding bind parameters; defaults to zero. Must be an integer in 0..65534. */
  readonly parameterOffset?: number;
}

/** One immediate task name, bound after the caller's existing parameters. */
export type PostgresEncodedRowsPublicationParameters = readonly [
  taskName: string,
];

/** Exact decimal text from RETURNING, never normalized through a JavaScript number first. */
export interface PostgresPublicationRow {
  readonly task_id: string;
}

export interface PostgresPublicationFragment {
  /** Single INSERT ... SELECT ... RETURNING body suitable for a data-modifying CTE. */
  readonly sql: string;
  readonly parameters: PostgresEncodedRowsPublicationParameters;
  /**
   * Validates every returned ID before returning any receipts. Empty rows yield no receipts.
   * IDs must be canonical positive decimal text and fit a safe JS integer. Unsafe exact text
   * is retained in PostgresPublishedTaskIdError. Malformed/nonpositive IDs throw TypeError.
   * Row order has no contract; there is no positional correspondence with source rows.
   *
   * Call inside the enclosing transaction and let failures abort it. Receipts remain
   * provisional until caller commit; decoding neither rolls back nor commits.
   */
  readonly readReceipts: (
    rows: readonly PostgresPublicationRow[],
  ) => readonly PublishedTask[];
}

/**
 * Pure SQL composition for one publishable definition and zero or more already-encoded rows.
 * This trusted encoding boundary never invokes the task codec: SQL must produce compatible
 * text (not necessarily JSON). Callback-bearing definitions register no callbacks; singleton
 * definitions are excluded. The fragment binds the task name and owns all insertion defaults.
 *
 * Source identifiers use `[a-z_][a-z0-9_]*`, limited to 63 ASCII bytes to prevent PostgreSQL
 * truncation, and are quoted. Schema options retain the existing backend semantics: explicit
 * qualification when supplied, the caller's search_path otherwise, with no public fallback.
 * The single bind must fit PostgreSQL's 65535-parameter limit, including the caller's offset.
 *
 * Preparation and decoding never execute SQL, acquire/close resources, initialize schemas,
 * change search_path, retry, dispatch, or commit. Execute once in the caller's transaction,
 * decode before commit, and dispatch only after confirmed commit. Never automatically retry
 * uncertain statement/commit outcomes. TS receipts stop at Number.MAX_SAFE_INTEGER; Rust's
 * integer representation can retain positive PostgreSQL IDs through i64::MAX.
 */
export function preparePostgresPublicationFromEncodedRows<TPayload, TCallback>(
  task: PublishTaskDefinition<TPayload, TCallback>,
  options: PostgresEncodedRowsPublicationOptions,
): PostgresPublicationFragment {
  const table = qualifiedTasksTable(parseSchemaOption(options));
  const relation = quoteSourceIdentifier(options.source.relation);
  const payloadColumn = quoteSourceIdentifier(options.source.payloadColumn);
  const offset =
    options.parameterOffset === undefined ? 0 : options.parameterOffset;
  if (!Number.isInteger(offset) || offset < 0 || offset > 65_534) {
    throw new RangeError(
      "PostgreSQL publication parameterOffset must be an integer in 0..65534.",
    );
  }
  const parameters: PostgresEncodedRowsPublicationParameters = Object.freeze([
    task.name,
  ]);
  return Object.freeze({
    sql: postgresPublicationSql(
      table,
      [`$${offset + 1}`, `${relation}.${payloadColumn}`, "NULL", "NULL"],
      relation,
    ),
    parameters,
    readReceipts(rows: readonly PostgresPublicationRow[]) {
      return rows.map(({ task_id }) => {
        if (
          typeof task_id !== "string" ||
          !/^[1-9]/.test(task_id) ||
          /[^0-9]/.test(task_id)
        ) {
          throw new TypeError(
            "PostgreSQL publication task_id must be canonical positive decimal text.",
          );
        }
        return readPostgresPublishedTaskId(task_id);
      });
    },
  });
}

function quoteSourceIdentifier(identifier: string): string {
  if (
    typeof identifier !== "string" ||
    identifier.length > 63 ||
    !/^[a-z_]/.test(identifier) ||
    /[^a-z0-9_]/.test(identifier)
  ) {
    throw new TypeError(
      "PostgreSQL publication source identifiers must match [a-z_][a-z0-9_]* and be at most 63 bytes.",
    );
  }
  return `"${identifier}"`;
}
