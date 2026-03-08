<p align="center">
  <h1 align="center">bellows</h1>
</p>

**Durable task processing framework in Rust for applications of all sizes**

## Built-in backends

`bellows` currently ships with:

- an in-memory backend for lightweight testing;
- a SQLite backend for durable local development and single-process deployment scenarios; and
- a Postgres backend for durable multi-process and distributed deployment scenarios.

### SQLite signaling model

> [!IMPORTANT]
>
> The SQLite backend **ONLY** supports single-process deployment.

The SQLite backend persists tasks durably, but SQLite does not provide a native notification mechanism that can wake dispatchers in other processes.

Because of that, the built-in SQLite backend uses an in-process signal channel. Clones of the same `SqliteBackend` instance receive new-task notifications immediately, but separate processes sharing the same database file do not.

That means the SQLite backend is appropriate for local development, tests, and same-process worker setups, but it should not be treated as a distributed production backend.

A planned future extension will allow the in-memory signaling to be swapped out to something like Redis to support the multi-process deployment model.

### Postgres signaling model

The Postgres backend uses native `LISTEN`/`NOTIFY` signaling. Task inserts trigger `pg_notify`, and dispatchers subscribe through a dedicated listener connection.

Because the signaling is provided by Postgres itself, this backend works naturally across multiple worker processes and across multiple machines, as long as they can all reach the same Postgres database.

This makes the Postgres backend the built-in option intended for durable distributed deployments, while SQLite remains the lightweight single-process durable option.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
