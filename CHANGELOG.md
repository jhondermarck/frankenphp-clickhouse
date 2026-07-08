# Changelog

All notable changes to this project are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-08

First public release: a native PHP extension for FrankenPHP that talks to
ClickHouse over the native TCP protocol and builds PHP arrays directly in C,
bypassing HTTP and JSON.

### Added

- **Queries**: `clickhouse_query_array` (materialized) and
  `clickhouse_query_cursor` / `cursor_fetch` / `cursor_close` (bounded-memory
  streaming), with native `{name:Type}` parameter binding (named and
  positional, including `Array(T)` params for `IN`).
- **Writes**: `clickhouse_insert` (flat / nested / associative rows),
  incremental batches (`batch_begin` / `append` / `flush` / `send` / `abort`),
  and `clickhouse_async_insert`. Map/Array columns (including nullable
  elements) accept native PHP arrays.
- **Connections**: single driver-managed pool via `clickhouse_connect`, plus
  additional named pools (`clickhouse_open` / `clickhouse_close`),
  `clickhouse_ping`, `clickhouse_server_version`. DSN supports multi-host,
  pool sizing, compression, and TLS (incl. mutual TLS via `ca_cert` /
  `client_cert` / `client_key`).
- **Per-call options**: `settings`, `query_id`, `timeout`, `connection`.
- **Errors**: failures raise `RuntimeException`; ClickHouse server error codes
  are exposed via `getCode()`.
- **Types**: numeric, String/FixedString/Enum, Date*/DateTime*, UUID, IPv4/6,
  Decimal, Bool, Nullable, LowCardinality, Array (nested), Map, Int128/256
  (as strings), and JSON (as nested PHP arrays).
- **Reliability**: panic guards on every exported function, an idle-handle
  reaper, a per-thread last-error channel for worker-mode safety, and bounded
  JSON recursion.
- Migration guide from `smi2/phpclickhouse`, benchmarks vs SMI2, Docker and
  standalone build paths.

[Unreleased]: https://github.com/jhondermarck/frankenphp-clickhouse/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jhondermarck/frankenphp-clickhouse/releases/tag/v0.1.0
