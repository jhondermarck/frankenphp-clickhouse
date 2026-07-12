# Changelog

All notable changes to this project are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-07-12

### Added

- **`clickhouse_query_columns(): array`** — a columnar result mode returning
  one array per column (`['id' => […], 'price' => […]]`) instead of one per
  row. Same data as `query_array`, transposed. Allocating one PHP array per
  column rather than per row makes it faster and much lighter on wide/large
  results (a 500k×5 result held ~56 MB vs ~204 MB, ~3.6× less). Built via
  `ch_add_columns`, the transpose of the batched row builder.
- **`Variant(…)` and `Dynamic` types** (read): each row resolves to the
  concrete value of whichever type it holds — a PHP scalar, `null`, or a nested
  array for composite values. Also usable nested (`Array(Variant)`,
  `Map(_, Dynamic)`, Variant/Dynamic fields in a `Tuple`).

## [0.3.0] - 2026-07-12

### Added

- **Geo types** (read): `Point` → `[x, y]`, and `Ring`, `LineString`,
  `Polygon`, `MultiPolygon`, `MultiLineString` as the corresponding nested
  arrays of coordinate pairs. Also usable nested (`Array(Point)`, geo fields in
  a `Tuple`).
- **Optional OO wrapper** (`packages/oo`, `jhondermarck/frankenphp-clickhouse-oo`):
  a `ClickHouse` facade over the procedural API with `Cursor`/`Batch` handle
  objects, a lazy `Cursor::rows()` generator, and a Prometheus/OpenMetrics
  exporter (`ClickHouse::formatMetrics()`), demoed by
  `examples/metrics_endpoint.php`.

### Changed

- **Faster reads**: scalar-only result rows are now built in batches per CGo
  crossing (`ch_add_rows`) instead of one call per row, cutting Go↔C transition
  overhead. Measured ~13% faster `query_array` and ~21% faster cursor on a
  100k-row SELECT (×8.4→×9.4 and ×7.6→×9.5 vs smi2). Results with a composite
  column (Array/Map/Tuple/Geo/JSON) are unchanged (one row per call).
- **CI**: Go and PHP suites now run on both amd64 and arm64; the PHP suite is
  matrixed over ClickHouse versions (pinned 26.5 is required, `latest` is a
  non-blocking canary). `actions/checkout` bumped to v5.

### Fixed

- **Docker healthcheck**: the compose `frankenphp` service had no explicit
  healthcheck and inherited one probing the disabled Caddy admin API, so
  `docker compose --wait` never saw it healthy. It now probes an app `/healthz`
  route.

## [0.2.0] - 2026-07-11

### Added

- **`clickhouse_stats(): array`** — a process-wide observability snapshot for
  health checks and leak diagnosis in worker mode: `connected`, process
  `uptime_seconds`, cached `server_version`, open-handle gauges
  (`cursors_open` / `batches_open`) with reaper state, driver-pool gauges
  (`open` / `idle` / `max_*`), and lifetime counters (`queries`, `inserts`,
  `execs`, `async_inserts`, `cursors_opened`, `batches_opened`, `errors`).
  No server round-trip.
- **`Tuple(…)` type** (read and write): named tuples map to associative PHP
  arrays keyed by field name, unnamed tuples to indexed arrays in field order.
  Fields may be any supported type, including nested tuples, `Array(Tuple)` and
  `Map(K, Tuple)`. This also covers `Nested(…)` columns when `flatten_nested=0`
  (represented as `Array(Tuple(…))`).
- **Release automation** (`.github/workflows/release.yml`): pushing a `v*` tag
  publishes standalone FrankenPHP binaries (`linux/amd64` + `linux/arm64`) as
  release assets with SHA-256 sums, and a multi-arch Docker image to
  `ghcr.io/jhondermarck/frankenphp-clickhouse` (`docker/release/Dockerfile`).
- **IDE stubs package** (`stubs/`) — `jhondermarck/frankenphp-clickhouse-stubs`,
  fully documented signatures for editor autocompletion and static analysis.
- **ETL example** (`examples/etl_export.php`) — a bounded-memory streaming
  cursor → batch table copy.
- **Fuzz target** `FuzzParseColMeta` for the column-type parser, and
  `make test_resilience` verifying the connection pool redials transparently
  after a ClickHouse restart.

### Fixed

- **Type parser crash on malformed names** (found by fuzzing): `Array(` with no
  closing paren, and a backtick-quoted tuple field name like `` Tuple(``) ``,
  caused an out-of-bounds slice panic in `parseColMeta`. Both now return an
  error / parse cleanly; the crashers are kept as regression seeds.

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
