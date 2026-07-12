# frankenphp-clickhouse

[![PHP tests](https://github.com/jhondermarck/frankenphp-clickhouse/actions/workflows/test-php.yml/badge.svg)](https://github.com/jhondermarck/frankenphp-clickhouse/actions/workflows/test-php.yml)
[![Go tests](https://github.com/jhondermarck/frankenphp-clickhouse/actions/workflows/test-go.yml/badge.svg)](https://github.com/jhondermarck/frankenphp-clickhouse/actions/workflows/test-go.yml)
[![Build](https://github.com/jhondermarck/frankenphp-clickhouse/actions/workflows/build.yml/badge.svg)](https://github.com/jhondermarck/frankenphp-clickhouse/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Native PHP extension for [FrankenPHP](https://frankenphp.dev/) that exposes ClickHouse directly to PHP via a Go implementation, bypassing JSON entirely.

## Requirements

- **Runtime**: a FrankenPHP binary built with this extension (see [Build](#build)). PHP ≥ 8.2.
- **Build**: [xcaddy](https://github.com/caddyserver/xcaddy), Go ≥ 1.26, PHP ≥ 8.2 with dev headers, a C toolchain. The simplest path is the Docker build, which needs only Docker.
- **Server**: ClickHouse reachable over the native TCP protocol (port 9000 by default).

## Why

Existing PHP ClickHouse clients (e.g. [smi2/phpclickhouse](https://github.com/smi2/phpclickhouse)) use the HTTP protocol and return data as JSON — two major bottlenecks at scale:

1. **HTTP overhead**: connection setup, encoding, decompression
2. **JSON deserialization**: `json_decode()` on 200k objects costs ~150ms in PHP

This extension eliminates both:

- **Native TCP protocol** (binary, LZ4-compressed)
- **PHP arrays built directly in C** from Go — zero JSON involved

The result is a ready-to-use associative PHP array with no intermediate allocations on the PHP side.

## Architecture

```
PHP                   Go (FrankenPHP extension)        ClickHouse
─────                 ─────────────────────────        ──────────
clickhouse_query_array($q)
         ──────────────────────────►  rows.Query(ctx, q)
                                           ◄─────────── TCP blocks (LZ4)
                      rows.Scan() → packCol()
                      ch_add_rows() [1 CGo / N rows]
         ◄──────────────────────────  zend_array*
$rows = [['id'=>..., 'start'=>...], ...]
```

**Implementation highlights:**

| Layer | Technique |
|-------|-----------|
| Transport | Native ClickHouse binary protocol (port 9000) + LZ4 |
| Scan | `rows.Scan()` into typed destinations allocated once |
| Serialization | `packCol()` — cumulative byte buffer, zero allocation in the hot loop |
| PHP array | `ch_add_rows()` — direct C construction; scalar results build many rows per CGo crossing (composite results fall back to one row per call) |
| DateTime | `appendClickHouseDateTime()` / `appendClickHouseDateTime64()` — ClickHouse-native formatting without `time.Format` or allocation |

## Benchmarks

Machine: Apple M-series. ClickHouse 26.5 in Docker (native TCP port).
FrankenPHP 1.12.4 (PHP 8.5), clickhouse-go v2.47.0.
Baseline: smi2/phpclickhouse (HTTP + `json_decode`).
Parameters: 3 warmup + 20 iterations, 100k rows.

### SELECT (100k rows)

```
  Method                          avg      min      p95      rows    vs SMI2
  ──────────────────────────────────────────────────────────────────────────
  SMI2 – HTTP + php-array        0.324s   0.296s   0.364s   100,000   ref
  Go TCP + query_array            0.034s   0.034s   0.035s   100,000  ×9.42
  Go TCP + cursor (10k chunks)    0.034s   0.032s   0.038s   100,000  ×9.50
```

### INSERT (100k rows batch)

```
  Method                          avg      min      p95      rows/s   vs SMI2
  ──────────────────────────────────────────────────────────────────────────
  SMI2 – HTTP insert             0.453s   0.430s   0.537s   220,882   ref
  Go TCP + clickhouse_insert     0.144s   0.130s   0.201s   692,786  ×3.14
```

> INSERT variance comes from MergeTree background merges on the ClickHouse side, not the code.
> SELECT gains grow with volume because the fixed HTTP/JSON overhead becomes proportionally larger.

### Memory — streaming cursor (1M rows read, `make bench_memory`)

```
  Method                        peak RAM
  ───────────────────────────────────────
  query_array (full result)     +498 MB
  cursor (10k chunks)             +6 MB
```

The cursor keeps memory bounded by the chunk size regardless of result size —
use it for exports, ETL, or any result too large to materialize at once.

## PHP API

```php
clickhouse_connect(string $dsn): string
clickhouse_open(string $dsn): int                 // extra named connection
clickhouse_close(int $connection): string
clickhouse_query_array(string $query, ?array $params = null, ?array $options = null): array
clickhouse_query_columns(string $query, ?array $params = null, ?array $options = null): array
clickhouse_query_cursor(string $query, ?array $params = null, ?array $options = null): int
clickhouse_cursor_fetch(int $cursor, int $max_rows = 10000): array
clickhouse_cursor_close(int $cursor): string
clickhouse_exec(string $query, ?array $params = null, ?array $options = null): string
clickhouse_insert(string $table, array $values, ?array $columns = null, ?array $options = null): string
clickhouse_batch_begin(string $table, ?array $columns = null, ?array $options = null): int
clickhouse_batch_append(int $batch, array $values): string
clickhouse_batch_flush(int $batch): string
clickhouse_batch_send(int $batch): string
clickhouse_batch_abort(int $batch): string
clickhouse_async_insert(string $query, bool $wait = true, ?array $params = null, ?array $options = null): string
clickhouse_ping(?int $connection = null): string
clickhouse_server_version(?int $connection = null): string
clickhouse_stats(): array
clickhouse_disconnect(): string
```

### Per-call options

The `$options` array accepts:

| Key | Value | Effect |
|-----|-------|--------|
| `connection` | int | Route the call to a connection opened with `clickhouse_open()` (default: the global connection) |
| `settings` | assoc array | ClickHouse query settings (`max_execution_time`, `max_result_rows`, `max_threads`…) |
| `query_id` | string | Tags the query — visible in `system.query_log`, usable with `KILL QUERY WHERE query_id = '…'` |
| `timeout` | Go duration (`"5s"`) | Per-call timeout, overrides the DSN `timeout` |

```php
// Cap a heavy query, tag it for observability
$rows = clickhouse_query_array($sql, null, [
    'settings' => ['max_execution_time' => 30, 'max_result_rows' => 1_000_000],
    'query_id' => 'report-' . $jobId,
    'timeout'  => '35s',
]);

// Kill it from another connection if needed
clickhouse_exec("KILL QUERY WHERE query_id = {id:String}", ['id' => "report-$jobId"]);
```

### Multiple connections

`clickhouse_connect()` manages the default global connection. For a second
cluster or database, open independent handles and route any call to them
via the `connection` option:

```php
clickhouse_connect($dsnMain);                    // default connection
$analytics = clickhouse_open($dsnAnalytics);     // second cluster

$rows = clickhouse_query_array('SELECT …', null, ['connection' => $analytics]);
clickhouse_ping($analytics);
clickhouse_close($analytics);
```

### Incremental batches (unbounded-size writes)

`clickhouse_insert` needs the whole payload in one PHP array. Batch handles
stream instead: append chunks as you produce them, `flush` ships buffered
rows to the server (memory stays bounded on both sides), `send` finalizes.
The pooled connection is held from `begin` to `send`/`abort`.

```php
$batch = clickhouse_batch_begin('events', ['id', 'start', 'type']);
foreach ($bigSource as $i => $row) {
    clickhouse_batch_append($batch, [$row]);        // nested/assoc/flat, same formats as insert
    if ($i % 10_000 === 0) {
        clickhouse_batch_flush($batch);             // ship buffered rows, keep going
    }
}
clickhouse_batch_send($batch);                      // or clickhouse_batch_abort($batch)
```

Notes: rows appended but never flushed are discarded by `abort`; flushed
rows are already on the server. Flat values and associative rows require
`$columns` at `begin` (the INSERT statement is fixed there); nested rows
without columns follow the table's DDL order.

> **Always close handles.** Cursors and batches hold a pooled connection from
> `open`/`begin` until `close`/`send`/`abort` — wrap them in `try`/`finally`.
> In worker mode the process outlives requests, so a leaked handle pins a socket
> until the pool is exhausted. A background reaper releases handles left idle for
> more than 10 minutes as a safety net, but it is not a substitute for closing.

> **Map / Array columns** accept native PHP arrays on write: an associative
> array for `Map(K, V)`, a list for `Array(T)` (nested arrays too). The value is
> coerced to the column's concrete type, so `['hits' => 42]` inserts cleanly into
> `Map(String, UInt64)`.

### Async inserts (high-frequency small writes)

For many small inserts where client-side batching isn't practical, let the
server buffer them ([async inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts)):

```php
// wait=true (default): returns once the server flushed the buffer — durable
clickhouse_async_insert("INSERT INTO events VALUES ({id:UUID}, {t:DateTime}, {ty:UInt8})",
    true, ['id' => $id, 't' => $t, 'ty' => $type]);

// wait=false: fire-and-forget (accept possible loss on server crash)
clickhouse_async_insert("INSERT INTO metrics VALUES (now(), 1)", false);
```

All functions throw `RuntimeException` on error. The exception message
contains the ClickHouse error detail, and for server-side errors
`$e->getCode()` carries the [ClickHouse error code](https://clickhouse.com/docs/en/native-protocol/server#exception)
(e.g. `60` = UNKNOWN_TABLE, `62` = SYNTAX_ERROR) — client-side errors keep code `0`.

### Error handling

```php
try {
    clickhouse_connect('clickhouse://default@localhost:9000/mydb');
    $rows = clickhouse_query_array('SELECT * FROM events LIMIT 50000');
} catch (RuntimeException $e) {
    echo "ClickHouse error: " . $e->getMessage();
    // e.g. "ping failed: dial tcp localhost:9000: connection refused"
    // e.g. "code: 60, message: Table default.events doesn't exist"
}
```

Errors that throw:
- Connection failure (bad DSN, unreachable host, authentication)
- SQL syntax error or unknown table
- Insert with mismatched values/columns count
- Any operation when not connected (`clickhouse_disconnect()` called twice, query before connect)

### Example

```php
clickhouse_connect('clickhouse://default@localhost:9000/mydb');

// SELECT → native PHP array, no json_decode
$events = clickhouse_query_array('SELECT * FROM events LIMIT 50000');
// [['id' => 'abc', 'start' => '2024-01-15 08:00:00', ...], ...]

// SELECT with named parameters (prevents SQL injection)
$rows = clickhouse_query_array(
    'SELECT * FROM events WHERE machine_id = {machine:String} AND start > {after:DateTime}',
    ['machine' => 'M-001', 'after' => '2024-01-01 00:00:00']
);

// Streaming read — bounded memory, chunk by chunk (always close the cursor)
$cursor = clickhouse_query_cursor('SELECT * FROM events');
while (count($rows = clickhouse_cursor_fetch($cursor, 10000)) > 0) {
    foreach ($rows as $row) {
        // process / write to file / forward…
    }
}
clickhouse_cursor_close($cursor);

// DDL
clickhouse_exec('TRUNCATE TABLE staging');

// Batch INSERT — flat array (original format)
$values = [];
foreach ($data as $row) {
    array_push($values, $row['id'], $row['start'], $row['end'], $row['machine_id'], $row['type']);
}
clickhouse_insert('staging', $values, ['id', 'start', 'end', 'machine_id', 'type']);

// Batch INSERT — nested rows
clickhouse_insert('staging', [
    ['evt-1', '2024-01-01 08:00:00', '2024-01-01 09:00:00', 'M-001', 'run'],
    ['evt-2', '2024-01-01 10:00:00', '2024-01-01 11:00:00', 'M-002', 'idle'],
], ['id', 'start', 'end', 'machine_id', 'type']);

// Batch INSERT — associative arrays (columns inferred from keys)
clickhouse_insert('staging', [
    ['id' => 'evt-3', 'machine_id' => 'M-001', 'type' => 'run'],
    ['id' => 'evt-4', 'machine_id' => 'M-002', 'type' => 'idle'],
]);

clickhouse_disconnect();
```

### Columnar results

`clickhouse_query_columns()` returns the same data as `clickhouse_query_array()`
but **transposed** — one array per column instead of one per row:

```php
$cols = clickhouse_query_columns('SELECT id, price FROM events');
// ['id' => [1, 2, 3, …], 'price' => [1.5, 2.5, 3.0, …]]
$cols['price'][2];   // == query_array()[2]['price']
```

Because it allocates one PHP array per column instead of one per row, it is
faster and **much lighter** on wide/large results — a 500k-row × 5-column
result held **~56 MB vs ~204 MB** for `query_array` (~3.6× less). Use it for
analytics, column-wise processing, or exporting to a dataframe; use
`query_array` when you iterate row by row.

## Supported ClickHouse Types

| ClickHouse Type | PHP Type | Notes |
|----------------|----------|-------|
| `String`, `FixedString(N)` | `string` | |
| `DateTime` | `string` | `Y-m-d H:i:s` (e.g. `"2024-01-15 08:00:00"`) |
| `DateTime64` | `string` | `Y-m-d H:i:s.u` (e.g. `"2024-01-15 08:00:00.123456"`) |
| `Date`, `Date32` | `string` | `Y-m-d H:i:s` (time part is `00:00:00`) |
| `Int8`, `Int16`, `Int32`, `Int64` | `int` | |
| `UInt8`, `UInt16`, `UInt32` | `int` | |
| `UInt64` | `int` or `float` | `float` if > PHP_INT_MAX |
| `Float32`, `Float64` | `float` | |
| `Bool` | `int` | `1` = true, `0` = false |
| `UUID` | `string` | `"550e8400-e29b-..."` |
| `IPv4`, `IPv6` | `string` | `"192.168.1.1"`, `"::1"` |
| `Decimal(P,S)` | `string` | Preserves precision (incl. Decimal128/256) |
| `Int128`, `UInt128`, `Int256`, `UInt256` | `string` | Exact value, beyond PHP int range |
| `JSON` | `array` | Nested PHP array; dynamic leaf types mapped like their column equivalents |
| `Enum8`, `Enum16` | `string` | Enum name (e.g. `"active"`) |
| `Nullable(T)` | `T` or `null` | Any supported type |
| `Array(T)` | `array` | Indexed PHP array, any inner type incl. `Array(Array(T))`, `Array(Map(K, V))` and `Array(Tuple(…))` |
| `Map(K, V)` | `array` | Keyed PHP array — `int` keys for integer `K`, `string` otherwise; keys sorted |
| `Tuple(…)` | `array` | Named tuple → assoc array keyed by field name; unnamed tuple → indexed array in field order. Any field type, incl. nested tuples/arrays/maps. Reads and writes. |
| `Nested(…)` | `array` | Via its `Array(T)` sub-columns (default `flatten_nested=1`), or as `Array(Tuple(…))` when `flatten_nested=0` |
| `LowCardinality(T)` | same as `T` | Transparent wrapper, incl. `LowCardinality(Nullable(T))` |
| `Point` | `array` | `[x, y]` (two floats) |
| `Ring`, `LineString` | `array` | List of points: `[[x, y], …]` |
| `Polygon`, `MultiLineString` | `array` | List of rings/lines: `[[[x, y], …], …]` |
| `MultiPolygon` | `array` | List of polygons: `[[[[x, y], …], …], …]` |
| `Variant(…)` | mixed | The concrete value of whichever branch a row holds (or `null`) |
| `Dynamic` | mixed | The concrete value, whatever its runtime type — scalar or nested array |

Types not listed above (`AggregateFunction`/`SimpleAggregateFunction` states, `Interval*`, `Nothing`) are not yet supported and will throw a `RuntimeException`.

Tuple values also round-trip on write: pass a nested PHP list for an unnamed
`Tuple(…)`, or an associative array (keyed by field name) for a named one.

## DSN Format

```
clickhouse://[user[:password]@]host1:9000[,host2:9000…]/database[?param=value]
```

The connection is a single driver-managed pool shared by all PHP worker
threads. Multiple hosts give failover / load-balancing across a cluster.

| Parameter | Value | Effect |
|-----------|-------|--------|
| `secure` | `true` | Enable TLS |
| `skip_verify` | `true` | Skip certificate verification |
| `ca_cert` | path | PEM CA bundle for TLS (implies TLS) |
| `client_cert` / `client_key` | paths | Client certificate for mutual TLS |
| `compress` | `lz4` (default), `zstd`, `gzip`, `false`/`none` | Transport compression |
| `timeout` | Go duration (`30s`, `2m`) | Per-call timeout — without it a hung query blocks a PHP worker forever |
| `connection_open_strategy` | `in_order`, `round_robin`, `random` | Host selection across the address list |
| `max_open_conns` | int (default 10) | Upper bound on sockets in the pool |
| `max_idle_conns` | int (default 5) | Idle sockets kept warm |
| `conn_max_lifetime` | Go duration (default `1h`) | Recycle sockets after this age |
| `dial_timeout` / `read_timeout` | Go duration | Driver-level network timeouts |

Any other parameter is passed through as a ClickHouse **query setting**
(driver behavior) — e.g. `?max_execution_time=60` applies to every query
on the connection.

## Security Notes

- **The DSN is trusted configuration.** `ca_cert`, `client_cert`, and
  `client_key` are read from the host filesystem, so a DSN assembled from
  untrusted input would be an arbitrary-file-read probe. Treat the DSN like a
  connection string in a config file — never build it from user data.
- **SQL parameters are safe; identifiers are not parameterized.** Values go
  through native `{name:Type}` bindings. Table and column names are validated
  (`validIdent`) but not bound — pass them as code constants, not user input.
- **The `docker-compose.yml` is a dev/bench stack**, not a production
  template: it ships a default password (override `CLICKHOUSE_PASSWORD` or use
  secrets) and runs containers as root. `sample/Dockerfile` shows a non-root
  production build; the main build verifies Go module checksums (no
  `GONOSUMDB`/`GOPROXY=direct`).

## Worker Mode (FrankenPHP)

In worker mode, the ClickHouse connection is established **once** at process boot and reused for all HTTP requests — eliminating TCP + handshake cost per call. Connection is retried up to 5 times on startup.

```php
// web/worker.php
try {
    clickhouse_connect($dsn);
} catch (RuntimeException $e) {
    fwrite(STDERR, "ClickHouse: " . $e->getMessage() . "\n");
}

while (frankenphp_handle_request(function () use ($query): void {
    $rows = clickhouse_query_array($query);
    echo json_encode(['count' => count($rows)]);
})) {
    gc_collect_cycles();
}

clickhouse_disconnect();
```

## Observability

`clickhouse_stats()` returns a process-wide snapshot — cheap (no server
round-trip) and safe to expose on a health-check endpoint. In worker mode the
process is long-lived, so this is the fastest way to spot a leaked cursor/batch
(the [leak warning](#incremental-batches-unbounded-size-writes) above) or a
saturated driver pool.

```php
$s = clickhouse_stats();
// [
//   'connected'         => 1,
//   'uptime_seconds'    => 3600,
//   'server_version'    => '26.5.5',   // cached from the handshake
//   'named_connections' => 0,          // extra clickhouse_open() handles
//   'handles' => [
//     'cursors_open'     => 0,         // ← climbing = leaked cursors
//     'batches_open'     => 0,         // ← climbing = leaked batches
//     'idle_ttl_seconds' => 600,       // reaper threshold
//     'last_reap_unix'   => 1720000000,
//     'last_reap_count'  => 0,
//   ],
//   'pool' => [                        // driver pool gauges (empty if not connected)
//     'open' => 1, 'idle' => 1, 'max_open_conns' => 10, 'max_idle_conns' => 5,
//   ],
//   'counters' => [                    // lifetime totals since process boot
//     'queries' => 42, 'inserts' => 7, 'execs' => 3, 'async_inserts' => 0,
//     'cursors_opened' => 5, 'batches_opened' => 2, 'errors' => 1,
//   ],
// ]

if ($s['handles']['cursors_open'] > 100) {
    error_log("clickhouse: {$s['handles']['cursors_open']} cursors open — leak?");
}
```

To expose this on a Prometheus `/metrics` endpoint, the optional OO package
formats the snapshot as OpenMetrics text (gauges for state/pool/handles,
counters for the lifetime totals, a version-labelled `build_info`):

```php
header('Content-Type: text/plain; version=0.0.4');
echo \Jhondermarck\ClickHouse\ClickHouse::formatMetrics(clickhouse_stats());
```

See [`examples/metrics_endpoint.php`](examples/metrics_endpoint.php).

## Testing

```bash
make test             # PHP integration tests (382 assertions)
make test_go          # Go unit tests (incl. a race-tested concurrency stress test)
make test_resilience  # Restart ClickHouse, verify the pool transparently redials
```

The type parser is also fuzzed (crashers are kept as regression seeds):

```bash
go test -C clickhouse-ext -run=xxx -fuzz=FuzzParseColMeta -fuzztime=30s .
```

The test suite covers:
- **SELECT / cursor**: query_array, columnar query_columns, and streaming cursors return correct PHP arrays for every supported type
- **INSERT / batch / async**: all write paths with value verification, including Map/Array/Tuple (and nullable) columns
- **Types**: numeric, String/FixedString/Enum, Date*/DateTime*, UUID, IPv4/6, Decimal, Bool, Nullable, LowCardinality, Array, Map, Tuple (named/unnamed/nested, incl. `Array(Tuple)` and `Map(_, Tuple)`), Geo (Point/Ring/LineString/Polygon/Multi*), Variant/Dynamic, Int128/256, JSON
- **Options**: per-call settings / query_id / timeout, multiple connections, ClickHouse error codes via `getCode()`
- **Exceptions**: RuntimeException on bad query, bad DSN, not connected, closed handles
- **Observability**: `clickhouse_stats` shape, counter deltas, open-handle gauge
- **OO wrapper**: facade query/cursor/batch, `rows()` generator, Prometheus `formatMetrics`
- **Memory leaks**: repeated query/insert/exec with no growth

## Install

Each tagged release ships pre-built artifacts, so most users don't need the
build toolchain:

- **Docker image** (`linux/amd64` + `linux/arm64`) — mount your app into `/app`:
  ```bash
  docker run -v "$PWD/app:/app" ghcr.io/jhondermarck/frankenphp-clickhouse
  ```
- **Standalone binaries** — download `frankenphp-clickhouse-linux-<arch>` (and
  its `.sha256`) from the [Releases](https://github.com/jhondermarck/frankenphp-clickhouse/releases) page.

Editor autocompletion for the `clickhouse_*` functions comes from a stubs
package (IDE-only, not loaded at runtime):

```bash
composer require --dev jhondermarck/frankenphp-clickhouse-stubs
```

Prefer objects to global functions? An optional OO facade wraps the procedural
API (with cursor/batch handle objects, a lazy `rows()` generator, and a
Prometheus exporter):

```bash
composer require jhondermarck/frankenphp-clickhouse-oo
```

```php
use Jhondermarck\ClickHouse\ClickHouse;

$ch = new ClickHouse($dsn);
foreach ($ch->cursor('SELECT * FROM events')->rows() as $row) { /* … */ }
```

## Build

### Docker (recommended)

```bash
make up       # docker-compose up -d
make restart  # rebuild + restart
```

The Docker image builds the extension from the committed Go module in `clickhouse-ext/`.

### Local dev (macOS, xcaddy)

```bash
make build     # Compile FrankenPHP binary with the extension
make bench     # INSERT + SELECT benchmark vs SMI2
```

> ⚠️ Do **not** run `make ext`. The C bridge (`clickhousephp.c`,
> `clickhousephp_arginfo.h`, `clickhousephp.stub.php`) is hand-maintained —
> the extension generator would overwrite the custom exception/error-code
> logic. See [CONTRIBUTING.md](CONTRIBUTING.md).

### Configuration (.env)

```ini
CH_DSN=clickhouse://default@localhost:9000/default
CH_HOST=localhost
CH_PORT=8123
CH_USER=default
CH_PASS=
CH_DB=default
CH_QUERY=SELECT ...
CH_INSERT_ROWS=1000000
CH_SELECT_LIMIT=200000
```

## Project Structure

```
clickhouse-ext/
  clickhousephp.go        # Module registration, pool globals, connect/disconnect
  conn.go                 # Named connections, ping/version/open/close, DSN → pool
  errors.go               # CH error codes, per-thread last-error, callSetup, panic guards
  query.go                # insert/exec/query_array/async, row normalization, type coercion
  handles.go              # rowPacker, idle-handle reaper, batch + cursor state and exports
  clickhousearray.go      # C PHP-array construction + CGo helpers
  clickhousetypes.go      # ClickHouse type system + DateTime formatting
  clickhousephp.c         # C bridge (PHP_FUNCTION → Go; throws RuntimeException) — hand-maintained
  clickhousephp.h         # C header — hand-maintained
  clickhousephp_arginfo.h # PHP argument info — hand-maintained
  clickhousephp.stub.php  # PHP function signatures — hand-maintained
  stats.go                # Runtime counters + clickhouse_stats state
  clickhousetypes_test.go # Go unit tests + FuzzParseColMeta (types, reaper, concurrency)
  go.mod                  # Go module (imported by Docker / xcaddy)
web/
  test.php                # PHP integration tests
  resilience.php          # Reconnection test (make test_resilience)
  bench.php               # INSERT + SELECT benchmark vs SMI2
  worker.php              # FrankenPHP worker (persistent connection + retry)
examples/
  etl_export.php          # Streaming cursor → batch table copy (bounded memory)
  metrics_endpoint.php    # clickhouse_stats() → Prometheus /metrics exposition
stubs/                    # Composer IDE stubs package (clickhouse_* signatures)
packages/oo/              # Optional OO wrapper package (facade + Prometheus exporter)
docker/                   # Docker config: dev stack + docker/release/ (distributable image)
sample/                   # Standalone production Dockerfile
docs/                     # Migration guide (smi2 → native)
.github/workflows/        # CI: Go tests, PHP tests, build; release (binaries + GHCR image)
Makefile
```

## License

[MIT](LICENSE) © Jérôme Hondermarck
