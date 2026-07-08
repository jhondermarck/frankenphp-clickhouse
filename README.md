# frankenphp-clickhouse

Native PHP extension for [FrankenPHP](https://frankenphp.dev/) that exposes ClickHouse directly to PHP via a Go implementation, bypassing JSON entirely.

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
                      addGenericRow() [1 CGo/row]
         ◄──────────────────────────  zend_array*
$rows = [['id'=>..., 'start'=>...], ...]
```

**Implementation highlights:**

| Layer | Technique |
|-------|-----------|
| Transport | Native ClickHouse binary protocol (port 9000) + LZ4 |
| Scan | `rows.Scan()` into typed destinations allocated once |
| Serialization | `packCol()` — cumulative byte buffer, zero allocation in the hot loop |
| PHP array | `ch_add_row()` — direct C construction, no intermediate serialization |
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
  SMI2 – HTTP + php-array        0.324s   0.285s   0.397s   100,000   ref
  Go TCP + query_array            0.038s   0.036s   0.040s   100,000  ×8.57
  Go TCP + cursor (10k chunks)    0.040s   0.039s   0.043s   100,000  ×8.08
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
clickhouse_query_array(string $query, ?array $params = null, ?array $options = null): array
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
clickhouse_ping(): string
clickhouse_server_version(): string
clickhouse_disconnect(): string
```

### Per-call options

The `$options` array accepts:

| Key | Value | Effect |
|-----|-------|--------|
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

All functions throw `RuntimeException` on error. The exception message contains the ClickHouse error detail.

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
| `Decimal(P,S)` | `string` | Preserves precision |
| `Enum8`, `Enum16` | `string` | Enum name (e.g. `"active"`) |
| `Nullable(T)` | `T` or `null` | Any supported type |
| `Array(T)` | `array` | Indexed PHP array, any inner type incl. `Array(Array(T))` and `Array(Map(K, V))` |
| `Map(K, V)` | `array` | Keyed PHP array — `int` keys for integer `K`, `string` otherwise; keys sorted |
| `LowCardinality(T)` | same as `T` | Transparent wrapper, incl. `LowCardinality(Nullable(T))` |

Types not listed above (Tuple, JSON, Int128/256…) are not yet supported and will throw a `RuntimeException`.

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

## Testing

```bash
make test      # 118 PHP integration tests
make test_go   # Go unit tests
```

The test suite covers:
- **SELECT**: query_array returns correct PHP arrays with all column types
- **INSERT**: batch insert with value verification
- **19 types**: String, DateTime, Int8-64, UInt8-64, Float32/64, Bool, UUID, IPv4/IPv6, Decimal, Enum
- **Nullable**: NULL values for all supported types
- **Exceptions**: RuntimeException on bad query, bad DSN, not connected, double disconnect
- **Memory leaks**: 200 iterations of query/insert/exec with 0 bytes growth

## Build

### Docker (recommended)

```bash
make up       # docker-compose up -d
make restart  # rebuild + restart
```

The Docker image builds the extension from the committed Go module in `clickhouse-ext/`.

### Local dev (macOS, xcaddy)

Requirements: [xcaddy](https://github.com/caddyserver/xcaddy), Go >= 1.21, PHP >= 8.2 with dev headers.

```bash
make ext       # Regenerate C bridge files (downloads frankenphp if needed)
make build     # Compile FrankenPHP binary with the extension
make bench     # INSERT + SELECT benchmark vs SMI2
```

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
  clickhousephp.go        # PHP-exported functions (//export) + FrankenPHP registration
  clickhousephp.c         # C bridge (PHP_FUNCTION → Go, throws RuntimeException on error)
  clickhousephp.h         # C header
  clickhousephp_arginfo.h # PHP argument info
  clickhousearray.go      # C PHP array construction + CGo helpers
  clickhousetypes.go      # ClickHouse type system (19 types) + DateTime formatting
  clickhousetypes_test.go # Go unit tests
  go.mod                  # Go module (imported by Docker)
web/
  test.php                # PHP integration tests (118 assertions)
  bench.php               # INSERT + SELECT benchmark vs SMI2
  bench_http.php          # HTTP worker mode benchmark
  worker.php              # FrankenPHP worker (persistent connection + retry)
docker/                   # Docker config (ClickHouse, FrankenPHP)
sample/                   # Standalone Dockerfile (imports from GitHub)
.github/workflows/        # CI: Go tests, PHP tests, build
Makefile
```
