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

Machine: Apple M-series, ClickHouse on localhost.
Baseline: smi2/phpclickhouse (HTTP + `json_decode`).
Parameters: 3 warmup + 20 iterations, 100k rows.

### SELECT (100k rows)

```
  Method                          avg      min      p95      rows    vs SMI2
  ──────────────────────────────────────────────────────────────────────────
  SMI2 – HTTP + php-array        0.394s   0.337s   0.530s   100,000   ref
  Go TCP + query_array            0.046s   0.043s   0.062s   100,000  ×8.51
```

### INSERT (100k rows batch)

```
  Method                          avg      min      p95      rows/s   vs SMI2
  ──────────────────────────────────────────────────────────────────────────
  SMI2 – HTTP insert             0.496s   0.475s   0.547s   201,560   ref
  Go TCP + clickhouse_insert     0.172s   0.149s   0.225s   581,786  ×2.89
```

> INSERT variance comes from MergeTree background merges on the ClickHouse side, not the code.
> SELECT gains grow with volume because the fixed HTTP/JSON overhead becomes proportionally larger.

## PHP API

```php
clickhouse_connect(string $dsn): string
clickhouse_query_array(string $query, ?array $params = null): array
clickhouse_exec(string $query, ?array $params = null): string
clickhouse_insert(string $table, array $values, ?array $columns = null): string
clickhouse_disconnect(): string
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
| `String`, `FixedString` | `string` | |
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
| `Array(T)` | `array` | Indexed PHP array, any inner type |
| `LowCardinality(T)` | same as `T` | Transparent wrapper |

Types not listed above (Map, Tuple) are not yet supported and will throw a `RuntimeException`.

## DSN Format

```
clickhouse://[user[:password]@]host:port/database[?param=value]
```

| Parameter | Value | Effect |
|-----------|-------|--------|
| `secure` | `true` | Enable TLS |
| `skip_verify` | `true` | Skip certificate verification |
| `compress` | `false` | Disable LZ4 (useful for localhost) |

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
