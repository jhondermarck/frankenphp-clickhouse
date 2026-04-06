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
| DateTime | `appendTimeRaw()` — RFC3339 formatting without `time.Format` or allocation |

## Benchmarks

Machine: Apple M-series, ClickHouse on localhost.
Baseline: smi2/phpclickhouse (HTTP + `json_decode`).
Parameters: 3 warmup + 20 iterations.

### SELECT

```
  Rows       SMI2 avg    Go avg     vs SMI2
  ─────────────────────────────────────────
    1 000     0.005s     0.002s      ×3.13
   10 000     0.035s     0.009s      ×3.91
  100 000     0.366s     0.043s      ×8.47
1 000 000     3.861s     0.389s      ×9.93
```

### INSERT (batch)

```
  Rows       SMI2 avg    Go avg     vs SMI2    Go rows/s
  ───────────────────────────────────────────────────────
    1 000     0.012s     0.006s      ×2.08      179 153
   10 000     0.052s     0.021s      ×2.48      473 727
  100 000     0.494s     0.157s      ×3.14      635 762
1 000 000     4.911s     1.668s      ×2.94      599 464
```

> INSERT variance comes from MergeTree background merges on the ClickHouse side, not the code.
> SELECT gains grow with volume because the fixed HTTP/JSON overhead becomes proportionally larger.

## PHP API

```php
clickhouse_connect(string $dsn): string         // "Ok" or throws RuntimeException
clickhouse_query_array(string $query): array    // [['col' => val, ...], ...] or throws
clickhouse_exec(string $query): string          // "Ok" or throws RuntimeException
clickhouse_insert(string $table, array $values, array $columns): string  // "Ok" or throws
clickhouse_disconnect(): string                 // "Ok" or throws RuntimeException
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
// [['id' => 'abc', 'start' => '2024-01-15T08:00:00Z', ...], ...]

// DDL
clickhouse_exec('TRUNCATE TABLE staging');

// Batch INSERT via flat array
$values = [];
foreach ($data as $row) {
    array_push($values, $row['id'], $row['start'], $row['end'], $row['machine_id'], $row['type']);
}
clickhouse_insert('staging', $values, ['id', 'start', 'end', 'machine_id', 'type']);

clickhouse_disconnect();
```

## Supported ClickHouse Types

| ClickHouse Type | PHP Type | Notes |
|----------------|----------|-------|
| `String`, `FixedString` | `string` | |
| `DateTime`, `DateTime64` | `string` | Formatted as RFC3339Nano |
| `Date`, `Date32` | `string` | Formatted as RFC3339 |
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
| `LowCardinality(T)` | same as `T` | Transparent wrapper |

Types not listed above (Array, Map, Tuple) are not yet supported and will throw a `RuntimeException`.

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
make test      # 89 PHP integration tests
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
  test.php                # PHP integration tests (89 assertions)
  bench.php               # INSERT + SELECT benchmark vs SMI2
  bench_http.php          # HTTP worker mode benchmark
  worker.php              # FrankenPHP worker (persistent connection + retry)
docker/                   # Docker config (ClickHouse, FrankenPHP)
sample/                   # Standalone Dockerfile (imports from GitHub)
.github/workflows/        # CI: Go tests, PHP tests, build
Makefile
```
