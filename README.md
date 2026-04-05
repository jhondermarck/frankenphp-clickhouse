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
// Connect once at startup (in worker mode: at process boot)
clickhouse_connect(string $dsn): string         // "Ok" | "ERROR: ..."

// Query → associative PHP array, any schema
clickhouse_query_array(string $query): array    // [['col' => val, ...], ...]

// DDL / DML with no result set
clickhouse_exec(string $query): string          // "Ok" | "ERROR: ..."

// Batch INSERT — flat array [$v1, $v2, ..., $vN] (all columns concatenated)
clickhouse_insert(string $table, array $values, array $columns): string

// Disconnect
clickhouse_disconnect(): string
```

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

In worker mode, the ClickHouse connection is established **once** at process boot and reused for all HTTP requests — eliminating TCP + handshake cost per call.

```php
// web/worker.php
clickhouse_connect($dsn);  // once at boot

while (frankenphp_handle_request(function () use ($query): void {
    $rows = clickhouse_query_array($query);
    echo json_encode(['count' => count($rows)]);
})) {
    gc_collect_cycles();
}

clickhouse_disconnect();
```

## Build

### Docker (recommended)

```bash
make up     # docker-compose up -d
make bench  # run benchmark inside the container
```

The Docker image imports the extension directly from GitHub — no local compilation needed.

### Local dev (macOS, xcaddy)

Requirements: [xcaddy](https://github.com/caddyserver/xcaddy), Go ≥ 1.21, PHP ≥ 8.2 with dev headers.

```bash
make ext     # Regenerate extension build/ (after modifying Go sources)
make build   # Compile FrankenPHP binary with the extension

make test    # PHP integration tests (30 assertions)
make test_go # Go unit tests

make bench   # INSERT + SELECT benchmark vs SMI2

make serve        # Start HTTP server in worker mode
make bench_worker # HTTP worker benchmark (separate terminal)
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
  clickhousephp.go        # PHP-exported functions (export_php:function ...)
  clickhousearray.go      # C PHP array construction + CGo helpers
  clickhousetypes.go      # ClickHouse type system + DateTime formatting
  clickhousetypes_test.go # Go unit tests
  build/                  # Generated module (committed — imported by Docker)
web/
  bench.php               # INSERT + SELECT benchmark vs SMI2
  bench_http.php          # HTTP worker mode benchmark
  test.php                # PHP integration tests (30 assertions)
  worker.php              # FrankenPHP worker (persistent connection)
  Caddyfile               # FrankenPHP HTTP server config
docker/                   # Docker config (ClickHouse, FrankenPHP)
Makefile
```
