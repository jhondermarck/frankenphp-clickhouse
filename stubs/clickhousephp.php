<?php

/**
 * IDE stubs for the `clickhousephp` FrankenPHP extension.
 *
 * These declarations exist only so editors and static analysers (PhpStorm,
 * Intelephense, PHPStan, Psalm…) can resolve the extension's global functions
 * with full type information. The file is NOT autoloaded — the real functions
 * are provided by the compiled FrankenPHP binary at runtime, so requiring this
 * file would trigger a redeclaration error. Install it with:
 *
 *     composer require --dev jhondermarck/frankenphp-clickhouse-stubs
 *
 * Every function throws \RuntimeException on error; for server-side errors the
 * exception code carries the ClickHouse error code (see getCode()).
 */

/**
 * Open (or replace) the default, process-wide connection pool.
 *
 * @param string $dsn clickhouse://[user[:pass]@]host:9000[,host2…]/db[?param=value]
 * @return string "Ok"
 * @throws \RuntimeException on a bad DSN, unreachable host, or auth failure
 */
function clickhouse_connect(string $dsn): string {}

/**
 * Close the default connection pool.
 *
 * @return string "Ok"
 * @throws \RuntimeException when no default connection is open
 */
function clickhouse_disconnect(): string {}

/**
 * Open an extra named connection and return its handle, for routing calls to a
 * second cluster/database via the `connection` option.
 *
 * @param string $dsn see {@see clickhouse_connect()}
 * @return int a connection handle (> 0)
 * @throws \RuntimeException on connection failure
 */
function clickhouse_open(string $dsn): int {}

/**
 * Close a connection opened with {@see clickhouse_open()}.
 *
 * @param int $connection handle from clickhouse_open()
 * @return string "Ok"
 * @throws \RuntimeException if the handle is unknown
 */
function clickhouse_close(int $connection): string {}

/**
 * Run a SELECT and return every row as a native PHP array (no JSON).
 *
 * @param string $query SQL with optional {name:Type} placeholders
 * @param array<string,mixed>|list<mixed>|null $params named (assoc) or positional (list) bindings
 * @param array{connection?:int,settings?:array<string,mixed>,query_id?:string,timeout?:string}|null $options
 * @return list<array<string,mixed>> rows, each an associative array keyed by column
 * @throws \RuntimeException on query error or an unsupported column type
 */
function clickhouse_query_array(string $query, ?array $params = null, ?array $options = null): array {}

/**
 * Open a streaming cursor for a SELECT — bounded memory, fetched in chunks.
 * Always release it with {@see clickhouse_cursor_close()}.
 *
 * @param string $query SQL with optional {name:Type} placeholders
 * @param array<string,mixed>|list<mixed>|null $params bindings
 * @param array{connection?:int,settings?:array<string,mixed>,query_id?:string,timeout?:string}|null $options
 * @return int a cursor handle (> 0)
 * @throws \RuntimeException on query error
 */
function clickhouse_query_cursor(string $query, ?array $params = null, ?array $options = null): int {}

/**
 * Fetch the next chunk of rows from a cursor. Returns an empty array once the
 * result is exhausted.
 *
 * @param int $cursor handle from clickhouse_query_cursor()
 * @param int $max_rows maximum rows to return this call
 * @return list<array<string,mixed>> rows (empty when exhausted)
 * @throws \RuntimeException on a mid-stream failure or an unknown cursor
 */
function clickhouse_cursor_fetch(int $cursor, int $max_rows = 10000): array {}

/**
 * Close a cursor and release its pooled connection.
 *
 * @param int $cursor handle from clickhouse_query_cursor()
 * @return string "Ok"
 */
function clickhouse_cursor_close(int $cursor): string {}

/**
 * Execute a statement that returns no rows (DDL, TRUNCATE, KILL QUERY…).
 *
 * @param string $query SQL with optional {name:Type} placeholders
 * @param array<string,mixed>|list<mixed>|null $params bindings
 * @param array{connection?:int,settings?:array<string,mixed>,query_id?:string,timeout?:string}|null $options
 * @return string "Ok"
 * @throws \RuntimeException on error
 */
function clickhouse_exec(string $query, ?array $params = null, ?array $options = null): string {}

/**
 * Batch INSERT from a fully-materialized payload. Accepts flat values (with
 * $columns), nested rows, or associative rows (columns inferred from keys).
 *
 * @param string $table target table (validated identifier, never user input)
 * @param array $values flat list, list of rows, or list of assoc rows
 * @param list<string>|null $columns column names (required for flat/assoc input)
 * @param array{connection?:int,settings?:array<string,mixed>,query_id?:string,timeout?:string}|null $options
 * @return string "Ok"
 * @throws \RuntimeException on a value/column mismatch or server error
 */
function clickhouse_insert(string $table, array $values, ?array $columns = null, ?array $options = null): string {}

/**
 * Begin an incremental insert batch for unbounded-size writes; returns a
 * handle. Append chunks, flush periodically, then send or abort. The pooled
 * connection is held from begin to send/abort.
 *
 * @param string $table target table
 * @param list<string>|null $columns column names (required for flat/assoc appends)
 * @param array{connection?:int,settings?:array<string,mixed>,query_id?:string,timeout?:string}|null $options
 * @return int a batch handle (> 0)
 * @throws \RuntimeException on error
 */
function clickhouse_batch_begin(string $table, ?array $columns = null, ?array $options = null): int {}

/**
 * Append rows to an open batch (buffered client-side).
 *
 * @param int $batch handle from clickhouse_batch_begin()
 * @param array $values same shapes as {@see clickhouse_insert()} $values
 * @return string "Ok"
 * @throws \RuntimeException if the batch is closed or the rows mismatch
 */
function clickhouse_batch_append(int $batch, array $values): string {}

/**
 * Ship buffered rows to the server, keeping the batch open (memory stays
 * bounded on both sides).
 *
 * @param int $batch handle from clickhouse_batch_begin()
 * @return string "Ok"
 * @throws \RuntimeException on flush failure (the batch is then closed)
 */
function clickhouse_batch_flush(int $batch): string {}

/**
 * Finalize a batch: flush any buffered rows and commit, then close it.
 *
 * @param int $batch handle from clickhouse_batch_begin()
 * @return string "Ok"
 * @throws \RuntimeException on send failure
 */
function clickhouse_batch_send(int $batch): string {}

/**
 * Discard an open batch. Rows already flushed remain on the server; buffered
 * rows are dropped.
 *
 * @param int $batch handle from clickhouse_batch_begin()
 * @return string "Ok"
 */
function clickhouse_batch_abort(int $batch): string {}

/**
 * Server-side buffered INSERT for high-frequency small writes.
 *
 * @param string $query INSERT … VALUES (…) with optional {name:Type} placeholders
 * @param bool $wait true = return once the server flushed the buffer (durable);
 *                   false = fire-and-forget
 * @param array<string,mixed>|list<mixed>|null $params bindings
 * @param array{connection?:int,settings?:array<string,mixed>,query_id?:string,timeout?:string}|null $options
 * @return string "Ok"
 * @throws \RuntimeException on error
 */
function clickhouse_async_insert(string $query, bool $wait = true, ?array $params = null, ?array $options = null): string {}

/**
 * Ping a connection.
 *
 * @param int|null $connection handle from clickhouse_open() (null = default)
 * @return string "Ok"
 * @throws \RuntimeException if the server is unreachable
 */
function clickhouse_ping(?int $connection = null): string {}

/**
 * Return the server version as "major.minor.patch".
 *
 * @param int|null $connection handle from clickhouse_open() (null = default)
 * @return string e.g. "26.5.5"
 * @throws \RuntimeException on error
 */
function clickhouse_server_version(?int $connection = null): string {}

/**
 * Return a process-wide runtime snapshot (no server round-trip) for health
 * checks and leak diagnosis.
 *
 * @return array{
 *   connected:int,
 *   uptime_seconds:int,
 *   server_version:string,
 *   named_connections:int,
 *   handles:array{cursors_open:int,batches_open:int,idle_ttl_seconds:int,last_reap_unix:int,last_reap_count:int},
 *   pool:array{open?:int,idle?:int,max_open_conns?:int,max_idle_conns?:int},
 *   counters:array{queries:int,inserts:int,execs:int,async_inserts:int,cursors_opened:int,batches_opened:int,errors:int}
 * }
 */
function clickhouse_stats(): array {}
