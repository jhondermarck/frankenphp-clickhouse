<?php

/** @generate-class-entries */

// Manually maintained — kept in sync with clickhousephp.c / clickhousephp_arginfo.h.
// Do NOT regenerate with the FrankenPHP extension generator: the C bridge
// carries hand-written exception handling that regeneration would clobber.

function clickhouse_connect(string $dsn): string {}

function clickhouse_disconnect(): string {}

function clickhouse_insert(string $table, array $values, ?array $columns = null, ?array $options = null): string {}

function clickhouse_exec(string $query, ?array $params = null, ?array $options = null): string {}

function clickhouse_query_array(string $query, ?array $params = null, ?array $options = null): array {}

function clickhouse_query_cursor(string $query, ?array $params = null, ?array $options = null): int {}

function clickhouse_cursor_fetch(int $cursor, int $max_rows = 10000): array {}

function clickhouse_cursor_close(int $cursor): string {}

function clickhouse_ping(?int $connection = null): string {}

function clickhouse_server_version(?int $connection = null): string {}

function clickhouse_batch_begin(string $table, ?array $columns = null, ?array $options = null): int {}

function clickhouse_batch_append(int $batch, array $values): string {}

function clickhouse_batch_flush(int $batch): string {}

function clickhouse_batch_send(int $batch): string {}

function clickhouse_batch_abort(int $batch): string {}

function clickhouse_async_insert(string $query, bool $wait = true, ?array $params = null, ?array $options = null): string {}

function clickhouse_open(string $dsn): int {}

function clickhouse_close(int $connection): string {}
