<?php

/** @generate-class-entries */

// Manually maintained — kept in sync with clickhousephp.c / clickhousephp_arginfo.h.
// Do NOT regenerate with the FrankenPHP extension generator: the C bridge
// carries hand-written exception handling that regeneration would clobber.

function clickhouse_connect(string $dsn): string {}

function clickhouse_disconnect(): string {}

function clickhouse_insert(string $table, array $values, ?array $columns = null): string {}

function clickhouse_exec(string $query, ?array $params = null): string {}

function clickhouse_query_array(string $query, ?array $params = null): array {}

function clickhouse_query_cursor(string $query, ?array $params = null): int {}

function clickhouse_cursor_fetch(int $cursor, int $max_rows = 10000): array {}

function clickhouse_cursor_close(int $cursor): string {}
