<?php

/** @generate-class-entries */

function clickhouse_connect(string $dsn): string {}

function clickhouse_disconnect(): string {}

function clickhouse_insert(string $table,array $values,array $columns): string {}

function clickhouse_exec(string $query): string {}

function clickhouse_query_array(string $query): array {}


