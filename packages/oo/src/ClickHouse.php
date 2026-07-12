<?php

declare(strict_types=1);

namespace Jhondermarck\ClickHouse;

/**
 * Thin object-oriented facade over the procedural clickhouse_* functions.
 *
 * The native extension exposes one process-wide default connection plus extra
 * named handles; this class simply forwards to it, adding cursor/batch handle
 * objects and a Prometheus exporter. Optional — the procedural API stays fully
 * usable.
 */
final class ClickHouse
{
    /** Connects the default pool when a DSN is given (e.g. at worker boot). */
    public function __construct(?string $dsn = null)
    {
        if ($dsn !== null) {
            clickhouse_connect($dsn);
        }
    }

    /** @return list<array<string,mixed>> */
    public function query(string $sql, ?array $params = null, ?array $options = null): array
    {
        return clickhouse_query_array($sql, $params, $options);
    }

    /**
     * Columnar result — one array per column instead of one per row. Lighter
     * and faster on wide/large results.
     *
     * @return array<string,list<mixed>>
     */
    public function columns(string $sql, ?array $params = null, ?array $options = null): array
    {
        return clickhouse_query_columns($sql, $params, $options);
    }

    public function cursor(string $sql, ?array $params = null, ?array $options = null): Cursor
    {
        return new Cursor(clickhouse_query_cursor($sql, $params, $options));
    }

    public function exec(string $sql, ?array $params = null, ?array $options = null): string
    {
        return clickhouse_exec($sql, $params, $options);
    }

    public function insert(string $table, array $values, ?array $columns = null, ?array $options = null): string
    {
        return clickhouse_insert($table, $values, $columns, $options);
    }

    public function batch(string $table, ?array $columns = null, ?array $options = null): Batch
    {
        return new Batch(clickhouse_batch_begin($table, $columns, $options));
    }

    public function asyncInsert(string $sql, bool $wait = true, ?array $params = null, ?array $options = null): string
    {
        return clickhouse_async_insert($sql, $wait, $params, $options);
    }

    public function ping(?int $connection = null): string
    {
        return clickhouse_ping($connection);
    }

    public function serverVersion(?int $connection = null): string
    {
        return clickhouse_server_version($connection);
    }

    /** @return array<string,mixed> */
    public function stats(): array
    {
        return clickhouse_stats();
    }

    /** Prometheus/OpenMetrics exposition for the current runtime snapshot. */
    public function metricsText(string $prefix = 'clickhouse'): string
    {
        return self::formatMetrics($this->stats(), $prefix);
    }

    public function open(string $dsn): int
    {
        return clickhouse_open($dsn);
    }

    public function close(int $connection): string
    {
        return clickhouse_close($connection);
    }

    public function disconnect(): string
    {
        return clickhouse_disconnect();
    }

    /**
     * Render a clickhouse_stats() array as Prometheus text — gauges for the
     * state/pool/handle values, counters for the lifetime totals, and a
     * build-info metric labelled with the server version. Also usable from
     * procedural code: ClickHouse::formatMetrics(clickhouse_stats()).
     *
     * @param array<string,mixed> $stats
     */
    public static function formatMetrics(array $stats, string $prefix = 'clickhouse'): string
    {
        $lines = [];
        $gauge = static function (string $name, int|float $value) use (&$lines, $prefix): void {
            $lines[] = "# TYPE {$prefix}_{$name} gauge";
            $lines[] = "{$prefix}_{$name} {$value}";
        };
        $counter = static function (string $name, int|float $value) use (&$lines, $prefix): void {
            $lines[] = "# TYPE {$prefix}_{$name}_total counter";
            $lines[] = "{$prefix}_{$name}_total {$value}";
        };

        $gauge('connected', (int) ($stats['connected'] ?? 0));
        $gauge('uptime_seconds', (int) ($stats['uptime_seconds'] ?? 0));
        $gauge('named_connections', (int) ($stats['named_connections'] ?? 0));

        foreach ((array) ($stats['handles'] ?? []) as $key => $value) {
            $gauge("handles_{$key}", (int) $value);
        }
        foreach ((array) ($stats['pool'] ?? []) as $key => $value) {
            $gauge("pool_{$key}", (int) $value);
        }
        foreach ((array) ($stats['counters'] ?? []) as $key => $value) {
            $counter($key, (int) $value);
        }

        $timing = (array) ($stats['timing'] ?? []);
        if (isset($timing['operations'])) {
            $counter('query_operations', (int) $timing['operations']);
            $counter('query_duration_us', (int) ($timing['total_us'] ?? 0));
            $gauge('query_duration_us_max', (int) ($timing['max_us'] ?? 0));
        }

        $version = (string) ($stats['server_version'] ?? '');
        if ($version !== '') {
            $version = str_replace(['\\', '"'], ['\\\\', '\\"'], $version);
            $lines[] = "# TYPE {$prefix}_build_info gauge";
            $lines[] = "{$prefix}_build_info{server_version=\"{$version}\"} 1";
        }

        return implode("\n", $lines) . "\n";
    }
}
