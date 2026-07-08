<?php
// =============================================================================
// Memory benchmark — streaming cursor vs full materialization
// Usage: ./frankenphp-clickhouse php-cli web/bench_memory.php
//
// Data is generated SERVER-SIDE (numbers()) so the PHP process peak reflects
// only the read path — an insert phase in PHP would dominate the watermark.
// =============================================================================

require __DIR__ . '/vendor/autoload.php';
ini_set('memory_limit', '8G');

if (class_exists('Dotenv\\Dotenv')) {
    Dotenv\Dotenv::createImmutable(__DIR__, '.env')->safeLoad();
}

$dsn     = $_ENV['CH_DSN'] ?? 'clickhouse://default@localhost:9000/default?secure=false';
$numRows = (int)($_ENV['CH_MEM_ROWS'] ?? 1000000);
$table   = ($_ENV['CH_DB'] ?? 'default') . '._bench_memory';

clickhouse_connect($dsn);
clickhouse_exec("DROP TABLE IF EXISTS $table");
clickhouse_exec("CREATE TABLE $table (
    id String, start DateTime, machine_id String, type UInt8, value Float64
) ENGINE = MergeTree() ORDER BY id");
clickhouse_exec("
    INSERT INTO $table
    SELECT
        concat('evt-', toString(number)),
        now() - toIntervalSecond(number % 86400),
        concat('machine-', toString(number % 100)),
        number % 20,
        number / 7
    FROM numbers($numRows)
");

$query = "SELECT * FROM $table";

printf("\n  Memory peak — cursor (10k chunks) vs query_array, %s rows\n", number_format($numRows));
echo '  ' . str_repeat('─', 70) . "\n";

// Peak memory is monotonic within a process — measure the cursor first
// (low watermark), then query_array (which dominates the final peak).
$base = memory_get_peak_usage(true);

$cur = clickhouse_query_cursor($query);
$streamed = 0;
while (count($chunk = clickhouse_cursor_fetch($cur, 10000)) > 0) {
    $streamed += count($chunk);
    unset($chunk);
}
clickhouse_cursor_close($cur);
$cursorPeak = memory_get_peak_usage(true) - $base;

$rows = clickhouse_query_array($query);
$arrayPeak = memory_get_peak_usage(true) - $base;
$materialized = count($rows);
unset($rows);

clickhouse_exec("DROP TABLE IF EXISTS $table");
clickhouse_disconnect();

printf("  cursor (streaming)     peak %+9.1f MB   (%s rows)\n", $cursorPeak / 1048576, number_format($streamed));
printf("  query_array            peak %+9.1f MB   (%s rows)\n", $arrayPeak / 1048576, number_format($materialized));
echo '  ' . str_repeat('─', 70) . "\n\n";

if ($streamed !== $numRows || $materialized !== $numRows) {
    fwrite(STDERR, "row count mismatch\n");
    exit(1);
}
