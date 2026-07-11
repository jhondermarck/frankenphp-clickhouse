<?php
// =============================================================================
// ETL example — stream a large table through PHP with bounded memory.
//
// Reads the source with a streaming cursor (only one chunk in memory at a time)
// and writes to the destination with an incremental batch (buffered, flushed
// per chunk). Neither side ever holds the full result set — this is the
// pattern for exports, table-to-table copies, and any dataset too large to
// materialize with clickhouse_query_array().
//
// Run:
//   CH_DSN='clickhouse://default:pass@localhost:9000/db' \
//     ./frankenphp-clickhouse php-cli examples/etl_export.php
//
// Env knobs: ETL_ROWS (source rows, default 500000), ETL_CHUNK (rows per
// fetch/flush, default 50000), ETL_KEEP=1 to keep the demo tables.
// =============================================================================

$dsn   = getenv('CH_DSN') ?: ($argv[1] ?? 'clickhouse://default@localhost:9000/default?secure=false');
$rows  = (int) (getenv('ETL_ROWS') ?: 500_000);
$chunk = (int) (getenv('ETL_CHUNK') ?: 50_000);
$keep  = (bool) getenv('ETL_KEEP');

$src = 'etl_demo_source';
$dst = 'etl_demo_dest';

function fmtMB(int $bytes): string { return number_format($bytes / 1048576, 1) . ' MB'; }

clickhouse_connect($dsn);

// ── Setup: a source table with $rows rows, generated server-side ─────────────
foreach ([$src, $dst] as $t) {
    clickhouse_exec("DROP TABLE IF EXISTS $t");
    clickhouse_exec("CREATE TABLE $t (
        id    UInt64,
        name  String,
        value Float64
    ) ENGINE = MergeTree ORDER BY id");
}
clickhouse_exec("INSERT INTO $src
    SELECT number AS id, concat('row-', toString(number)) AS name, number * 1.5 AS value
    FROM numbers({n:UInt64})", ['n' => $rows]);

printf("Source: %s rows in %s\n", number_format($rows), $src);

// ── ETL: cursor (read) → batch (write), one chunk at a time ──────────────────
$t0     = microtime(true);
$cursor = clickhouse_query_cursor("SELECT id, name, value FROM $src ORDER BY id");
$batch  = clickhouse_batch_begin($dst, ['id', 'name', 'value']);

$copied = 0;
try {
    while (true) {
        $batchRows = clickhouse_cursor_fetch($cursor, $chunk);
        if (count($batchRows) === 0) {
            break;
        }
        clickhouse_batch_append($batch, $batchRows); // assoc rows → columns declared at begin
        clickhouse_batch_flush($batch);              // ship this chunk; memory stays flat
        $copied += count($batchRows);
        printf("\r  copied %s / %s   peak PHP mem %s",
            number_format($copied), number_format($rows), fmtMB(memory_get_peak_usage(true)));
    }
    clickhouse_batch_send($batch);
} catch (\Throwable $e) {
    clickhouse_batch_abort($batch);
    clickhouse_cursor_close($cursor);
    fwrite(STDERR, "\nETL failed: " . $e->getMessage() . "\n");
    exit(1);
}
clickhouse_cursor_close($cursor);

$elapsed = microtime(true) - $t0;

// ── Verify ───────────────────────────────────────────────────────────────────
$dstCount = (int) clickhouse_query_array("SELECT count() AS c FROM $dst")[0]['c'];
printf("\nDone: %s rows copied in %.2fs (%s rows/s), peak PHP mem %s\n",
    number_format($dstCount), $elapsed,
    number_format($elapsed > 0 ? (int) ($dstCount / $elapsed) : 0),
    fmtMB(memory_get_peak_usage(true)));

$ok = $dstCount === $rows;
echo $ok ? "✅ source and destination row counts match\n"
         : "❌ count mismatch: expected $rows, got $dstCount\n";

if (!$keep) {
    foreach ([$src, $dst] as $t) {
        clickhouse_exec("DROP TABLE IF EXISTS $t");
    }
}
clickhouse_disconnect();
exit($ok ? 0 : 1);
