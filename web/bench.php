<?php
require __DIR__ . '/vendor/autoload.php';
ini_set('memory_limit', '4G');

use ClickHouseDB\Client;

// ── Config ────────────────────────────────────────────────────────────────────
if (class_exists('Dotenv\\Dotenv')) {
    Dotenv\Dotenv::createImmutable(__DIR__, '.env')->safeLoad();
}

$dsn        = $_ENV['CH_DSN']          ?? 'clickhouse://default@localhost:9000/default';
$insertRows = (int)($_ENV['CH_INSERT_ROWS'] ?? 100000);
$selectLimit = (int)($_ENV['CH_SELECT_LIMIT'] ?? min($insertRows, 200000));
$benchTable = ($_ENV['CH_DB'] ?? 'default') . '._bench_events';

const WARMUP = 3;
const ITERS  = 20;

// ── Helpers ───────────────────────────────────────────────────────────────────
function benchLoop(callable $fn): array
{
    for ($i = 0; $i < WARMUP; $i++) {
        $r = $fn();
        unset($r);
    }
    $rows  = 0;
    $times = [];
    for ($i = 0; $i < ITERS; $i++) {
        $t = microtime(true);
        $r = $fn();
        $times[] = microtime(true) - $t;
        $rows = is_countable($r) ? count($r) : $rows;
        unset($r);
    }
    sort($times);
    return [
        'avg'  => array_sum($times) / ITERS,
        'min'  => $times[0],
        'p95'  => $times[(int)(ITERS * 0.95)],
        'rows' => $rows,
    ];
}

function benchGo(string $dsn, callable $fn): array
{
    clickhouse_connect($dsn);
    $result = benchLoop($fn);
    clickhouse_disconnect();
    return $result;
}

function printRow(string $label, array $r, float $ref): void
{
    $vs = $ref > 0
        ? sprintf("\u{00D7}%.2f", $ref / $r['avg'])
        : '  ref ';
    printf(
        "  %-48s  avg %6.3fs  min %6.3fs  p95 %6.3fs  %7s rows  %s\n",
        $label,
        $r['avg'],
        $r['min'],
        $r['p95'],
        number_format($r['rows']),
        $vs
    );
}

function printInsertRow(string $label, array $r, int $batchSize, float $ref): void
{
    $rowsPerSec = $batchSize / $r['avg'];
    $vs = $ref > 0
        ? sprintf("\u{00D7}%.2f", $ref / $r['avg'])
        : '  ref ';
    printf(
        "  %-48s  avg %6.3fs  min %6.3fs  p95 %6.3fs  %7s r/s  %s\n",
        $label,
        $r['avg'],
        $r['min'],
        $r['p95'],
        number_format((int)$rowsPerSec),
        $vs
    );
}

function separator(string $c = '─'): void
{
    echo '  ' . str_repeat($c, 100) . "\n";
}

function smi2Client(): Client
{
    $db = new Client([
        'host'     => $_ENV['CH_HOST'] ?? 'localhost',
        'port'     => $_ENV['CH_PORT'] ?? '8123',
        'username' => $_ENV['CH_USER'] ?? 'default',
        'password' => $_ENV['CH_PASS'] ?? '',
        'https'    => (bool)($_ENV['CH_HTTPS'] ?? false),
    ]);
    $db->database($_ENV['CH_DB'] ?? 'default');
    $db->enableHttpCompression(true);
    return $db;
}

// ── Setup table ──────────────────────────────────────────────────────────────
echo "\n  Bench table: $benchTable  ($insertRows rows, SELECT LIMIT $selectLimit)\n";
separator('═');

try {
    clickhouse_connect($dsn);
} catch (RuntimeException $e) {
    fwrite(STDERR, "clickhouse_connect failed: " . $e->getMessage() . "\n");
    exit(1);
}
clickhouse_exec("DROP TABLE IF EXISTS $benchTable");
clickhouse_exec("CREATE TABLE $benchTable (
    id String, start String, end String, machine_id String, event_type_id String
) ENGINE = MergeTree() ORDER BY id");
clickhouse_disconnect();

// ── Generate events ──────────────────────────────────────────────────────────
$insertCols   = ['id', 'start', 'end', 'machine_id', 'event_type_id'];
$insertNested = [];
$insertFlat   = [];

$rangeStart = strtotime('2024-01-01');
$rangeEnd   = strtotime('2025-01-01');

echo "  Generating " . number_format($insertRows) . " events…";
$genStart = microtime(true);
for ($i = 0; $i < $insertRows; $i++) {
    $id    = sprintf('%08x-%04x-%04x-%04x-%012x', $i, mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffffffffffff));
    $ts    = random_int($rangeStart, $rangeEnd);
    $dur   = random_int(60, 86400);
    $start = date('Y-m-d H:i:s', $ts);
    $end   = date('Y-m-d H:i:s', $ts + $dur);
    $mid   = 'machine-' . ($i % 100);
    $tid   = 'type-' . ($i % 20);

    $insertNested[] = [$id, $start, $end, $mid, $tid];
    $insertFlat[] = $id;
    $insertFlat[] = $start;
    $insertFlat[] = $end;
    $insertFlat[] = $mid;
    $insertFlat[] = $tid;
}
printf(" done (%.1fs)\n\n", microtime(true) - $genStart);

// ══════════════════════════════════════════════════════════════════════════════
// INSERT benchmark
// ══════════════════════════════════════════════════════════════════════════════

echo "  INSERT benchmark: " . number_format($insertRows) . " rows per batch\n";
separator('═');
printf("  %-48s  %14s  %14s  %14s  %12s  %s\n", 'Method', 'avg', 'min', 'p95', 'rows/s', 'vs SMI2');
separator();

// ── 1. SMI2 – HTTP insert (référence) ────────────────────────────────────────
$dbW = smi2Client();
if (!$dbW->ping()) {
    fwrite(STDERR, "SMI2 ping failed\n");
    exit(1);
}

$smi2Ins = benchLoop(function () use ($dbW, $benchTable, $insertNested, $insertCols, $insertRows) {
    $dbW->write("TRUNCATE TABLE $benchTable");
    $dbW->insert($benchTable, $insertNested, $insertCols);
    $cnt = (int)$dbW->select("SELECT count() AS c FROM $benchTable")->fetchOne('c');
    if ($cnt !== $insertRows) {
        throw new \RuntimeException("SMI2 insert: expected $insertRows rows, got $cnt");
    }
    return null;
});
unset($dbW);
printInsertRow('SMI2 – HTTP insert', $smi2Ins, $insertRows, 0);
$insRef = $smi2Ins['avg'];

// ── 2. Go TCP + clickhouse_insert (batch) ────────────────────────────────────
$goIns = benchGo($dsn, function () use ($benchTable, $insertFlat, $insertCols, $insertRows) {
    clickhouse_exec("TRUNCATE TABLE $benchTable");
    clickhouse_insert($benchTable, $insertFlat, $insertCols);
    $rows = clickhouse_query_array("SELECT count() AS c FROM $benchTable");
    $cnt = (int)($rows[0]['c'] ?? 0);
    if ($cnt !== $insertRows) {
        throw new \RuntimeException("Go insert: expected $insertRows rows, got $cnt");
    }
    return null;
});
printInsertRow('Go TCP + clickhouse_insert (batch)', $goIns, $insertRows, $insRef);

// ── INSERT résumé ────────────────────────────────────────────────────────────
separator();

$insWinner = $goIns['avg'] < $smi2Ins['avg']
    ? ['Go TCP + clickhouse_insert (batch)', $goIns['avg']]
    : ['SMI2 – HTTP insert', $smi2Ins['avg']];

printf(
    "\n  Winner: %-48s  avg %.3fs  \u{00D7}%.2f vs SMI2  (%s r/s)\n\n",
    $insWinner[0],
    $insWinner[1],
    $insRef / $insWinner[1],
    number_format((int)($insertRows / $insWinner[1]))
);

// ══════════════════════════════════════════════════════════════════════════════
// SELECT benchmark (reads from the table filled by INSERT above)
// ══════════════════════════════════════════════════════════════════════════════

$selectQuery = "SELECT id, start, end, machine_id, event_type_id FROM $benchTable LIMIT $selectLimit";

echo "  SELECT benchmark: $selectQuery\n";
separator('═');
printf("  %-48s  %14s  %14s  %14s  %12s  %s\n", 'Method', 'avg', 'min', 'p95', 'rows', 'vs SMI2');
separator();

// ── 3. SMI2 – HTTP select (référence) ────────────────────────────────────────
$dbR = smi2Client();
$dbR->settings()->set('max_threads', 0);
$dbR->settings()->set('max_block_size', 65536);
$dbR->settings()->set('readonly', 1);

$smi2Sel = benchLoop(function () use ($dbR, $selectQuery) {
    $r = $dbR->select($selectQuery);
    return $r->rows();
});
unset($dbR);
printRow('SMI2 – HTTP + php-array', $smi2Sel, 0);
$selRef = $smi2Sel['avg'];

// ── 4. Go TCP + query_array (générique) ──────────────────────────────────────
$goQueryArray = benchGo($dsn, function () use ($selectQuery) {
    return clickhouse_query_array($selectQuery);
});
printRow('Go TCP + query_array (générique)', $goQueryArray, $selRef);

// ── SELECT résumé ────────────────────────────────────────────────────────────
separator();

$selWinner = $goQueryArray['avg'] < $smi2Sel['avg']
    ? ['Go TCP + query_array (générique)', $goQueryArray['avg']]
    : ['SMI2 – HTTP + php-array', $smi2Sel['avg']];

printf(
    "\n  Winner: %-48s  avg %.3fs  \u{00D7}%.2f vs SMI2\n\n",
    $selWinner[0],
    $selWinner[1],
    $selRef / $selWinner[1]
);

// ── Cleanup ──────────────────────────────────────────────────────────────────
clickhouse_connect($dsn);
clickhouse_exec("DROP TABLE IF EXISTS $benchTable");
clickhouse_disconnect();

separator('═');
echo "\n";
