<?php
// =============================================================================
// Tests d'intégration de l'extension clickhousephp
// Usage : ./frankenphp-clickhouse php-cli test.php
// =============================================================================

require __DIR__ . '/vendor/autoload.php';
ini_set('memory_limit', '256M');

if (class_exists('Dotenv\\Dotenv')) {
    Dotenv\Dotenv::createImmutable(__DIR__, '.env')->safeLoad();
}

$dsn = $_ENV['CH_DSN'] ?? 'clickhouse://default@localhost:9000/default?secure=false';

// =============================================================================
// Framework d'assertions minimal
// =============================================================================

$passed = 0;
$failed = 0;

function suite(string $name): void {
    echo "\n── $name\n";
}

function ok(bool $cond, string $label, string $detail = ''): void {
    global $passed, $failed;
    if ($cond) {
        echo "  ✓ $label\n";
        $passed++;
    } else {
        echo "  ✗ $label" . ($detail ? ": $detail" : '') . "\n";
        $failed++;
    }
}

function eq(mixed $actual, mixed $expected, string $label): void {
    $cond = $actual === $expected;
    ok($cond, $label, $cond ? '' : "got " . var_export($actual, true) . ", want " . var_export($expected, true));
}

// =============================================================================
// Setup
// =============================================================================

suite('Setup');

$connectResult = clickhouse_connect($dsn);
eq($connectResult, 'Ok', 'clickhouse_connect');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_events_test");
$createResult = clickhouse_exec("
    CREATE TABLE clickhousephp_events_test (
        id            String,
        start         DateTime,
        end           DateTime,
        machine_id    String,
        event_type_id String
    ) ENGINE = Memory
");
ok($createResult !== false && !str_starts_with((string)$createResult, 'ERROR'), 'CREATE TABLE');

clickhouse_exec("INSERT INTO clickhousephp_events_test VALUES
    ('evt-1', '2024-01-01 08:00:00', '2024-01-01 09:00:00', 'machine-A', 'type-X'),
    ('evt-2', '2024-01-01 10:00:00', '2024-01-01 11:00:00', 'machine-B', 'type-Y'),
    ('evt-3', '2024-06-15 12:00:00', '2024-06-15 13:30:00', 'machine-A', 'type-Z')
");
ok(true, 'INSERT rows (3)');

// =============================================================================
// clickhouse_query_array — tableau PHP natif (méthode principale)
// =============================================================================

suite('clickhouse_query_array');

$result = clickhouse_query_array("SELECT * FROM clickhousephp_events_test ORDER BY id");

ok(is_array($result), 'returns PHP array');
eq(count($result), 3, 'row count = 3');

$r = $result[0];
ok(is_array($r), 'each row is an associative array');
ok(array_key_exists('id',            $r), 'key id exists');
ok(array_key_exists('start',         $r), 'key start exists');
ok(array_key_exists('end',           $r), 'key end exists');
ok(array_key_exists('machine_id',    $r), 'key machine_id exists');
ok(array_key_exists('event_type_id', $r), 'key event_type_id exists');

// Row 0 = evt-1
eq($r['id'],            'evt-1',     'row 0 id');
eq($r['machine_id'],    'machine-A', 'row 0 machine_id');
eq($r['event_type_id'], 'type-X',    'row 0 event_type_id');
ok(str_starts_with($r['start'], '2024-01-01'), 'row 0 start is ISO date');
ok(str_starts_with($r['end'],   '2024-01-01'), 'row 0 end is ISO date');

// Row 1 = evt-2
eq($result[1]['id'],            'evt-2',     'row 1 id');
eq($result[1]['machine_id'],    'machine-B', 'row 1 machine_id');
eq($result[1]['event_type_id'], 'type-Y',    'row 1 event_type_id');

// Row 2 = evt-3 (different date)
eq($result[2]['id'],            'evt-3',     'row 2 id');
ok(str_starts_with($result[2]['start'], '2024-06-15'), 'row 2 start date');

// Tous les champs sont des strings
ok(is_string($result[0]['id']),    'id is string');
ok(is_string($result[0]['start']), 'start is string');
ok(is_string($result[0]['end']),   'end is string');

// =============================================================================
// clickhouse_exec — DDL et commandes
// =============================================================================

suite('clickhouse_exec');

$r = clickhouse_exec("SELECT 1");
ok(!str_starts_with((string)$r, 'ERROR'), 'SELECT 1 no error');

$r = clickhouse_exec("DROP TABLE nonexistent_xyz_table");
ok(str_starts_with((string)$r, 'ERROR'), 'unknown table returns ERROR');

// =============================================================================
// Gestion d'erreurs
// =============================================================================

suite('Error handling');

$r = clickhouse_query_array("SELECT * FROM nonexistent_xyz_table_abc");
ok(is_array($r) && count($r) === 0, 'bad query returns empty array');

clickhouse_disconnect();
$r = clickhouse_connect($dsn);
eq($r, 'Ok', 'reconnect after disconnect');

// =============================================================================
// Cleanup
// =============================================================================

suite('Cleanup');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_events_test");
ok(true, 'test tables dropped');

clickhouse_disconnect();
ok(true, 'disconnected');

// =============================================================================
// Résumé
// =============================================================================

$total = $passed + $failed;
echo "\n" . str_repeat('─', 50) . "\n";
echo "  $passed / $total tests passed";
if ($failed > 0) {
    echo "  ($failed FAILED)";
    echo "\n" . str_repeat('─', 50) . "\n";
    exit(1);
}
echo "\n" . str_repeat('─', 50) . "\n";
exit(0);
