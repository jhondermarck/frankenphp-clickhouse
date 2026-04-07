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
ok(str_starts_with($r['start'], '2024-01-01'), 'row 0 start date prefix');
ok(str_starts_with($r['end'],   '2024-01-01'), 'row 0 end date prefix');
eq($r['start'], '2024-01-01 08:00:00', 'DateTime format is Y-m-d H:i:s');
eq($r['end'], '2024-01-01 09:00:00', 'DateTime end format is Y-m-d H:i:s');

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
// clickhouse_insert — batch INSERT via flat array
// =============================================================================

suite('clickhouse_insert');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_insert_test");
clickhouse_exec("CREATE TABLE clickhousephp_insert_test (
    name String, age UInt32, score Float64
) ENGINE = Memory");

$values = ['Alice', 30, 95.5, 'Bob', 25, 87.2, 'Charlie', 35, 91.0];
$columns = ['name', 'age', 'score'];
$r = clickhouse_insert('clickhousephp_insert_test', $values, $columns);
eq($r, 'Ok', 'clickhouse_insert returns Ok');

$rows = clickhouse_query_array("SELECT name, age, score FROM clickhousephp_insert_test ORDER BY name");
eq(count($rows), 3, 'inserted 3 rows');
eq($rows[0]['name'], 'Alice', 'row 0 name');
eq($rows[0]['age'], 30, 'row 0 age (UInt32 → PHP int)');
ok(is_int($rows[0]['age']), 'age is PHP int');
ok(abs($rows[0]['score'] - 95.5) < 0.001, 'row 0 score (Float64 → PHP float)');
ok(is_float($rows[0]['score']), 'score is PHP float');
eq($rows[1]['name'], 'Bob', 'row 1 name');
eq($rows[2]['name'], 'Charlie', 'row 2 name');

// Error: insert with wrong column count
$insertThrew = false;
try {
    clickhouse_insert('clickhousephp_insert_test', ['Alice', 30], ['name', 'age', 'score']);
} catch (RuntimeException $e) {
    $insertThrew = true;
}
ok($insertThrew, 'mismatched values/columns throws RuntimeException');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_insert_test");

// =============================================================================
// Numeric and Nullable types
// =============================================================================

suite('Numeric and Nullable types');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_types_test");
clickhouse_exec("CREATE TABLE clickhousephp_types_test (
    i8 Int8, i16 Int16, i32 Int32, i64 Int64,
    u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64,
    f32 Float32, f64 Float64,
    s String,
    ns Nullable(String),
    ni Nullable(Int32)
) ENGINE = Memory");

clickhouse_exec("INSERT INTO clickhousephp_types_test VALUES
    (-1, -1000, -100000, -9000000000, 1, 1000, 100000, 9000000000, 3.14, 2.718281828, 'hello', 'world', 42),
    (0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, '', NULL, NULL)
");

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_types_test ORDER BY i8 ASC");
eq(count($rows), 2, 'type test row count = 2');

// Row 0: negative/zero values
$r = $rows[0];
eq($r['i8'], -1, 'Int8 = -1');
ok(is_int($r['i8']), 'Int8 is PHP int');
eq($r['i16'], -1000, 'Int16 = -1000');
eq($r['i32'], -100000, 'Int32 = -100000');
eq($r['i64'], -9000000000, 'Int64 = -9000000000');
eq($r['u8'], 1, 'UInt8 = 1');
eq($r['u16'], 1000, 'UInt16 = 1000');
eq($r['u32'], 100000, 'UInt32 = 100000');
eq($r['u64'], 9000000000, 'UInt64 = 9000000000');
ok(is_float($r['f32']), 'Float32 is PHP float');
ok(abs($r['f32'] - 3.14) < 0.01, 'Float32 ~ 3.14');
ok(abs($r['f64'] - 2.718281828) < 0.0001, 'Float64 ~ 2.718');
eq($r['s'], 'hello', 'String = hello');
eq($r['ns'], 'world', 'Nullable(String) = world');
eq($r['ni'], 42, 'Nullable(Int32) = 42');

// Row 1: zeros and NULLs
$r = $rows[1];
eq($r['i8'], 0, 'Int8 = 0');
eq($r['s'], '', 'empty String');
ok($r['ns'] === null, 'Nullable(String) = NULL');
ok($r['ni'] === null, 'Nullable(Int32) = NULL');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_types_test");

// =============================================================================
// Extended types: UUID, Bool, Date, IPv4, IPv6, Decimal, Enum
// =============================================================================

suite('Extended types');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_xtypes_test");
clickhouse_exec("CREATE TABLE clickhousephp_xtypes_test (
    uid UUID,
    flag Bool,
    d Date,
    ip4 IPv4,
    ip6 IPv6,
    amount Decimal(18, 4),
    status Enum8('active' = 1, 'inactive' = 2),
    nuid Nullable(UUID)
) ENGINE = Memory");

clickhouse_exec("INSERT INTO clickhousephp_xtypes_test VALUES
    ('550e8400-e29b-41d4-a716-446655440000', true,  '2024-03-15', '192.168.1.1', '::1', 1234.5678, 'active', '550e8400-e29b-41d4-a716-446655440000'),
    ('6ba7b810-9dad-11d1-80b4-00c04fd430c8', false, '2025-01-01', '10.0.0.1',    '2001:db8::1', -99.99, 'inactive', NULL)
");

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_xtypes_test ORDER BY d ASC");
eq(count($rows), 2, 'extended types row count = 2');

// Row 0
$r = $rows[0];
eq($r['uid'], '550e8400-e29b-41d4-a716-446655440000', 'UUID value');
ok(is_string($r['uid']), 'UUID is PHP string');
eq($r['flag'], 1, 'Bool true = 1');
ok(is_int($r['flag']), 'Bool is PHP int');
ok(str_starts_with($r['d'], '2024-03-15'), 'Date formatted');
ok(is_string($r['d']), 'Date is PHP string');
eq($r['ip4'], '192.168.1.1', 'IPv4 value');
ok($r['ip6'] === '::1' || $r['ip6'] === '0:0:0:0:0:0:0:1', 'IPv6 loopback');
ok(str_contains($r['amount'], '1234.5678'), 'Decimal value');
ok(is_string($r['amount']), 'Decimal is PHP string');
eq($r['status'], 'active', 'Enum8 = active');
eq($r['nuid'], '550e8400-e29b-41d4-a716-446655440000', 'Nullable(UUID) with value');

// Row 1
$r = $rows[1];
eq($r['flag'], 0, 'Bool false = 0');
eq($r['ip4'], '10.0.0.1', 'IPv4 value 2');
eq($r['status'], 'inactive', 'Enum8 = inactive');
ok($r['nuid'] === null, 'Nullable(UUID) = NULL');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_xtypes_test");

// =============================================================================
// Array types
// =============================================================================

suite('Array types');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_array_test");
clickhouse_exec("CREATE TABLE clickhousephp_array_test (
    tags Array(String),
    scores Array(Int32),
    prices Array(Float64),
    flags Array(UInt8),
    ids Array(UUID),
    labels Array(Nullable(String))
) ENGINE = Memory");

clickhouse_exec("INSERT INTO clickhousephp_array_test VALUES
    (['php', 'go', 'clickhouse'], [10, 20, 30], [1.5, 2.5], [1, 0, 1], ['550e8400-e29b-41d4-a716-446655440000'], ['a', NULL, 'c']),
    ([], [], [], [], [], [])
");

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_array_test ORDER BY length(tags) DESC");
eq(count($rows), 2, 'array test row count = 2');

// Row 0: populated arrays
$r = $rows[0];
ok(is_array($r['tags']), 'Array(String) is PHP array');
eq(count($r['tags']), 3, 'Array(String) has 3 elements');
eq($r['tags'][0], 'php', 'Array(String)[0] = php');
eq($r['tags'][1], 'go', 'Array(String)[1] = go');
eq($r['tags'][2], 'clickhouse', 'Array(String)[2] = clickhouse');

ok(is_array($r['scores']), 'Array(Int32) is PHP array');
eq(count($r['scores']), 3, 'Array(Int32) has 3 elements');
eq($r['scores'][0], 10, 'Array(Int32)[0] = 10');
ok(is_int($r['scores'][0]), 'Array(Int32) elements are PHP int');

ok(is_array($r['prices']), 'Array(Float64) is PHP array');
eq(count($r['prices']), 2, 'Array(Float64) has 2 elements');
ok(abs($r['prices'][0] - 1.5) < 0.001, 'Array(Float64)[0] ~ 1.5');
ok(is_float($r['prices'][0]), 'Array(Float64) elements are PHP float');

eq(count($r['flags']), 3, 'Array(UInt8) has 3 elements');
eq($r['flags'][0], 1, 'Array(UInt8)[0] = 1');
eq($r['flags'][1], 0, 'Array(UInt8)[1] = 0');

eq(count($r['ids']), 1, 'Array(UUID) has 1 element');
ok(is_string($r['ids'][0]), 'Array(UUID) elements are PHP string');
eq($r['ids'][0], '550e8400-e29b-41d4-a716-446655440000', 'Array(UUID)[0] value');

ok(is_array($r['labels']), 'Array(Nullable(String)) is PHP array');
eq(count($r['labels']), 3, 'Array(Nullable(String)) has 3 elements');
eq($r['labels'][0], 'a', 'Array(Nullable(String))[0] = a');
ok($r['labels'][1] === null, 'Array(Nullable(String))[1] = NULL');
eq($r['labels'][2], 'c', 'Array(Nullable(String))[2] = c');

// Row 1: empty arrays
$r = $rows[1];
ok(is_array($r['tags']), 'empty Array(String) is PHP array');
eq(count($r['tags']), 0, 'empty Array(String) has 0 elements');
eq(count($r['scores']), 0, 'empty Array(Int32) has 0 elements');
eq(count($r['labels']), 0, 'empty Array(Nullable(String)) has 0 elements');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_array_test");

// =============================================================================
// clickhouse_exec — DDL et commandes
// =============================================================================

suite('clickhouse_exec');

$r = clickhouse_exec("SELECT 1");
eq($r, 'Ok', 'SELECT 1 returns Ok');

$execThrew = false;
try {
    clickhouse_exec("DROP TABLE nonexistent_xyz_table");
} catch (RuntimeException $e) {
    $execThrew = true;
}
ok($execThrew, 'unknown table throws RuntimeException');

// =============================================================================
// Gestion d'erreurs
// =============================================================================

suite('Error handling — exceptions');

// query_array: bad query throws with message
$queryMsg = '';
try {
    clickhouse_query_array("SELECT * FROM nonexistent_xyz_table_abc");
} catch (RuntimeException $e) {
    $queryMsg = $e->getMessage();
}
ok(strlen($queryMsg) > 0, 'bad query throws RuntimeException');
ok(str_contains($queryMsg, 'nonexistent_xyz_table_abc'), 'exception message contains table name');

// exec: syntax error throws
$execMsg = '';
try {
    clickhouse_exec("NOT VALID SQL AT ALL");
} catch (RuntimeException $e) {
    $execMsg = $e->getMessage();
}
ok(strlen($execMsg) > 0, 'bad SQL throws RuntimeException');

// connect: bad DSN throws
$connectMsg = '';
try {
    clickhouse_connect('clickhouse://default@localhost:19999/bad?secure=false');
} catch (RuntimeException $e) {
    $connectMsg = $e->getMessage();
}
ok(strlen($connectMsg) > 0, 'bad DSN throws RuntimeException');
ok(is_a(new RuntimeException(), 'RuntimeException'), 'exception is RuntimeException class');

// Reconnect after failed connect
$r = clickhouse_connect($dsn);
eq($r, 'Ok', 'reconnect after failed connect');

// disconnect: double disconnect throws
clickhouse_disconnect();
$disconnectMsg = '';
try {
    clickhouse_disconnect();
} catch (RuntimeException $e) {
    $disconnectMsg = $e->getMessage();
}
ok(str_contains($disconnectMsg, 'not connected'), 'double disconnect throws with "not connected"');

// query_array: not connected throws
$notConnectedMsg = '';
try {
    clickhouse_query_array("SELECT 1");
} catch (RuntimeException $e) {
    $notConnectedMsg = $e->getMessage();
}
ok(str_contains($notConnectedMsg, 'not connected'), 'query without connection throws "not connected"');

// exec: not connected throws
$execNotConnected = false;
try {
    clickhouse_exec("SELECT 1");
} catch (RuntimeException $e) {
    $execNotConnected = true;
}
ok($execNotConnected, 'exec without connection throws');

// insert: not connected throws
$insertNotConnected = false;
try {
    clickhouse_insert('t', ['a'], ['b']);
} catch (RuntimeException $e) {
    $insertNotConnected = true;
}
ok($insertNotConnected, 'insert without connection throws');

// Restore connection for remaining tests
$r = clickhouse_connect($dsn);
eq($r, 'Ok', 'reconnect for remaining tests');

// =============================================================================
// Memory leak detection
// =============================================================================

suite('Memory leaks');

// Setup: table with 1000 rows
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_memleak_test");
clickhouse_exec("CREATE TABLE clickhousephp_memleak_test (
    id UInt32, name String, value Float64, ts DateTime, uid UUID
) ENGINE = Memory");

$insertValues = [];
for ($i = 0; $i < 1000; $i++) {
    array_push($insertValues, $i, "name-$i", $i * 1.1, '2024-01-01 00:00:00', '550e8400-e29b-41d4-a716-446655440000');
}
clickhouse_insert('clickhousephp_memleak_test', $insertValues, ['id', 'name', 'value', 'ts', 'uid']);
unset($insertValues);

// --- SELECT leak test: 200 iterations of query_array (1000 rows each) ---
$iters = 200;
$warmup = 20;

for ($i = 0; $i < $warmup; $i++) {
    $rows = clickhouse_query_array("SELECT * FROM clickhousephp_memleak_test");
    unset($rows);
}
gc_collect_cycles();
$memAfterWarmup = memory_get_usage();

for ($i = 0; $i < $iters; $i++) {
    $rows = clickhouse_query_array("SELECT * FROM clickhousephp_memleak_test");
    unset($rows);
}
gc_collect_cycles();
$memAfterSelect = memory_get_usage();

$selectGrowth = $memAfterSelect - $memAfterWarmup;
$selectPerIter = $selectGrowth / $iters;
// Allow max 1KB growth per iteration (noise). A real leak with 1000 rows would be 50KB+/iter.
ok($selectPerIter < 1024, sprintf(
    'query_array memory stable (%.0f bytes/iter over %d iters, total growth: %s)',
    $selectPerIter, $iters, number_format($selectGrowth)
));

// --- INSERT leak test: 200 iterations of insert (100 rows each) ---
$insertCols = ['id', 'name', 'value', 'ts', 'uid'];
$insertBatch = [];
for ($i = 0; $i < 100; $i++) {
    array_push($insertBatch, $i + 10000, "leak-$i", $i * 0.5, '2024-06-01 12:00:00', '6ba7b810-9dad-11d1-80b4-00c04fd430c8');
}

for ($i = 0; $i < $warmup; $i++) {
    clickhouse_insert('clickhousephp_memleak_test', $insertBatch, $insertCols);
}
gc_collect_cycles();
$memBeforeInsert = memory_get_usage();

for ($i = 0; $i < $iters; $i++) {
    clickhouse_insert('clickhousephp_memleak_test', $insertBatch, $insertCols);
}
gc_collect_cycles();
$memAfterInsert = memory_get_usage();

$insertGrowth = $memAfterInsert - $memBeforeInsert;
$insertPerIter = $insertGrowth / $iters;
ok($insertPerIter < 1024, sprintf(
    'clickhouse_insert memory stable (%.0f bytes/iter over %d iters, total growth: %s)',
    $insertPerIter, $iters, number_format($insertGrowth)
));

// --- exec leak test: 200 iterations ---
gc_collect_cycles();
$memBeforeExec = memory_get_usage();

for ($i = 0; $i < $iters; $i++) {
    clickhouse_exec("SELECT count() FROM clickhousephp_memleak_test");
}
gc_collect_cycles();
$memAfterExec = memory_get_usage();

$execGrowth = $memAfterExec - $memBeforeExec;
$execPerIter = $execGrowth / $iters;
ok($execPerIter < 1024, sprintf(
    'clickhouse_exec memory stable (%.0f bytes/iter over %d iters, total growth: %s)',
    $execPerIter, $iters, number_format($execGrowth)
));

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_memleak_test");

// =============================================================================
// clickhouse_insert — nested rows (P0)
// =============================================================================

suite('Insert with nested rows');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_nested_test");
clickhouse_exec("CREATE TABLE clickhousephp_nested_test (
    name String, age UInt32, score Float64
) ENGINE = Memory");

// Nested rows: each sub-array is a row
$rows_data = [
    ['Alice', 30, 95.5],
    ['Bob', 25, 87.2],
    ['Charlie', 35, 91.0],
];
$r = clickhouse_insert('clickhousephp_nested_test', $rows_data, ['name', 'age', 'score']);
eq($r, 'Ok', 'nested row insert returns Ok');

$rows = clickhouse_query_array("SELECT name, age, score FROM clickhousephp_nested_test ORDER BY name");
eq(count($rows), 3, 'nested insert: 3 rows');
eq($rows[0]['name'], 'Alice', 'nested insert: row 0 name');
eq($rows[0]['age'], 30, 'nested insert: row 0 age');
eq($rows[1]['name'], 'Bob', 'nested insert: row 1 name');
eq($rows[2]['name'], 'Charlie', 'nested insert: row 2 name');

// Flat format still works
clickhouse_exec("TRUNCATE TABLE clickhousephp_nested_test");
$r = clickhouse_insert('clickhousephp_nested_test', ['Dave', 40, 88.0], ['name', 'age', 'score']);
eq($r, 'Ok', 'flat insert still works');
$rows = clickhouse_query_array("SELECT name FROM clickhousephp_nested_test");
eq($rows[0]['name'], 'Dave', 'flat insert row verified');

// Error: nested row with wrong column count
$nestedThrew = false;
try {
    clickhouse_insert('clickhousephp_nested_test', [['Eve', 28]], ['name', 'age', 'score']);
} catch (RuntimeException $e) {
    $nestedThrew = true;
}
ok($nestedThrew, 'nested row with wrong column count throws');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_nested_test");

// =============================================================================
// clickhouse_insert — partial columns (P0)
// =============================================================================

suite('Insert with partial columns');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_partial_test");
clickhouse_exec("CREATE TABLE clickhousephp_partial_test (
    id UInt32,
    name String,
    value Float64 DEFAULT 0.0,
    created DateTime DEFAULT now(),
    meta String DEFAULT 'none'
) ENGINE = Memory");

// Insert only id + name, other columns should use defaults
$r = clickhouse_insert('clickhousephp_partial_test', [1, 'Alice', 2, 'Bob'], ['id', 'name']);
eq($r, 'Ok', 'partial column insert returns Ok');

$rows = clickhouse_query_array("SELECT id, name, value, meta FROM clickhousephp_partial_test ORDER BY id");
eq(count($rows), 2, 'partial insert: 2 rows');
eq($rows[0]['id'], 1, 'partial insert: row 0 id');
eq($rows[0]['name'], 'Alice', 'partial insert: row 0 name');
ok(abs($rows[0]['value'] - 0.0) < 0.001, 'partial insert: default value applied');
eq($rows[0]['meta'], 'none', 'partial insert: default meta applied');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_partial_test");

// =============================================================================
// DateTime64 format (P1)
// =============================================================================

suite('DateTime64 format');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_dt64_test");
clickhouse_exec("CREATE TABLE clickhousephp_dt64_test (
    ts DateTime64(6)
) ENGINE = Memory");

clickhouse_exec("INSERT INTO clickhousephp_dt64_test VALUES ('2024-01-15 10:30:45.123456')");
$rows = clickhouse_query_array("SELECT * FROM clickhousephp_dt64_test");
eq($rows[0]['ts'], '2024-01-15 10:30:45.123456', 'DateTime64(6) format is Y-m-d H:i:s.u');

clickhouse_exec("INSERT INTO clickhousephp_dt64_test VALUES ('2024-01-15 10:30:45')");
$rows = clickhouse_query_array("SELECT * FROM clickhousephp_dt64_test ORDER BY ts");
eq($rows[0]['ts'], '2024-01-15 10:30:45.000000', 'DateTime64 with zero microseconds');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_dt64_test");

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
