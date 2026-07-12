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
// clickhouse_query_columns — columnar result mode
// =============================================================================

suite('clickhouse_query_columns');

$cols = clickhouse_query_columns("SELECT * FROM clickhousephp_events_test ORDER BY id");
ok(is_array($cols), 'returns a PHP array');
eq(array_keys($cols), ['id', 'start', 'end', 'machine_id', 'event_type_id'], 'keyed by column name');
ok(is_array($cols['id']) && count($cols['id']) === 3, 'each column is an array of 3 values');
eq($cols['id'], ['evt-1', 'evt-2', 'evt-3'], 'id column values in row order');
eq($cols['machine_id'], ['machine-A', 'machine-B', 'machine-A'], 'machine_id column values');
eq($cols['start'][0], '2024-01-01 08:00:00', 'DateTime formatted in columnar mode');

// Columnar is a transpose of query_array — same data, different shape.
$rowsView = clickhouse_query_array("SELECT * FROM clickhousephp_events_test ORDER BY id");
eq($cols['event_type_id'][2], $rowsView[2]['event_type_id'], 'columns[col][i] == rows[i][col]');

// Typed values, arrays and nulls survive the columnar path.
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_qc_test");
clickhouse_exec("CREATE TABLE clickhousephp_qc_test (
    n UInt32, tags Array(String), score Nullable(Float64)
) ENGINE = Memory");
clickhouse_exec("INSERT INTO clickhousephp_qc_test VALUES
    (10, ['x', 'y'], 1.5), (20, [], NULL)");
$qc = clickhouse_query_columns("SELECT * FROM clickhousephp_qc_test ORDER BY n");
eq($qc['n'], [10, 20], 'UInt32 column');
ok(is_int($qc['n'][0]), 'column values keep their PHP type (int)');
eq($qc['tags'], [['x', 'y'], []], 'Array(String) column of nested arrays');
eq($qc['score'][0], 1.5, 'Nullable float value');
ok($qc['score'][1] === null, 'Nullable NULL in columnar mode');
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_qc_test");

// Empty result → empty columns (still keyed).
$empty = clickhouse_query_columns("SELECT * FROM clickhousephp_events_test WHERE id = 'nope'");
eq(count($empty['id']), 0, 'empty result → empty column arrays');

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
// Tuple types (named, unnamed, nested, Array(Tuple), Map(_, Tuple))
// =============================================================================

suite('Tuple types');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_tuple_test");
clickhouse_exec("CREATE TABLE clickhousephp_tuple_test (
    unnamed  Tuple(UInt32, String),
    named    Tuple(id UInt64, label String, ts DateTime),
    nested   Tuple(inner Tuple(x Int32, y Int32), tags Array(String)),
    nul      Tuple(a Nullable(String), b Nullable(Int64)),
    arr_tup  Array(Tuple(k String, v UInt32)),
    map_tup  Map(String, Tuple(lo Int32, hi Int32))
) ENGINE = Memory");

clickhouse_exec("INSERT INTO clickhousephp_tuple_test VALUES
    ((42, 'hello'),
     (7, 'seven', '2024-01-15 08:30:00'),
     ((1, 2), ['a', 'b']),
     ('present', NULL),
     [('x', 1), ('y', 2)],
     {'range': (10, 20)})
");

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_tuple_test");
eq(count($rows), 1, 'tuple test row count = 1');
$r = $rows[0];

// Unnamed tuple → indexed array in field order
ok(is_array($r['unnamed']), 'unnamed Tuple is PHP array');
eq($r['unnamed'][0], 42, 'unnamed Tuple[0] = 42');
ok(is_int($r['unnamed'][0]), 'unnamed Tuple[0] is PHP int');
eq($r['unnamed'][1], 'hello', 'unnamed Tuple[1] = hello');

// Named tuple → associative array keyed by field name
ok(is_array($r['named']), 'named Tuple is PHP array');
eq($r['named']['id'], 7, 'named Tuple.id = 7');
eq($r['named']['label'], 'seven', 'named Tuple.label = seven');
eq($r['named']['ts'], '2024-01-15 08:30:00', 'named Tuple.ts DateTime formatted Y-m-d H:i:s');

// Nested tuple + array field inside a tuple
eq($r['nested']['inner']['x'], 1, 'nested Tuple.inner.x = 1');
eq($r['nested']['inner']['y'], 2, 'nested Tuple.inner.y = 2');
ok(is_array($r['nested']['tags']), 'nested Tuple.tags is PHP array');
eq($r['nested']['tags'][0], 'a', 'nested Tuple.tags[0] = a');
eq($r['nested']['tags'][1], 'b', 'nested Tuple.tags[1] = b');

// Nullable fields inside a tuple
eq($r['nul']['a'], 'present', 'Tuple Nullable(String) a = present');
ok($r['nul']['b'] === null, 'Tuple Nullable(Int64) b = NULL');

// Array(Tuple)
ok(is_array($r['arr_tup']), 'Array(Tuple) is PHP array');
eq(count($r['arr_tup']), 2, 'Array(Tuple) has 2 elements');
eq($r['arr_tup'][0]['k'], 'x', 'Array(Tuple)[0].k = x');
eq($r['arr_tup'][0]['v'], 1, 'Array(Tuple)[0].v = 1');
eq($r['arr_tup'][1]['k'], 'y', 'Array(Tuple)[1].k = y');
eq($r['arr_tup'][1]['v'], 2, 'Array(Tuple)[1].v = 2');

// Map(String, Tuple)
ok(is_array($r['map_tup']), 'Map(String, Tuple) is PHP array');
eq($r['map_tup']['range']['lo'], 10, 'Map(String, Tuple)[range].lo = 10');
eq($r['map_tup']['range']['hi'], 20, 'Map(String, Tuple)[range].hi = 20');

// Write path: unnamed tuple as a nested list, named tuple as an assoc array.
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_tuple_write");
clickhouse_exec("CREATE TABLE clickhousephp_tuple_write (
    u Tuple(UInt32, String),
    n Tuple(id UInt64, label String)
) ENGINE = Memory");
$w = clickhouse_insert('clickhousephp_tuple_write',
    [[[42, 'hello'], ['id' => 7, 'label' => 'seven']]],
    ['u', 'n']);
eq($w, 'Ok', 'insert Tuple columns returns Ok');
$wr = clickhouse_query_array("SELECT * FROM clickhousephp_tuple_write");
eq($wr[0]['u'][0], 42, 'written unnamed Tuple[0] round-trips');
eq($wr[0]['u'][1], 'hello', 'written unnamed Tuple[1] round-trips');
eq($wr[0]['n']['id'], 7, 'written named Tuple.id round-trips');
eq($wr[0]['n']['label'], 'seven', 'written named Tuple.label round-trips');
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_tuple_write");

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_tuple_test");

// =============================================================================
// Geo types (Point, Ring, LineString, Polygon, MultiPolygon, MultiLineString)
// =============================================================================

suite('Geo types');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_geo_test");
clickhouse_exec("CREATE TABLE clickhousephp_geo_test (
    pt    Point,
    rng   Ring,
    ls    LineString,
    poly  Polygon,
    mpoly MultiPolygon,
    mls   MultiLineString,
    pts   Array(Point),
    named Tuple(name String, loc Point)
) ENGINE = Memory");

clickhouse_exec("INSERT INTO clickhousephp_geo_test VALUES (
    (10, 20),
    [(0, 0), (1, 0), (1, 1), (0, 0)],
    [(0, 0), (3, 4)],
    [[(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)]],
    [[[(0, 0), (1, 0), (1, 1), (0, 0)]]],
    [[(0, 0), (1, 1)], [(2, 2), (3, 3)]],
    [(5, 6), (7, 8)],
    ('site-A', (100, 200))
)");

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_geo_test");
eq(count($rows), 1, 'geo test row count = 1');
$r = $rows[0];

// Point → [x, y] indexed array of floats
ok(is_array($r['pt']) && count($r['pt']) === 2, 'Point is a 2-element array');
eq($r['pt'][0], 10.0, 'Point x = 10.0');
eq($r['pt'][1], 20.0, 'Point y = 20.0');
ok(is_float($r['pt'][0]), 'Point coord is PHP float');

// Ring → array of points
eq(count($r['rng']), 4, 'Ring has 4 points');
eq($r['rng'][0], [0.0, 0.0], 'Ring[0] = [0,0]');
eq($r['rng'][2], [1.0, 1.0], 'Ring[2] = [1,1]');

// LineString → array of points
eq($r['ls'][1], [3.0, 4.0], 'LineString[1] = [3,4]');

// Polygon → array of rings (array of arrays of points)
eq(count($r['poly']), 1, 'Polygon has 1 ring');
eq(count($r['poly'][0]), 5, 'Polygon ring has 5 points');
eq($r['poly'][0][1], [2.0, 0.0], 'Polygon[0][1] = [2,0]');

// MultiPolygon → array of polygons
eq($r['mpoly'][0][0][1], [1.0, 0.0], 'MultiPolygon[0][0][1] = [1,0]');

// MultiLineString → array of linestrings
eq(count($r['mls']), 2, 'MultiLineString has 2 lines');
eq($r['mls'][1][0], [2.0, 2.0], 'MultiLineString[1][0] = [2,2]');

// Array(Point) → array of points
eq($r['pts'][1], [7.0, 8.0], 'Array(Point)[1] = [7,8]');

// Point nested inside a named Tuple
eq($r['named']['name'], 'site-A', 'Tuple field name = site-A');
eq($r['named']['loc'], [100.0, 200.0], 'Tuple Point field = [100,200]');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_geo_test");

// =============================================================================
// Variant and Dynamic types (each row resolves to its concrete value)
// =============================================================================

suite('Variant and Dynamic types');

$vdOpt = ['settings' => [
    'allow_experimental_variant_type' => 1,
    'allow_experimental_dynamic_type' => 1,
    'allow_suspicious_variant_types'  => 1,
]];

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_vd_test", null, $vdOpt);
clickhouse_exec("CREATE TABLE clickhousephp_vd_test (
    id UInt8,
    v  Variant(UInt64, String),
    d  Dynamic
) ENGINE = Memory", null, $vdOpt);
clickhouse_exec("INSERT INTO clickhousephp_vd_test VALUES
    (1, 42, 'hello'),
    (2, 'text', 3.14),
    (3, NULL, [1, 2, 3])", null, $vdOpt);

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_vd_test ORDER BY id", null, $vdOpt);
eq(count($rows), 3, 'variant/dynamic row count = 3');

// Variant resolves to the concrete scalar of whichever branch a row holds
eq($rows[0]['v'], 42, 'Variant UInt64 branch → int 42');
ok(is_int($rows[0]['v']), 'Variant int branch is PHP int');
eq($rows[1]['v'], 'text', 'Variant String branch → string');
ok($rows[2]['v'] === null, 'Variant NULL → null');

// Dynamic can hold a different type in every row, including a composite
eq($rows[0]['d'], 'hello', 'Dynamic holds a string');
ok(abs($rows[1]['d'] - 3.14) < 0.001, 'Dynamic holds a float');
ok(is_float($rows[1]['d']), 'Dynamic float is PHP float');
eq($rows[2]['d'], [1, 2, 3], 'Dynamic holds an array → PHP array');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_vd_test", null, $vdOpt);

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
// Parameter bindings (P1)
// =============================================================================

suite('Parameter bindings');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_bind_test");
clickhouse_exec("CREATE TABLE clickhousephp_bind_test (
    id UInt32, name String, score Float64
) ENGINE = Memory");
clickhouse_exec("INSERT INTO clickhousephp_bind_test VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.2), (3, 'Charlie', 91.0)");

// Named parameters with query_array
$rows = clickhouse_query_array(
    "SELECT * FROM clickhousephp_bind_test WHERE name = {name:String}",
    ['name' => 'Alice']
);
eq(count($rows), 1, 'named param: 1 row');
eq($rows[0]['name'], 'Alice', 'named param: correct row');

// Named parameters with exec
clickhouse_exec(
    "INSERT INTO clickhousephp_bind_test VALUES ({id:UInt32}, {name:String}, {score:Float64})",
    ['id' => 4, 'name' => 'Dave', 'score' => 88.0]
);
$rows = clickhouse_query_array("SELECT * FROM clickhousephp_bind_test WHERE id = 4");
eq(count($rows), 1, 'exec named param: inserted');
eq($rows[0]['name'], 'Dave', 'exec named param: correct value');

// Without params (backward compat)
$rows = clickhouse_query_array("SELECT * FROM clickhousephp_bind_test ORDER BY id");
eq(count($rows), 4, 'no params: backward compat works');

// Multiple named parameters
$rows = clickhouse_query_array(
    "SELECT * FROM clickhousephp_bind_test WHERE score > {min:Float64} AND score < {max:Float64} ORDER BY name",
    ['min' => 88.0, 'max' => 96.0]
);
eq(count($rows), 2, 'multiple named params: 2 rows');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_bind_test");

// =============================================================================
// clickhouse_insert — associative arrays (P2)
// =============================================================================

suite('Insert with associative arrays');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_assoc_test");
clickhouse_exec("CREATE TABLE clickhousephp_assoc_test (
    id UInt32, name String, score Float64
) ENGINE = Memory");

// Associative rows — columns inferred from keys
$r = clickhouse_insert('clickhousephp_assoc_test', [
    ['id' => 1, 'name' => 'Alice', 'score' => 95.5],
    ['id' => 2, 'name' => 'Bob', 'score' => 87.2],
]);
eq($r, 'Ok', 'assoc insert returns Ok');

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_assoc_test ORDER BY id");
eq(count($rows), 2, 'assoc insert: 2 rows');
eq($rows[0]['name'], 'Alice', 'assoc insert: row 0 name');
eq($rows[1]['name'], 'Bob', 'assoc insert: row 1 name');

// Associative rows with partial columns (only id + name)
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_assoc_test");
clickhouse_exec("CREATE TABLE clickhousephp_assoc_test (
    id UInt32, name String, score Float64 DEFAULT 0.0
) ENGINE = Memory");

$r = clickhouse_insert('clickhousephp_assoc_test', [
    ['id' => 1, 'name' => 'Alice'],
    ['id' => 2, 'name' => 'Bob'],
]);
eq($r, 'Ok', 'assoc partial insert returns Ok');

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_assoc_test ORDER BY id");
eq($rows[0]['name'], 'Alice', 'assoc partial: row 0 name');
ok(abs($rows[0]['score'] - 0.0) < 0.001, 'assoc partial: default score applied');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_assoc_test");

// Error: flat values without columns
$flatNoColThrew = false;
try {
    clickhouse_insert('clickhousephp_assoc_test', ['a', 1, 2.0]);
} catch (RuntimeException $e) {
    $flatNoColThrew = true;
}
ok($flatNoColThrew, 'flat values without columns throws');

// Error: nested sequential without columns
$nestedNoColThrew = false;
try {
    clickhouse_insert('clickhousephp_assoc_test', [['a', 1], ['b', 2]]);
} catch (RuntimeException $e) {
    $nestedNoColThrew = true;
}
ok($nestedNoColThrew, 'nested sequential without columns throws');

// =============================================================================
// Type-parse edge cases — LC(Nullable), FixedString(N), unsupported types
// =============================================================================

suite('Type-parse edge cases');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_edge_test");
clickhouse_exec("CREATE TABLE clickhousephp_edge_test (
    tag     LowCardinality(String),
    opt_tag LowCardinality(Nullable(String)),
    code    FixedString(3)
) ENGINE = Memory");
clickhouse_exec("INSERT INTO clickhousephp_edge_test VALUES ('prod', 'x', 'ABC'), ('dev', NULL, 'XYZ')");

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_edge_test ORDER BY tag");
eq(count($rows), 2, 'edge case rows = 2');
eq($rows[1]['tag'], 'prod', 'LowCardinality(String) value');
eq($rows[1]['opt_tag'], 'x', 'LowCardinality(Nullable(String)) value');
ok($rows[0]['opt_tag'] === null, 'LowCardinality(Nullable(String)) NULL');
eq($rows[1]['code'], 'ABC', 'FixedString(3) value');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_edge_test");

// Unsupported column type throws instead of returning an empty array.
// INTERVAL yields an IntervalDay column, which the extension does not map —
// no table or experimental setting needed.
$unsupportedThrew = false;
$unsupportedMsg = '';
try {
    clickhouse_query_array("SELECT INTERVAL 1 DAY AS ival");
} catch (RuntimeException $e) {
    $unsupportedThrew = true;
    $unsupportedMsg = $e->getMessage();
}
ok($unsupportedThrew, 'unsupported column type throws RuntimeException');
ok(str_contains($unsupportedMsg, 'unsupported type'), 'exception names the unsupported type', $unsupportedMsg);

// =============================================================================
// Map(K, V) and nested arrays
// =============================================================================

suite('Map and nested arrays');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_nested_test");
clickhouse_exec("CREATE TABLE clickhousephp_nested_test (
    labels     Map(String, String),
    counters   Map(String, UInt64),
    by_code    Map(UInt8, String),
    tags_by_ns Map(String, Array(String)),
    opt_vals   Map(String, Nullable(String)),
    matrix     Array(Array(UInt32)),
    names2d    Array(Array(String)),
    opt_flags  Array(Nullable(UInt64))
) ENGINE = Memory");
clickhouse_exec("INSERT INTO clickhousephp_nested_test VALUES (
    map('env', 'prod', 'region', 'eu'),
    map('hits', 42, 'big', 18446744073709551615),
    map(1, 'run', 2, 'idle'),
    map('a', ['x', 'y'], 'b', []),
    map('k1', 'v1', 'k2', NULL),
    [[1, 2], [3], []],
    [['a', 'b'], ['c']],
    [7, NULL, 9]
)");

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_nested_test");
eq(count($rows), 1, 'nested test row count');
$r = $rows[0];

// Map(String, String) → assoc array
eq($r['labels'], ['env' => 'prod', 'region' => 'eu'], 'Map(String, String) as assoc array');

// Map(String, UInt64) → int values, float above PHP_INT_MAX
eq($r['counters']['hits'], 42, 'Map value UInt64 as int');
ok(is_float($r['counters']['big']), 'Map value UInt64 > PHP_INT_MAX as float');

// Map(UInt8, String) → integer keys
eq($r['by_code'], [1 => 'run', 2 => 'idle'], 'Map(UInt8, String) with int keys');
ok(array_key_exists(1, $r['by_code']) && is_string($r['by_code'][1]), 'int key is a real PHP int key');

// Map(String, Array(String)) → nested arrays as values
eq($r['tags_by_ns'], ['a' => ['x', 'y'], 'b' => []], 'Map(String, Array(String))');

// Map(String, Nullable(String)) → null values preserved
eq($r['opt_vals'], ['k1' => 'v1', 'k2' => null], 'Map(String, Nullable(String)) keeps NULL');

// Array(Array(UInt32)) — previously unreadable
eq($r['matrix'], [[1, 2], [3], []], 'Array(Array(UInt32))');
eq($r['names2d'], [['a', 'b'], ['c']], 'Array(Array(String))');

// Array(Nullable(UInt64)) — previously a silently empty array
eq($r['opt_flags'], [7, null, 9], 'Array(Nullable(UInt64)) no longer silently empty');

// Empty map row
clickhouse_exec("INSERT INTO clickhousephp_nested_test VALUES (map(), map(), map(), map(), map(), [], [], [])");
$rows = clickhouse_query_array("SELECT labels, matrix FROM clickhousephp_nested_test ORDER BY length(labels) ASC LIMIT 1");
eq($rows[0]['labels'], [], 'empty Map is empty array');
eq($rows[0]['matrix'], [], 'empty nested array');

// Map through the streaming cursor
$cur = clickhouse_query_cursor("SELECT labels FROM clickhousephp_nested_test ORDER BY length(labels) DESC LIMIT 1");
$chunk = clickhouse_cursor_fetch($cur);
eq($chunk[0]['labels'], ['env' => 'prod', 'region' => 'eu'], 'Map through cursor');
clickhouse_cursor_close($cur);

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_nested_test");

// =============================================================================
// DSN timeout & identifier validation
// =============================================================================

suite('DSN timeout & identifier validation');

// Base DSN without any timeout param — the configured DSN may already
// carry one, and a duplicated param would shadow the test value.
$dsnBase = preg_replace('/\?timeout=[^&]*&/', '?', $dsn);
$dsnBase = preg_replace('/[?&]timeout=[^&]*/', '', $dsnBase);
$sep = str_contains($dsnBase, '?') ? '&' : '?';

// Invalid timeout value → connect fails without clobbering the pool
$badTimeoutThrew = false;
try {
    clickhouse_connect($dsnBase . $sep . 'timeout=banana');
} catch (RuntimeException $e) {
    $badTimeoutThrew = true;
}
ok($badTimeoutThrew, 'invalid timeout value throws');
$rows = clickhouse_query_array("SELECT 1 AS one");
eq($rows[0]['one'], 1, 'previous connection survives failed reconnect');

// Short timeout → a slow query is aborted by the context
$r = clickhouse_connect($dsnBase . $sep . 'timeout=200ms');
eq($r, 'Ok', 'connect with timeout param');
$timeoutThrew = false;
try {
    clickhouse_query_array("SELECT sleep(1)");
} catch (RuntimeException $e) {
    $timeoutThrew = true;
}
ok($timeoutThrew, 'slow query aborted by DSN timeout');

// Back to a connection without timeout
$r = clickhouse_connect($dsn);
eq($r, 'Ok', 'reconnect without timeout');

// Invalid identifiers are rejected before reaching SQL
$badTableThrew = false;
try {
    clickhouse_insert('events; DROP TABLE x', ['a'], ['col']);
} catch (RuntimeException $e) {
    $badTableThrew = true;
}
ok($badTableThrew, 'invalid table name throws');

$badColThrew = false;
try {
    clickhouse_insert('clickhousephp_events_test', ['a'], ['col) VALUES (1); --']);
} catch (RuntimeException $e) {
    $badColThrew = true;
}
ok($badColThrew, 'invalid column name throws');

// =============================================================================
// Streaming cursor — bounded-memory chunked reads
// =============================================================================

suite('Cursor streaming');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_cursor_test");
clickhouse_exec("CREATE TABLE clickhousephp_cursor_test (n UInt32, label String) ENGINE = Memory");
clickhouse_exec("INSERT INTO clickhousephp_cursor_test SELECT number, concat('row-', toString(number)) FROM numbers(25000)");

$cur = clickhouse_query_cursor("SELECT n, label FROM clickhousephp_cursor_test ORDER BY n");
ok(is_int($cur), 'query_cursor returns int handle');

$chunk1 = clickhouse_cursor_fetch($cur, 10000);
eq(count($chunk1), 10000, 'chunk 1 = 10000 rows');
eq($chunk1[0]['n'], 0, 'chunk 1 starts at row 0');
eq($chunk1[9999]['n'], 9999, 'chunk 1 ends at row 9999');
eq($chunk1[0]['label'], 'row-0', 'row format matches query_array');

$chunk2 = clickhouse_cursor_fetch($cur, 10000);
eq(count($chunk2), 10000, 'chunk 2 = 10000 rows');
eq($chunk2[0]['n'], 10000, 'chunk 2 continues at row 10000');

$chunk3 = clickhouse_cursor_fetch($cur, 10000);
eq(count($chunk3), 5000, 'chunk 3 = 5000 rows (tail)');
eq($chunk3[4999]['n'], 24999, 'last row is 24999');

$chunk4 = clickhouse_cursor_fetch($cur, 10000);
eq(count($chunk4), 0, 'fetch after exhaustion returns empty array');

eq(clickhouse_cursor_close($cur), 'Ok', 'cursor_close returns Ok');

$closedThrew = false;
try {
    clickhouse_cursor_fetch($cur);
} catch (RuntimeException $e) {
    $closedThrew = true;
}
ok($closedThrew, 'fetch after close throws');

$doubleCloseThrew = false;
try {
    clickhouse_cursor_close($cur);
} catch (RuntimeException $e) {
    $doubleCloseThrew = true;
}
ok($doubleCloseThrew, 'double close throws');

// Named parameter binding works through cursors too
$cur2 = clickhouse_query_cursor(
    "SELECT n FROM clickhousephp_cursor_test WHERE n < {lim:UInt32} ORDER BY n",
    ['lim' => 42]
);
$rows = clickhouse_cursor_fetch($cur2, 100);
eq(count($rows), 42, 'cursor with named params');
clickhouse_cursor_close($cur2);

// Two cursors interleaved on the same pool
$curA = clickhouse_query_cursor("SELECT n FROM clickhousephp_cursor_test ORDER BY n");
$curB = clickhouse_query_cursor("SELECT n FROM clickhousephp_cursor_test ORDER BY n DESC");
$a1 = clickhouse_cursor_fetch($curA, 5);
$b1 = clickhouse_cursor_fetch($curB, 5);
$a2 = clickhouse_cursor_fetch($curA, 5);
eq($a1[0]['n'], 0, 'cursor A chunk 1');
eq($b1[0]['n'], 24999, 'cursor B interleaved (DESC)');
eq($a2[0]['n'], 5, 'cursor A position preserved across B fetches');
clickhouse_cursor_close($curA);
clickhouse_cursor_close($curB);

// Mid-stream close releases the pooled connection cleanly
$curC = clickhouse_query_cursor("SELECT n FROM clickhousephp_cursor_test");
clickhouse_cursor_fetch($curC, 10);
eq(clickhouse_cursor_close($curC), 'Ok', 'mid-stream close returns Ok');
$rows = clickhouse_query_array("SELECT count() AS c FROM clickhousephp_cursor_test");
eq($rows[0]['c'], 25000, 'pool healthy after mid-stream close');

// Errors at open
$badCursorThrew = false;
try {
    clickhouse_query_cursor("SELECT * FROM nonexistent_cursor_table_xyz");
} catch (RuntimeException $e) {
    $badCursorThrew = true;
}
ok($badCursorThrew, 'bad query throws at cursor open');

// Default chunk size
$curD = clickhouse_query_cursor("SELECT n FROM clickhousephp_cursor_test ORDER BY n");
$rows = clickhouse_cursor_fetch($curD);
eq(count($rows), 10000, 'default max_rows is 10000');
clickhouse_cursor_close($curD);

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_cursor_test");

// =============================================================================
// Per-call options, ping, server version
// =============================================================================

suite('Options, ping, server version');

eq(clickhouse_ping(), 'Ok', 'ping returns Ok');

$ver = clickhouse_server_version();
ok(preg_match('/^\d+\.\d+\.\d+$/', $ver) === 1, 'server_version is X.Y.Z', $ver);

// query_id propagates to the server (queryID() echoes the current query's id)
$rows = clickhouse_query_array("SELECT queryID() AS qid", null, ['query_id' => 'franken-test-qid-123']);
eq($rows[0]['qid'], 'franken-test-qid-123', 'query_id propagated to server');

// Settings are applied — max_result_rows with overflow throw
$threw = false;
try {
    clickhouse_query_array("SELECT number FROM numbers(100)", null,
        ['settings' => ['max_result_rows' => 5, 'result_overflow_mode' => 'throw']]);
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'settings enforced: max_result_rows overflow throws');

$rows = clickhouse_query_array("SELECT number FROM numbers(10)", null, ['settings' => ['max_threads' => 1]]);
eq(count($rows), 10, 'query with settings runs normally');

// Per-call timeout overrides the DSN timeout
$threw = false;
try {
    clickhouse_query_array("SELECT sleep(1)", null, ['timeout' => '200ms']);
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'per-call timeout aborts a slow query');

// Unknown option key is rejected (typo protection)
$threw = false;
$msg = '';
try {
    clickhouse_query_array("SELECT 1", null, ['bogus' => 1]);
} catch (RuntimeException $e) {
    $threw = true;
    $msg = $e->getMessage();
}
ok($threw && str_contains($msg, 'unknown option'), 'unknown option throws', $msg);

// exec and insert accept options too
$r = clickhouse_exec("SELECT count() FROM numbers(10)", null, ['settings' => ['max_threads' => 1]]);
eq($r, 'Ok', 'exec with settings');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_opts_test");
clickhouse_exec("CREATE TABLE clickhousephp_opts_test (n UInt32) ENGINE = Memory");
$r = clickhouse_insert('clickhousephp_opts_test', [1, 2, 3], ['n'], ['query_id' => 'franken-insert-qid']);
eq($r, 'Ok', 'insert with options');
$rows = clickhouse_query_array("SELECT count() AS c FROM clickhousephp_opts_test");
eq($rows[0]['c'], 3, 'insert with options wrote rows');
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_opts_test");

// Cursor honours settings (overflow throws at open or first fetch)
$threw = false;
try {
    $cur = clickhouse_query_cursor("SELECT number FROM numbers(1000000)", null,
        ['settings' => ['max_result_rows' => 5, 'result_overflow_mode' => 'throw']]);
    clickhouse_cursor_fetch($cur);
    clickhouse_cursor_close($cur);
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'cursor honours per-call settings');

// =============================================================================
// Incremental batch — begin / append / flush / send / abort
// =============================================================================

suite('Incremental batch');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_batch_test");
clickhouse_exec("CREATE TABLE clickhousephp_batch_test (n UInt32, label String) ENGINE = Memory");

$b = clickhouse_batch_begin('clickhousephp_batch_test', ['n', 'label']);
ok(is_int($b), 'batch_begin returns int handle');
eq(clickhouse_batch_append($b, [[1, 'a'], [2, 'b']]), 'Ok', 'append nested rows');
eq(clickhouse_batch_append($b, [3, 'c', 4, 'd']), 'Ok', 'append flat values');
eq(clickhouse_batch_append($b, [['n' => 5, 'label' => 'e']]), 'Ok', 'append assoc row');
eq(clickhouse_batch_flush($b), 'Ok', 'flush mid-stream');
eq(clickhouse_batch_append($b, [[6, 'f']]), 'Ok', 'append after flush');
eq(clickhouse_batch_send($b), 'Ok', 'send');

$rows = clickhouse_query_array("SELECT count() AS c, sum(n) AS s FROM clickhousephp_batch_test");
eq($rows[0]['c'], 6, 'all chunks landed');
eq($rows[0]['s'], 21, 'values correct across formats');

$threw = false;
try {
    clickhouse_batch_append($b, [[7, 'g']]);
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'append after send throws');

// Abort without flush discards everything
clickhouse_exec("TRUNCATE TABLE clickhousephp_batch_test");
$b = clickhouse_batch_begin('clickhousephp_batch_test', ['n', 'label']);
clickhouse_batch_append($b, [[10, 'x'], [11, 'y']]);
eq(clickhouse_batch_abort($b), 'Ok', 'abort returns Ok');
$rows = clickhouse_query_array("SELECT count() AS c FROM clickhousephp_batch_test");
eq($rows[0]['c'], 0, 'abort without flush discards rows');
$rows = clickhouse_query_array("SELECT 1 AS one");
eq($rows[0]['one'], 1, 'pool healthy after abort');

// No columns at begin: nested rows in DDL order work; flat values don't
$b = clickhouse_batch_begin('clickhousephp_batch_test');
eq(clickhouse_batch_append($b, [[20, 'z']]), 'Ok', 'append without columns (DDL order)');
eq(clickhouse_batch_send($b), 'Ok', 'send without columns');
$rows = clickhouse_query_array("SELECT n, label FROM clickhousephp_batch_test");
eq($rows[0]['n'], 20, 'row landed');

$b = clickhouse_batch_begin('clickhousephp_batch_test');
$threw = false;
try {
    clickhouse_batch_append($b, [21, 'w']);
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'flat append without columns throws');
clickhouse_batch_abort($b);

$threw = false;
try {
    clickhouse_batch_send(999999);
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'unknown batch handle throws');

$threw = false;
try {
    clickhouse_batch_begin('bad; DROP x');
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'invalid table at batch_begin throws');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_batch_test");

// =============================================================================
// Async insert
// =============================================================================

suite('Async insert');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_async_test");
clickhouse_exec("CREATE TABLE clickhousephp_async_test (n UInt32) ENGINE = MergeTree() ORDER BY n");

eq(clickhouse_async_insert("INSERT INTO clickhousephp_async_test VALUES (1), (2)", true), 'Ok', 'async insert wait=true');
$rows = clickhouse_query_array("SELECT count() AS c FROM clickhousephp_async_test");
eq($rows[0]['c'], 2, 'rows durable after wait=true');

eq(clickhouse_async_insert("INSERT INTO clickhousephp_async_test VALUES ({v:UInt32})", true, ['v' => 3]), 'Ok', 'async insert with params');
$rows = clickhouse_query_array("SELECT count() AS c FROM clickhousephp_async_test");
eq($rows[0]['c'], 3, 'param row visible');

eq(clickhouse_async_insert("INSERT INTO clickhousephp_async_test VALUES (4)", false), 'Ok', 'async insert wait=false returns immediately');
clickhouse_exec("SYSTEM FLUSH ASYNC INSERT QUEUE");
$rows = clickhouse_query_array("SELECT count() AS c FROM clickhousephp_async_test");
eq($rows[0]['c'], 4, 'wait=false row visible after queue flush');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_async_test");

// =============================================================================
// DSN pool & transport parameters
// =============================================================================

suite('DSN pool & transport');

// Reuse the timeout-free base DSN computed in the timeout suite
$u = parse_url($dsnBase);
$hostPort = $u['host'] . ':' . ($u['port'] ?? 9000);
$auth = $u['user'] . (isset($u['pass']) && $u['pass'] !== '' ? ':' . $u['pass'] : '');
$db = $u['path'] ?? '';
$qs = isset($u['query']) ? '?' . $u['query'] : '';

// Multi-host address list + open strategy (same host twice: exercises the parser)
$multi = "clickhouse://$auth@$hostPort,$hostPort$db$qs&connection_open_strategy=round_robin";
eq(clickhouse_connect($multi), 'Ok', 'multi-host DSN with round_robin connects');
$rows = clickhouse_query_array("SELECT 1 AS one");
eq($rows[0]['one'], 1, 'query works on multi-host pool');

// Pool sizing and connection lifetime
$sized = "clickhouse://$auth@$hostPort$db$qs&max_open_conns=8&max_idle_conns=4&conn_max_lifetime=10m";
eq(clickhouse_connect($sized), 'Ok', 'pool sizing DSN connects');
$cur = clickhouse_query_cursor("SELECT number FROM numbers(100)");
$rows = clickhouse_query_array("SELECT 2 AS two"); // concurrent query while cursor open
eq($rows[0]['two'], 2, 'query alongside open cursor on sized pool');
clickhouse_cursor_close($cur);

// ZSTD compression round-trip
$zstd = "clickhouse://$auth@$hostPort$db$qs&compress=zstd";
eq(clickhouse_connect($zstd), 'Ok', 'compress=zstd connects');
$rows = clickhouse_query_array("SELECT repeat('x', 10000) AS blob, number FROM numbers(50)");
eq(count($rows), 50, 'zstd round-trip row count');
eq(strlen($rows[0]['blob']), 10000, 'zstd payload intact');

// Legacy compress=false still accepted
$nocomp = "clickhouse://$auth@$hostPort$db$qs&compress=false";
eq(clickhouse_connect($nocomp), 'Ok', 'compress=false connects');
$rows = clickhouse_query_array("SELECT 3 AS three");
eq($rows[0]['three'], 3, 'uncompressed round-trip');

// Bad extension TLS param fails cleanly without touching the live pool
$threw = false;
try {
    clickhouse_connect($dsnBase . $sep . 'ca_cert=/nonexistent/ca.pem');
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'missing ca_cert file throws');
$rows = clickhouse_query_array("SELECT 4 AS four");
eq($rows[0]['four'], 4, 'previous pool survives failed TLS connect');

// Back to the standard DSN for cleanup
eq(clickhouse_connect($dsn), 'Ok', 'reconnect with standard DSN');

// =============================================================================
// Multiple connections
// =============================================================================

suite('Multiple connections');

$c2 = clickhouse_open($dsn);
ok(is_int($c2), 'open returns int handle');
$rows = clickhouse_query_array("SELECT 42 AS v", null, ['connection' => $c2]);
eq($rows[0]['v'], 42, 'query on named connection');
eq(clickhouse_ping($c2), 'Ok', 'ping named connection');
$ver2 = clickhouse_server_version($c2);
ok(preg_match('/^\d+\.\d+\.\d+$/', $ver2) === 1, 'server_version on named connection');

$rows = clickhouse_query_array("SELECT 1 AS one");
eq($rows[0]['one'], 1, 'default connection unaffected');

$cur = clickhouse_query_cursor("SELECT number FROM numbers(5)", null, ['connection' => $c2]);
eq(count(clickhouse_cursor_fetch($cur)), 5, 'cursor on named connection');
clickhouse_cursor_close($cur);

// Batch on a named connection, visible from the default one
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_conn_test");
clickhouse_exec("CREATE TABLE clickhousephp_conn_test (n UInt32) ENGINE = Memory");
$b = clickhouse_batch_begin('clickhousephp_conn_test', ['n'], ['connection' => $c2]);
clickhouse_batch_append($b, [[7]]);
clickhouse_batch_send($b);
$rows = clickhouse_query_array("SELECT count() AS c FROM clickhousephp_conn_test");
eq($rows[0]['c'], 1, 'batch via named connection visible from default');
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_conn_test");

eq(clickhouse_close($c2), 'Ok', 'close named connection');
$threw = false;
try {
    clickhouse_query_array("SELECT 1", null, ['connection' => $c2]);
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'query on closed connection throws');
$threw = false;
try {
    clickhouse_close($c2);
} catch (RuntimeException $e) {
    $threw = true;
}
ok($threw, 'double close throws');

// =============================================================================
// ClickHouse error codes in exceptions
// =============================================================================

suite('Error codes');

$code = -1;
try {
    clickhouse_query_array("SELECT * FROM nonexistent_error_code_table");
} catch (RuntimeException $e) {
    $code = $e->getCode();
}
eq($code, 60, 'UNKNOWN_TABLE exposes ClickHouse code 60');

$code = -1;
try {
    clickhouse_exec("SELECT WHERE FROM syntax(");
} catch (RuntimeException $e) {
    $code = $e->getCode();
}
eq($code, 62, 'SYNTAX_ERROR exposes ClickHouse code 62');

$code = -1;
try {
    clickhouse_query_array("SELECT 1", null, ['bogus' => 1]);
} catch (RuntimeException $e) {
    $code = $e->getCode();
}
eq($code, 0, 'client-side errors keep code 0');

// =============================================================================
// Int128/256 and JSON types
// =============================================================================

suite('BigInt & JSON');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_big_test");
clickhouse_exec("CREATE TABLE clickhousephp_big_test (
    i128   Int128,
    u256   UInt256,
    ni     Nullable(Int128),
    arr128 Array(Int128),
    j      JSON
) ENGINE = Memory");
clickhouse_exec("INSERT INTO clickhousephp_big_test VALUES (
    170141183460469231731687303715884105727,
    115792089237316195423570985008687907853269984665640564039457584007913129639935,
    NULL,
    [1, -170141183460469231731687303715884105728],
    '{\"a\": 1, \"b\": {\"c\": \"x\"}, \"arr\": [1, 2], \"f\": 1.5}'
)");

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_big_test");
$r = $rows[0];
eq($r['i128'], '170141183460469231731687303715884105727', 'Int128 max as exact string');
eq($r['u256'], '115792089237316195423570985008687907853269984665640564039457584007913129639935', 'UInt256 max as exact string');
ok($r['ni'] === null, 'Nullable(Int128) NULL');
eq($r['arr128'], ['1', '-170141183460469231731687303715884105728'], 'Array(Int128) as strings');

ok(is_array($r['j']), 'JSON is a PHP array');
eq($r['j']['a'], 1, 'JSON int leaf');
eq($r['j']['b']['c'], 'x', 'JSON nested object');
eq($r['j']['arr'], [1, 2], 'JSON array leaf');
ok(abs($r['j']['f'] - 1.5) < 0.0001, 'JSON float leaf');

// JSON array of objects — leaves are unwrapped recursively
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_jsonarr_test");
clickhouse_exec("CREATE TABLE clickhousephp_jsonarr_test (j JSON) ENGINE = Memory");
clickhouse_exec("INSERT INTO clickhousephp_jsonarr_test VALUES (
    '{\"items\": [{\"id\": 1, \"name\": \"a\"}, {\"id\": 2, \"name\": \"b\"}], \"n\": 3}'
)");
$r = clickhouse_query_array("SELECT * FROM clickhousephp_jsonarr_test")[0];
ok(is_array($r['j']['items']), 'JSON array-of-objects is a PHP array');
eq(count($r['j']['items']), 2, 'JSON array-of-objects length');
eq($r['j']['items'][0]['name'], 'a', 'JSON array element object leaf');
eq($r['j']['items'][1]['id'], 2, 'JSON array element int leaf');
eq($r['j']['n'], 3, 'JSON scalar alongside array');
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_jsonarr_test");

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_big_test");

// =============================================================================
// Write path — Map and Array columns (round trip)
// =============================================================================

suite('Write path — Map and Array columns');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_wmap_test");
clickhouse_exec("CREATE TABLE clickhousephp_wmap_test (
    id       UInt32,
    labels   Map(String, String),
    counters Map(String, UInt64),
    tags     Array(String),
    matrix   Array(Array(UInt32))
) ENGINE = Memory");

// clickhouse_insert with a single assoc row: assoc-array cells become Maps,
// list cells become Arrays.
$r = clickhouse_insert('clickhousephp_wmap_test',
    ['id' => 1,
     'labels'   => ['env' => 'prod', 'region' => 'eu'],
     'counters' => ['hits' => 42],
     'tags'     => ['x', 'y'],
     'matrix'   => [[1, 2], [3]]],
    ['id', 'labels', 'counters', 'tags', 'matrix']);
eq($r, 'Ok', 'insert row with Map/Array cells');

// Batch path exercises the same phpValue conversion.
$b = clickhouse_batch_begin('clickhousephp_wmap_test',
    ['id', 'labels', 'counters', 'tags', 'matrix']);
clickhouse_batch_append($b, [[
    2,
    ['env' => 'dev'],
    ['hits' => 7],
    ['z'],
    [[9]],
]]);
eq(clickhouse_batch_send($b), 'Ok', 'batch send with Map/Array cells');

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_wmap_test ORDER BY id");
eq(count($rows), 2, 'both write paths landed');
eq($rows[0]['labels'], ['env' => 'prod', 'region' => 'eu'], 'inserted Map(String,String) round-trips');
eq($rows[0]['counters'], ['hits' => 42], 'inserted Map(String,UInt64) round-trips');
eq($rows[0]['tags'], ['x', 'y'], 'inserted Array(String) round-trips');
eq($rows[0]['matrix'], [[1, 2], [3]], 'inserted Array(Array(UInt32)) round-trips');
eq($rows[1]['labels'], ['env' => 'dev'], 'batch Map round-trips');
eq($rows[1]['matrix'], [[9]], 'batch nested Array round-trips');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_wmap_test");

// Nullable element types scan into pointer Go types (map[K]*V, []*V) — the
// write path must box present values and pass typed nils, not drop them.
clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_wnull_test");
clickhouse_exec("CREATE TABLE clickhousephp_wnull_test (
    id      UInt32,
    opt_lbl Map(String, Nullable(String)),
    opt_arr Array(Nullable(UInt32))
) ENGINE = Memory");
eq(clickhouse_insert('clickhousephp_wnull_test',
    [['id' => 1, 'opt_lbl' => ['a' => 'x', 'b' => null], 'opt_arr' => [1, null, 3]]],
    ['id', 'opt_lbl', 'opt_arr']), 'Ok', 'insert Nullable Map/Array cells');
$b = clickhouse_batch_begin('clickhousephp_wnull_test', ['id', 'opt_lbl', 'opt_arr']);
clickhouse_batch_append($b, [[2, ['k' => null], [null, 5]]]);
eq(clickhouse_batch_send($b), 'Ok', 'batch Nullable Map/Array cells');

$rows = clickhouse_query_array("SELECT * FROM clickhousephp_wnull_test ORDER BY id");
eq($rows[0]['opt_lbl'], ['a' => 'x', 'b' => null], 'Map(String,Nullable(String)) keeps value and null');
eq($rows[0]['opt_arr'], [1, null, 3], 'Array(Nullable(UInt32)) keeps values and null');
eq($rows[1]['opt_lbl'], ['k' => null], 'batch Map null value round-trips');
eq($rows[1]['opt_arr'], [null, 5], 'batch Array null element round-trips');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_wnull_test");

// =============================================================================
// Handle lifetime vs DSN timeout
// =============================================================================

suite('Handle lifetime vs DSN timeout');

// A short DSN timeout is meant for single queries — it must NOT kill a
// long-lived cursor or batch whose context spans many calls.
$r = clickhouse_connect($dsnBase . $sep . 'timeout=300ms');
eq($r, 'Ok', 'connect with 300ms DSN timeout');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_life_test");
clickhouse_exec("CREATE TABLE clickhousephp_life_test (n UInt32) ENGINE = Memory");
clickhouse_exec("INSERT INTO clickhousephp_life_test SELECT number FROM numbers(5000)");

$cur = clickhouse_query_cursor("SELECT n FROM clickhousephp_life_test ORDER BY n");
clickhouse_cursor_fetch($cur, 10);
usleep(400000); // 400ms — well past the 300ms DSN timeout
$survived = true;
try {
    $chunk = clickhouse_cursor_fetch($cur, 10);
    $survived = count($chunk) === 10;
} catch (RuntimeException $e) {
    $survived = false;
}
ok($survived, 'cursor outlives the DSN timeout between fetches');
clickhouse_cursor_close($cur);

// Same for a batch held open across the timeout window.
$b = clickhouse_batch_begin('clickhousephp_life_test', ['n']);
clickhouse_batch_append($b, [[90001]]);
usleep(400000);
$batchSurvived = true;
try {
    clickhouse_batch_append($b, [[90002]]);
    clickhouse_batch_send($b);
} catch (RuntimeException $e) {
    $batchSurvived = false;
}
ok($batchSurvived, 'batch outlives the DSN timeout between appends');
$rows = clickhouse_query_array("SELECT count() AS c FROM clickhousephp_life_test WHERE n >= 90001");
eq($rows[0]['c'], 2, 'batch rows landed after timeout window');

// But an explicit per-call timeout on the cursor still bounds a slow query.
$optThrew = false;
try {
    $curT = clickhouse_query_cursor("SELECT sleep(1)", null, ['timeout' => '200ms']);
    clickhouse_cursor_fetch($curT);
    clickhouse_cursor_close($curT);
} catch (RuntimeException $e) {
    $optThrew = true;
}
ok($optThrew, 'explicit per-call timeout still aborts a slow cursor');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_life_test");
clickhouse_connect($dsn); // restore the default connection

// =============================================================================
// clickhouse_stats — runtime observability
// =============================================================================

suite('clickhouse_stats');

$st = clickhouse_stats();
ok(is_array($st), 'stats returns a PHP array');
eq($st['connected'], 1, 'connected = 1 while connected');
ok(is_array($st['handles']) && is_array($st['pool']) && is_array($st['counters']),
    'stats has handles / pool / counters sections');
ok(is_array($st['timing'] ?? null) && $st['timing']['operations'] > 0 && $st['timing']['max_us'] >= 0,
    'stats.timing tracks operations and max latency');
ok(is_string($st['server_version']) && $st['server_version'] !== '', 'server_version is a non-empty string');
ok(is_int($st['uptime_seconds']) && $st['uptime_seconds'] >= 0, 'uptime_seconds is a non-negative int');
eq($st['handles']['idle_ttl_seconds'], 600, 'handles.idle_ttl_seconds = 600 (10 min)');
ok($st['pool']['max_open_conns'] >= 1, 'pool.max_open_conns is set');

// Counters increment monotonically.
$q0 = $st['counters']['queries'];
clickhouse_query_array("SELECT 1");
clickhouse_query_array("SELECT 2");
$st2 = clickhouse_stats();
eq($st2['counters']['queries'], $q0 + 2, 'queries counter increments by 2');

// A failed query bumps the error counter.
$e0 = $st2['counters']['errors'];
try { clickhouse_query_array("SELECT * FROM clickhousephp_no_such_table_stats"); } catch (RuntimeException $e) {}
$st3 = clickhouse_stats();
eq($st3['counters']['errors'], $e0 + 1, 'errors counter increments on failure');

// Open handles are reflected as a gauge and drop back after close.
$openCursor = clickhouse_query_cursor("SELECT number FROM system.numbers LIMIT 3");
$stOpen = clickhouse_stats();
eq($stOpen['handles']['cursors_open'], 1, 'cursors_open = 1 with a live cursor');
clickhouse_cursor_close($openCursor);
$stClosed = clickhouse_stats();
eq($stClosed['handles']['cursors_open'], 0, 'cursors_open = 0 after close');

// =============================================================================
// OO wrapper package (packages/oo) — thin facade over the native functions
// =============================================================================

suite('OO wrapper');

// The OO package lives outside web/; resolve it from the repo layout (local
// runs) or from /packages (the Docker image copies it there). Skip gracefully
// if it isn't deployed alongside.
$ooDir = null;
foreach ([__DIR__ . '/../packages/oo/src', '/packages/oo/src'] as $cand) {
    if (is_file("$cand/ClickHouse.php")) {
        $ooDir = $cand;
        break;
    }
}
if ($ooDir === null) {
    ok(true, 'OO package not deployed here — suite skipped');
} else {
    require "$ooDir/Cursor.php";
    require "$ooDir/Batch.php";
    require "$ooDir/ClickHouse.php";

    $ch = new \Jhondermarck\ClickHouse\ClickHouse(); // default connection already open
    eq($ch->query('SELECT 7 AS n')[0]['n'], 7, 'OO query()');
    eq($ch->columns('SELECT 7 AS n')['n'], [7], 'OO columns()');

    $ch->exec("DROP TABLE IF EXISTS clickhousephp_oo_test");
    $ch->exec("CREATE TABLE clickhousephp_oo_test (id UInt32) ENGINE = Memory");
    $oob = $ch->batch('clickhousephp_oo_test', ['id']);
    $oob->append([[1], [2], [3]]);
    eq($oob->send(), 'Ok', 'OO batch send()');

    $collected = [];
    $oocur = $ch->cursor("SELECT id FROM clickhousephp_oo_test ORDER BY id");
    foreach ($oocur->rows(2) as $row) {   // 2-row chunks over 3 rows
        $collected[] = $row['id'];
    }
    $oocur->close();
    eq($collected, [1, 2, 3], 'OO cursor rows() generator streams all rows');

    $metrics = \Jhondermarck\ClickHouse\ClickHouse::formatMetrics($ch->stats());
    ok(str_contains($metrics, 'clickhouse_queries_total'), 'formatMetrics emits a counter');
    ok(str_contains($metrics, 'clickhouse_build_info{server_version='), 'formatMetrics emits build_info');

    $ch->exec("DROP TABLE IF EXISTS clickhousephp_oo_test");
}

// =============================================================================
// Property-based round-trip: random values per type → insert → read back via
// query_array AND query_columns → assert equality. Seeded for reproducibility.
// =============================================================================

suite('Property-based round-trip');

$seed = (int) (getenv('ROUNDTRIP_SEED') ?: 20260712);
mt_srand($seed);
// DateTime is deliberately excluded here: round-tripping a naive datetime
// string through clickhouse_insert (client-side parse) depends on the server
// timezone, so it's environment-fragile — the fixed DateTime tests (SQL insert)
// cover formatting instead. This suite sticks to types with an exact round-trip.

$rtN = 100;
$uuidv4 = function (): string {
    $b = '';
    for ($i = 0; $i < 16; $i++) {
        $b .= chr(mt_rand(0, 255));
    }
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    $h = bin2hex($b);
    return sprintf('%s-%s-%s-%s-%s', substr($h, 0, 8), substr($h, 8, 4), substr($h, 12, 4), substr($h, 16, 4), substr($h, 20, 12));
};

// Build expected rows with generators whose values round-trip exactly.
$expected = [];
for ($i = 0; $i < $rtN; $i++) {
    $arrLen = mt_rand(0, 4); // includes empty arrays
    $arr = [];
    for ($j = 0; $j < $arrLen; $j++) {
        $arr[] = mt_rand(-1000000, 1000000);
    }
    $expected[$i] = [
        'n'   => $i,
        'u8'  => mt_rand(0, 255),
        'i32' => mt_rand(-2147483648, 2147483647),
        'i64' => mt_rand(-2000000000, 2000000000) * 1000 + mt_rand(0, 999),
        'f64' => (float) mt_rand(-1000000, 1000000),
        's'   => 'str-' . mt_rand(0, 999999) . ['', ' café', "\ttab", ' 日本'][mt_rand(0, 3)],
        'b'   => mt_rand(0, 1),
        'id'  => $uuidv4(),
        'opt' => mt_rand(0, 9) < 3 ? null : 'opt-' . mt_rand(0, 999999),
        'arr' => $arr,
    ];
}

$rtCols = ['n', 'u8', 'i32', 'i64', 'f64', 's', 'b', 'id', 'opt', 'arr'];
$rtRows = [];
foreach ($expected as $e) {
    $rtRows[] = [$e['n'], $e['u8'], $e['i32'], $e['i64'], $e['f64'], $e['s'],
        (bool) $e['b'], $e['id'], $e['opt'], $e['arr']];
}

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_rt_test");
clickhouse_exec("CREATE TABLE clickhousephp_rt_test (
    n UInt32, u8 UInt8, i32 Int32, i64 Int64, f64 Float64, s String,
    b Bool, id UUID, opt Nullable(String), arr Array(Int64)
) ENGINE = Memory");
$rtIns = clickhouse_insert('clickhousephp_rt_test', $rtRows, $rtCols);
eq($rtIns, 'Ok', "insert $rtN random rows (seed $seed)");

// Compare a read-back row set against expected; b is read as int (Bool → int).
$compare = function (array $got, string $mode) use ($expected, $rtN, $seed): void {
    $mismatch = null;
    for ($i = 0; $i < $rtN; $i++) {
        $exp = $expected[$i]; // b is already 0/1, matching Bool → int
        foreach ($exp as $col => $want) {
            if ($got[$i][$col] !== $want) {
                $mismatch = "row $i col $col: got " . var_export($got[$i][$col], true) . " want " . var_export($want, true);
                break 2;
            }
        }
    }
    ok($mismatch === null, "$mode round-trips all $rtN rows (seed $seed)", (string) $mismatch);
};

// query_array → list of assoc rows, ordered by n.
$rtArray = clickhouse_query_array("SELECT * FROM clickhousephp_rt_test ORDER BY n");
eq(count($rtArray), $rtN, "query_array returned $rtN rows");
$compare($rtArray, 'query_array');

// query_columns → transpose back to rows for the same comparison.
$rtColumns = clickhouse_query_columns("SELECT * FROM clickhousephp_rt_test ORDER BY n");
$asRows = [];
for ($i = 0; $i < $rtN; $i++) {
    $row = [];
    foreach ($rtCols as $c) {
        $row[$c] = $rtColumns[$c][$i];
    }
    $asRows[$i] = $row;
}
$compare($asRows, 'query_columns');

clickhouse_exec("DROP TABLE IF EXISTS clickhousephp_rt_test");

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
