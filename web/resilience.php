<?php
// =============================================================================
// Resilience test — the persistent connection pool must survive a ClickHouse
// restart WITHOUT an explicit reconnect, exactly as it would in a long-lived
// FrankenPHP worker. Disruptive (it bounces the server), so it is not part of
// `make test`; run it on its own with `make test_resilience`.
//
// Requires: the docker CLI on PATH and permission to restart the container.
// Override the container name with CH_CONTAINER if yours differs.
// =============================================================================

require __DIR__ . '/vendor/autoload.php';

if (class_exists('Dotenv\\Dotenv')) {
    Dotenv\Dotenv::createImmutable(__DIR__, '.env')->safeLoad();
}

$dsn       = $_ENV['CH_DSN'] ?? 'clickhouse://default@localhost:9000/default?secure=false';
$container = getenv('CH_CONTAINER') ?: 'franken-clickhouse-clickhouse-1';

$failed = 0;
function check(bool $cond, string $label): void {
    global $failed;
    echo ($cond ? "  \u{2713} " : "  \u{2717} ") . $label . "\n";
    if (!$cond) { $failed++; }
}

echo "── Resilience: persistent pool survives a ClickHouse restart\n";

clickhouse_connect($dsn);
$r = clickhouse_query_array("SELECT 1 AS n");
check(($r[0]['n'] ?? null) === 1, "query works before restart");

echo "  … restarting container '$container'\n";
$out = [];
$code = 0;
exec("docker restart " . escapeshellarg($container) . " 2>&1", $out, $code);
check($code === 0, "docker restart issued (exit $code)");

// The pooled sockets are now dead. A robust client redials transparently on
// the next use — we never call clickhouse_connect() again. Poll (bounded)
// until a query succeeds, tolerating transient errors while the server boots.
$deadline   = microtime(true) + 60;
$recovered  = false;
$attempts   = 0;
$lastErr    = '';
while (microtime(true) < $deadline) {
    $attempts++;
    try {
        $r = clickhouse_query_array("SELECT 42 AS n", null, ['timeout' => '3s']);
        if (($r[0]['n'] ?? null) === 42) { $recovered = true; break; }
    } catch (\Throwable $e) {
        $lastErr = $e->getMessage();
    }
    usleep(500_000);
}
check($recovered, "query recovers on the same pool after restart (in $attempts attempts)"
    . ($recovered ? "" : " — last error: $lastErr"));

check(is_string($v = clickhouse_server_version()) && $v !== '', "server_version works post-recovery ($v)");

$s = clickhouse_stats();
check(($s['connected'] ?? 0) === 1, "stats.connected == 1 after recovery");

clickhouse_disconnect();

echo $failed === 0 ? "\n\u{2705} resilience OK\n" : "\n\u{274C} $failed resilience check(s) failed\n";
exit($failed === 0 ? 0 : 1);
