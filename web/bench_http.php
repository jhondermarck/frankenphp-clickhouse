<?php
// ============================================================
// Bench HTTP — à lancer avec "make bench_worker"
// Nécessite que "make serve" tourne dans un autre terminal
// ============================================================

$base       = $_ENV['BENCH_URL'] ?? getenv('BENCH_URL') ?: 'http://localhost:8080';
$iterations = 20;
$warmup     = 3;

function http_get(string $url): string {
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => 30,
        CURLOPT_FORBID_REUSE   => false,
        CURLOPT_FRESH_CONNECT  => false,
    ]);
    $body = curl_exec($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    if ($code !== 200 || $body === false) {
        fwrite(STDERR, "HTTP error $code on $url\n");
        exit(1);
    }
    return $body;
}

function bench(string $label, string $url, int $warmup, int $n): void {
    for ($i = 0; $i < $warmup; $i++) { http_get($url); }

    $times = [];
    $rows  = 0;
    for ($i = 0; $i < $n; $i++) {
        $t0      = microtime(true);
        $body    = http_get($url);
        $times[] = microtime(true) - $t0;
        $rows    = json_decode($body, true)['rows'] ?? 0;
    }

    sort($times);
    $avg = array_sum($times) / count($times);
    $p50 = $times[(int)($n * 0.50)];
    $p95 = $times[(int)($n * 0.95)];

    printf(
        "%-50s  avg=%6.1fms  p50=%6.1fms  p95=%6.1fms  rows=%d\n",
        $label, $avg * 1000, $p50 * 1000, $p95 * 1000, $rows
    );
}

echo "FrankenPHP Worker Mode — benchmark HTTP ($iterations req, $warmup warmup)\n";
echo str_repeat('-', 95) . "\n";

bench('GET /query_array (générique, PHP array direct)', "$base/query_array", $warmup, $iterations);

echo str_repeat('-', 95) . "\n";
