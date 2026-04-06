<?php
// ============================================================
// FrankenPHP Worker — code exécuté UNE SEULE FOIS au démarrage
// ============================================================
require __DIR__ . '/vendor/autoload.php';
ini_set('memory_limit', '1G');

if (class_exists('Dotenv\\Dotenv')) {
    Dotenv\Dotenv::createImmutable(__DIR__, '.env')->safeLoad();
}

$dsn   = $_ENV['CH_DSN']   ?? 'clickhouse://default@localhost:9000/default?secure=false';
$query = $_ENV['CH_QUERY'] ?? '';

// Connexion avec retry — ClickHouse peut ne pas être prêt au boot
$maxRetries = 5;
$retryDelay = 2;
$connected  = false;

for ($i = 1; $i <= $maxRetries; $i++) {
    try {
        clickhouse_connect($dsn);
        $connected = true;
        fwrite(STDERR, "[worker] ClickHouse connected\n");
        break;
    } catch (RuntimeException $e) {
        fwrite(STDERR, "[worker] ClickHouse connection attempt $i/$maxRetries failed: " . $e->getMessage() . "\n");
        if ($i < $maxRetries) {
            sleep($retryDelay);
        }
    }
}

if (!$connected) {
    fwrite(STDERR, "[worker] WARNING: running without ClickHouse connection\n");
}

// ============================================================
// Boucle de traitement des requêtes HTTP
// ============================================================
while (frankenphp_handle_request(function () use ($query): void {
    $uri = parse_url($_SERVER['REQUEST_URI'] ?? '/', PHP_URL_PATH);

    header('Content-Type: application/json');

    match ($uri) {
        '/query_array' => (function () use ($query): void {
            $rows = clickhouse_query_array($query);
            echo json_encode(['rows' => count($rows)]);
            unset($rows);
        })(),

        default => (function (): void {
            http_response_code(404);
            echo json_encode(['error' => 'unknown endpoint']);
        })(),
    };
})) {
    gc_collect_cycles();
}

// Appelé uniquement si le worker s'arrête proprement
clickhouse_disconnect();
