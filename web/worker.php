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

// Connexion unique — persiste pour toute la vie du worker
clickhouse_connect($dsn);

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
