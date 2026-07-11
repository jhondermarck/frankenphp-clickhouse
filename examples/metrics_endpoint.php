<?php
// Expose the runtime snapshot as a Prometheus /metrics endpoint.
//
// This CLI demo connects, runs a little traffic, and prints the exposition
// text. To wire it into a FrankenPHP worker, add a route to worker.php:
//
//   if (parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH) === '/metrics') {
//       header('Content-Type: text/plain; version=0.0.4');
//       echo \Jhondermarck\ClickHouse\ClickHouse::formatMetrics(clickhouse_stats());
//       return;
//   }
//
// Run:
//   CH_DSN='clickhouse://default:pass@localhost:9000/db' \
//     ./frankenphp-clickhouse php-cli examples/metrics_endpoint.php

require __DIR__ . '/../packages/oo/src/ClickHouse.php';

use Jhondermarck\ClickHouse\ClickHouse;

$dsn = getenv('CH_DSN') ?: ($argv[1] ?? 'clickhouse://default@localhost:9000/default?secure=false');

clickhouse_connect($dsn);

// A little traffic so the counters are non-zero in the output.
clickhouse_query_array('SELECT 1');
clickhouse_query_array('SELECT 2');

// The exact body a /metrics handler would return.
echo ClickHouse::formatMetrics(clickhouse_stats());

clickhouse_disconnect();
