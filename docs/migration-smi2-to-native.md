# Migration Guide: smi2/phpclickhouse → frankenphp-clickhouse native extension

> **Pour agents IA** : Ce document est la référence pour migrer un projet Laravel de `smi2/phpclickhouse` (client HTTP, port 8123) vers l'extension native Go compilée dans FrankenPHP (protocole natif TCP, port 9000).

## Vue d'ensemble

| | smi2/phpclickhouse | Extension native |
|---|---|---|
| Protocole | HTTP (port 8123) | Natif TCP (port 9000) |
| Sérialisation | JSON (`json_decode`) | Construction directe de `zend_array` en C |
| Perf SELECT 100k | ~0.34s (réf) | ~0.038s (**×8.5**) |
| Perf INSERT 100k | ~0.45s (réf) | ~0.144s (**×3.1**) |
| Streaming | non | curseur borné-mémoire (`+6 Mo` vs `+498 Mo` à 1M lignes) |
| Batch incrémental | non | `batch_begin/append/flush/send` |
| Binaire PHP | `php` | `frankenphp php-cli` |
| Dépendance | Composer (`smi2/phpclickhouse`) | Compilée dans le binaire FrankenPHP |

Chiffres de perf : voir le `README.md` (banc `web/bench.php`).

---

## API de l'extension (20 fonctions)

Toutes les fonctions lèvent `RuntimeException` en cas d'erreur ClickHouse. Le
code d'erreur serveur ClickHouse est disponible via `$e->getCode()` (0 pour les
erreurs côté client).

### Connexion

```php
clickhouse_connect(string $dsn): string          // pool par défaut (implicite)
clickhouse_disconnect(): string
clickhouse_open(string $dsn): int                 // pool nommé additionnel → handle
clickhouse_close(int $connection): string
clickhouse_ping(?int $connection = null): string
clickhouse_server_version(?int $connection = null): string
clickhouse_stats(): array                         // snapshot runtime (santé / fuites de handles)
```

### Lecture

```php
clickhouse_query_array(string $query, ?array $params = null, ?array $options = null): array
clickhouse_query_cursor(string $query, ?array $params = null, ?array $options = null): int
clickhouse_cursor_fetch(int $cursor, int $max_rows = 10000): array
clickhouse_cursor_close(int $cursor): string
```

### Écriture

```php
clickhouse_exec(string $query, ?array $params = null, ?array $options = null): string
clickhouse_insert(string $table, array $values, ?array $columns = null, ?array $options = null): string
clickhouse_async_insert(string $query, bool $wait = true, ?array $params = null, ?array $options = null): string

// Batch incrémental (mémoire bornée, connexion tenue le temps du batch)
clickhouse_batch_begin(string $table, ?array $columns = null, ?array $options = null): int
clickhouse_batch_append(int $batch, array $values): string
clickhouse_batch_flush(int $batch): string        // envoie les lignes accumulées, garde le batch ouvert
clickhouse_batch_send(int $batch): string         // flush final + ferme
clickhouse_batch_abort(int $batch): string        // jette tout
```

### Options par requête (`$options`)

Clé | Effet
----|------
`settings` | tableau assoc de settings ClickHouse (`['max_threads' => 1, 'max_result_rows' => 5]`)
`query_id` | identifiant de requête propagé au serveur
`timeout` | timeout de cette requête (`'200ms'`, `'30s'`) — surcharge celui du DSN
`connection` | handle de `clickhouse_open()` à utiliser au lieu du pool par défaut

Toute clé inconnue lève une exception (protection contre les fautes de frappe).

> **Handles et timeout DSN** : un `timeout` dans le DSN s'applique aux requêtes
> unitaires (`query_array`/`exec`/`insert`) mais **pas** aux curseurs ni aux
> batches — leur contexte couvre toute la durée de vie du handle. Pour borner un
> curseur, passez `['timeout' => '…']` explicitement à `query_cursor`.

---

## Correspondance des patterns

### 1. Connexion

**smi2 :**
```php
$client = new \ClickHouseDB\Client([
    'host'     => config('clickhouse.credentials.host'),
    'port'     => config('clickhouse.credentials.port'),     // 8123
    'username' => config('clickhouse.credentials.username'),
    'password' => config('clickhouse.credentials.password'),
    'https'    => config('clickhouse.credentials.https'),
]);
$client->database(config('clickhouse.options.database'));
$client->setTimeout(50);
$client->enableHttpCompression(true);
```

**Extension native :**
```php
$dsn = sprintf(
    'clickhouse://%s:%s@%s:%s/%s?secure=%s',
    $creds['username'], $creds['password'],
    $creds['host'], $creds['port'],          // 9000 (natif)
    $database, $https ? 'true' : 'false'
);
clickhouse_connect($dsn);
```

Le DSN accepte les paramètres du driver clickhouse-go (multi-hôtes,
`connection_open_strategy`, `max_open_conns`, `max_idle_conns`,
`conn_max_lifetime`, `compress=zstd`, …). Tout paramètre inconnu du driver est
transmis comme setting ClickHouse. Les paramètres `timeout`, `ca_cert`,
`client_cert`, `client_key` sont propres à l'extension (TLS mutuel).

> **IMPORTANT** : le port passe de `8123` (HTTP) à `9000` (natif). Compression
> LZ4 par défaut (`compress=zstd` pour zstd).

### 2. SELECT avec bindings

**smi2 :**
```php
$stmt = $client->select(
    'SELECT * FROM events WHERE tenant_id = :tenantId AND id = :id',
    ['tenantId' => $tenantId, 'id' => $eventId]
);
$rows  = $stmt->rows();
$value = $stmt->fetchOne('column_name');
```

**Extension native :**
```php
// Syntaxe ClickHouse native {param:Type}
$rows = clickhouse_query_array(
    'SELECT * FROM events WHERE tenant_id = {tenantId:String} AND id = {id:String}',
    ['tenantId' => $tenantId, 'id' => $eventId]
);
$value = $rows[0]['column_name'] ?? null;    // équivalent fetchOne('column_name')
$count = count($rows);
```

**Différences des bindings :**

| smi2 | Extension native | Notes |
|------|-----------------|-------|
| `:paramName` | `{paramName:Type}` | le type ClickHouse est requis |
| `IN (:values)` | `IN {values:Array(String)}` | **supporté** : passer un tableau PHP en paramètre |

Types courants : `String`, `UInt32/UInt64/Int32/Int64`, `Float64`,
`DateTime`, `DateTime64`, `Date`, `UUID`, `Array(String)`, …

### 3. Gros SELECT → curseur streaming

Pour les résultats volumineux, `clickhouse_query_array` matérialise tout en
mémoire. Le curseur lit par paquets bornés :

```php
$cur = clickhouse_query_cursor("SELECT * FROM big_table", $params, $options);
while (($chunk = clickhouse_cursor_fetch($cur, 10000)) && count($chunk) > 0) {
    foreach ($chunk as $row) { /* … */ }
}
clickhouse_cursor_close($cur);   // TOUJOURS fermer (voir « Points d'attention »)
```

### 4. INSERT

**smi2 :**
```php
$client->insert('events', [
    [$id, $tenantId, $start],
    [$id2, $tenantId2, $start2],
], ['id', 'tenant_id', 'start']);
```

**Extension native — 3 formats supportés :**

```php
// Format 1 : rows imbriquées (comme smi2) ✅ RECOMMANDÉ pour migration
clickhouse_insert('events', [
    [$id, $tenantId, $start],
    [$id2, $tenantId2, $start2],
], ['id', 'tenant_id', 'start']);

// Format 2 : tableaux associatifs (colonnes inférées des clés)
clickhouse_insert('events', [
    ['id' => $id, 'tenant_id' => $tenantId, 'start' => $start],
]);

// Format 3 : flat array (valeurs à plat, colonnes obligatoires)
clickhouse_insert('events', [$id, $tenantId, $start], ['id', 'tenant_id', 'start']);
```

Les colonnes `Map(K,V)` acceptent des tableaux associatifs PHP, et les colonnes
`Array(T)` (y compris imbriquées) des listes PHP :

```php
clickhouse_insert('events', [[
    'id'     => $id,
    'labels' => ['env' => 'prod', 'region' => 'eu'],   // Map(String, String)
    'tags'   => ['a', 'b'],                             // Array(String)
]], ['id', 'labels', 'tags']);
```

> **Colonnes partielles** : les colonnes non fournies prennent leur DEFAULT.

### 5. Batch incrémental (remplace les gros `insert()` en mémoire)

Là où `clickhouse_insert` exige tout le payload en un seul tableau PHP, le batch
tient la connexion et accumule par paquets — mémoire bornée pour des millions de
lignes :

```php
$b = clickhouse_batch_begin('events', ['id', 'tenant_id', 'start']);
foreach ($chunks as $rows) {
    clickhouse_batch_append($b, $rows);   // mêmes formats que insert
    clickhouse_batch_flush($b);           // optionnel : envoie et garde ouvert
}
clickhouse_batch_send($b);                // flush final + ferme
// en cas d'erreur applicative : clickhouse_batch_abort($b);
```

### 6. Async insert (petites écritures haute fréquence)

```php
clickhouse_async_insert(
    "INSERT INTO events VALUES ({id:UUID}, {t:DateTime})",
    true,                                  // wait_for_async_insert
    ['id' => $id, 't' => $now]
);
```

### 7. WRITE / EXEC (DDL, TRUNCATE, DELETE)

**smi2 :**
```php
$client->write('TRUNCATE TABLE IF EXISTS ' . $tableName);
$client->write('DELETE FROM events WHERE id = :id', ['id' => $id]);
```

**Extension native :**
```php
clickhouse_exec('TRUNCATE TABLE IF EXISTS ' . $tableName);
clickhouse_exec('DELETE FROM events WHERE id = {id:String}', ['id' => $id]);
```

> smi2 substitue les noms de tables (`{table}`). L'extension ne substitue pas les
> identifiants — concaténer en PHP (constantes côté code, pas d'injection).

### 8. Résultats

`smi2` retourne un objet `Statement` ; l'extension retourne directement un
`array` de rows associatifs :

```php
$rows = clickhouse_query_array('SELECT ...');
count($rows);                // $stmt->count()
$rows[0]['col'] ?? null;     // $stmt->fetchOne('col')
```

### 9. Gestion d'erreurs

```php
use ClickHouseDB\Exception\DatabaseException;   // smi2

try {
    $rows = clickhouse_query_array('...');
} catch (RuntimeException $e) {                 // extension native
    $e->getMessage();   // "Table default.events doesn't exist"
    $e->getCode();      // code serveur ClickHouse (60, 62, …) ; 0 = erreur client
}
```

> Remplacer `DatabaseException` par `RuntimeException` partout. `getCode()` porte
> désormais le code d'erreur ClickHouse.

### 10. Format des dates

Identique à smi2 — pas de changement de parsing :
```
2024-01-15 08:00:00           // DateTime
2024-01-15 08:00:00.123456    // DateTime64(6)
```

### 11. Plusieurs bases / clusters

`clickhouse_connect` établit le pool par défaut (utilisé implicitement). Pour un
second cluster, `clickhouse_open` renvoie un handle passé via `['connection' => $h]` :

```php
$analytics = clickhouse_open($analyticsDsn);
$rows = clickhouse_query_array('SELECT …', null, ['connection' => $analytics]);
clickhouse_close($analytics);
```

---

## Plan de migration pour le wrapper Laravel

### Architecture cible

```
app/Services/Clickhouse/
├── ClickhouseClientInterface.php     # interface commune (nouveau)
├── ClickhouseClientService.php       # implémentation smi2 (gardée pour fallback)
└── NativeClickhouseClientService.php # implémentation native (nouveau)
```

### Interface commune

```php
<?php

namespace App\Services\Clickhouse;

interface ClickhouseClientInterface
{
    public static function select(string $sql, array $bindings = []): array;
    public static function insert(string $table, array $values, array $columns = []): void;
    public static function write(string $sql, array $bindings = []): void;
    public static function truncate(string $table): void;
}
```

### Implémentation native

```php
<?php

namespace App\Services\Clickhouse;

class NativeClickhouseClientService implements ClickhouseClientInterface
{
    private static bool $connected = false;

    public function __construct(private array $config)
    {
        $this->connect();
    }

    private function connect(): void
    {
        if (self::$connected) {
            return;
        }

        $creds = $this->config['credentials'];
        $dsn = sprintf(
            'clickhouse://%s:%s@%s:%s/%s?secure=%s',
            $creds['username'],
            $creds['password'],
            $creds['host'],
            $creds['port'],
            $this->config['options']['database'],
            ($creds['https'] ?? false) ? 'true' : 'false'
        );

        clickhouse_connect($dsn);
        self::$connected = true;
    }

    /**
     * SELECT avec conversion automatique des bindings smi2 → natif.
     */
    public static function select(string $sql, array $bindings = []): array
    {
        if (empty($bindings)) {
            return clickhouse_query_array($sql);
        }

        [$nativeSql, $nativeBindings] = self::convertBindings($sql, $bindings);

        return clickhouse_query_array($nativeSql, $nativeBindings);
    }

    public static function insert(string $table, array $values, array $columns = []): void
    {
        if (empty($values)) {
            return;
        }

        empty($columns)
            ? clickhouse_insert($table, $values)
            : clickhouse_insert($table, $values, $columns);
    }

    public static function write(string $sql, array $bindings = []): void
    {
        if (empty($bindings)) {
            clickhouse_exec($sql);
            return;
        }

        // Les bindings smi2 de write() incluent souvent des noms de tables
        // qu'on ne peut pas paramétrer avec le protocole natif → substitution.
        clickhouse_exec(self::resolveIdentifierBindings($sql, $bindings));
    }

    public static function truncate(string $table): void
    {
        clickhouse_exec('TRUNCATE TABLE IF EXISTS ' . $table);
    }

    // ── Helpers de conversion ────────────────────────────────────────────

    /**
     * Convertit les bindings smi2 (:param) vers le format natif ({param:Type}).
     *
     * - string  → String        - null  → 'NULL' inliné
     * - int     → Int64          - array → {param:Array(String)} (IN supporté)
     * - float   → Float64
     * - bool    → UInt8
     */
    private static function convertBindings(string $sql, array $bindings): array
    {
        $nativeBindings = [];

        foreach ($bindings as $key => $value) {
            if ($value === null) {
                $sql = str_replace(':' . $key, 'NULL', $sql);
                continue;
            }

            if (is_array($value)) {
                // IN (:values) → IN {values:Array(String)} (binding natif)
                $sql = str_replace(':' . $key, '{' . $key . ':Array(String)}', $sql);
                $nativeBindings[$key] = array_map('strval', $value);
                continue;
            }

            $chType = match (true) {
                is_int($value)   => 'Int64',
                is_float($value) => 'Float64',
                is_bool($value)  => 'UInt8',
                default          => 'String',
            };

            $sql = str_replace(':' . $key, '{' . $key . ':' . $chType . '}', $sql);
            $nativeBindings[$key] = $value;
        }

        return [$sql, $nativeBindings];
    }

    /**
     * Substitution directe des identifiants ({table}) pour write().
     */
    private static function resolveIdentifierBindings(string $sql, array $bindings): string
    {
        foreach ($bindings as $key => $value) {
            $sql = str_replace('{' . $key . '}', (string) $value, $sql);
        }

        return $sql;
    }
}
```

### ServiceProvider

```php
$this->app->singleton('clickhouse', function () {
    if (function_exists('clickhouse_connect')) {
        return new NativeClickhouseClientService(config('clickhouse'));
    }
    return new ClickhouseClientService(config('clickhouse'));   // fallback smi2
});
```

### Config

```php
// config/clickhouse.php
'credentials' => [
    'host'     => env('CLICKHOUSE_HOST', 'clickhouse'),
    'port'     => env('CLICKHOUSE_NATIVE_PORT', '9000'),  // port natif
    'username' => env('CLICKHOUSE_USER', 'default'),
    'password' => env('CLICKHOUSE_PASSWORD', ''),
    'https'    => env('CLICKHOUSE_HTTPS', false),
],
```

---

## Checklist de migration

- [ ] Ajouter `CLICKHOUSE_NATIVE_PORT=9000` dans `.env`
- [ ] Créer `ClickhouseClientInterface.php`
- [ ] Créer `NativeClickhouseClientService.php` avec conversion de bindings
- [ ] Mettre à jour `ClickhouseServiceProvider` pour détecter l'extension
- [ ] Remplacer `DatabaseException` par `RuntimeException` dans les catches ; exploiter `getCode()`
- [ ] Remplacer `$stmt->rows()` / `$stmt->fetchOne('col')` par l'accès direct au tableau
- [ ] Basculer les gros SELECT sur `clickhouse_query_cursor` (mémoire bornée)
- [ ] Basculer les gros INSERT sur le batch incrémental
- [ ] Adapter `Dockerfile`/`Makefile` pour `frankenphp php-cli` au lieu de `php`
- [ ] Vérifier que ClickHouse expose le port 9000 dans `docker-compose.yml`
- [ ] Lancer la suite de tests

## Points d'attention

1. **`frankenphp php-cli`** : les fonctions de l'extension ne sont disponibles
   que via le binaire `frankenphp`, pas via `php`. Tous les scripts (`artisan`,
   cron, workers) doivent être lancés avec `frankenphp php-cli`.

2. **Toujours fermer les handles** : `clickhouse_cursor_close` /
   `clickhouse_batch_send`/`_abort` doivent être appelés (idéalement dans un
   `finally`). Un handle abandonné retient une socket du pool. Un ramasse-miettes
   interne récupère les handles inactifs depuis plus de 10 minutes, mais ne
   comptez pas dessus en régime nominal — en mode worker FrankenPHP le process ne
   redémarre pas entre les requêtes.

3. **`IN` avec tableaux** : supporté nativement via `{param:Array(T)}` — passer un
   tableau PHP en paramètre (voir `convertBindings`).

4. **Noms de tables en binding** : l'extension ne substitue pas les identifiants
   — utiliser la concaténation PHP (`resolveIdentifierBindings`).

5. **Pool de connexions** : `clickhouse.Conn` est lui-même un pool thread-safe. Un
   seul `clickhouse_connect()` au démarrage suffit ; dimensionner via le DSN
   (`max_open_conns`, `max_idle_conns`, `conn_max_lifetime`).

6. **Readonly** : pas de mode `readonly` dédié — créer un utilisateur ClickHouse
   aux permissions limitées, ou passer `['settings' => ['readonly' => 1]]`.
