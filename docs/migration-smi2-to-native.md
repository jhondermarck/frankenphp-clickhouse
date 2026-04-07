# Migration Guide: smi2/phpclickhouse -> frankenphp-clickhouse native extension

> **Pour agents IA** : Ce document est la reference pour migrer un projet Laravel de `smi2/phpclickhouse` (client HTTP, port 8123) vers l'extension native Go compilee dans FrankenPHP (protocole natif, port 9000).

## Vue d'ensemble

| | smi2/phpclickhouse | Extension native |
|---|---|---|
| Protocole | HTTP (port 8123) | Natif TCP (port 9000) |
| Serialisation | JSON (`json_decode`) | Construction directe de zend_array en C |
| Perf SELECT 100k | ~0.394s | ~0.046s (**x8.5**) |
| Perf INSERT 100k | ~0.496s | ~0.172s (**x2.9**) |
| Binaire PHP | `php` | `frankenphp php-cli` |
| Dependance | Composer (`smi2/phpclickhouse`) | Compilee dans le binaire FrankenPHP |

## API de l'extension

```php
// Connexion (DSN natif, port 9000)
clickhouse_connect(string $dsn): string

// Requetes SELECT -> tableau PHP natif
clickhouse_query_array(string $query, ?array $params = null): array

// DDL/DML sans resultat
clickhouse_exec(string $query, ?array $params = null): string

// INSERT batch performant
clickhouse_insert(string $table, array $values, ?array $columns = null): string

// Deconnexion
clickhouse_disconnect(): string
```

Toutes les fonctions lancent `RuntimeException` en cas d'erreur.

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
$client->setConnectTimeOut(5);
$client->enableHttpCompression(true);
```

**Extension native :**
```php
$dsn = sprintf(
    'clickhouse://%s:%s@%s:%s/%s?secure=%s',
    config('clickhouse.credentials.username'),
    config('clickhouse.credentials.password'),
    config('clickhouse.credentials.host'),
    config('clickhouse.credentials.port'),         // 9000 (natif)
    config('clickhouse.options.database'),
    config('clickhouse.credentials.https') ? 'true' : 'false'
);
clickhouse_connect($dsn);
```

> **IMPORTANT** : Le port change de `8123` (HTTP) a `9000` (natif). En Docker, le service ClickHouse expose deja les deux ports. Il faut ajouter/modifier la config pour utiliser le port natif.

### 2. SELECT avec bindings

**smi2 :**
```php
// smi2 utilise la syntaxe :paramName
$stmt = $client->select(
    'SELECT * FROM events WHERE tenant_id = :tenantId AND id = :id',
    ['tenantId' => $tenantId, 'id' => $eventId]
);
$rows = $stmt->rows();
$value = $stmt->fetchOne('column_name');
$count = $stmt->fetchOne('count');
```

**Extension native :**
```php
// L'extension utilise la syntaxe ClickHouse native {param:Type}
$rows = clickhouse_query_array(
    'SELECT * FROM events WHERE tenant_id = {tenantId:String} AND id = {id:String}',
    ['tenantId' => $tenantId, 'id' => $eventId]
);
// $rows est directement un array de rows associatifs
$firstRow = $rows[0] ?? null;          // equivalent de fetchOne() sur la row
$value = $rows[0]['column_name'];      // equivalent de fetchOne('column_name')
$count = $rows[0]['count'];            // equivalent de fetchOne('count')
```

**Differences critiques des bindings :**

| smi2 | Extension native | Notes |
|------|-----------------|-------|
| `:paramName` | `{paramName:Type}` | Le type ClickHouse est requis |
| `:tenantId` | `{tenantId:String}` | |
| `:count` | `{count:UInt64}` | |
| `:startDate` | `{startDate:DateTime}` | |
| `IN (:values)` | Non supporte directement | Construire la clause IN manuellement |

**Types ClickHouse courants pour les bindings :**
- `String` — pour les strings et UUIDs
- `UInt32`, `UInt64`, `Int32`, `Int64` — pour les entiers
- `Float64` — pour les decimaux
- `DateTime`, `DateTime64` — pour les dates
- `Date` — pour les dates sans heure

### 3. INSERT

**smi2 :**
```php
// smi2 attend des rows imbriquees + colonnes separees
$client->insert('events', [
    [$id, $tenantId, $start, $end, $machineId],
    [$id2, $tenantId2, $start2, $end2, $machineId2],
], ['id', 'tenant_id', 'start', 'end', 'machine_id']);
```

**Extension native — 3 formats supportes :**

```php
// Format 1 : Rows imbriquees (comme smi2) ✅ RECOMMANDE pour migration
clickhouse_insert('events', [
    [$id, $tenantId, $start, $end, $machineId],
    [$id2, $tenantId2, $start2, $end2, $machineId2],
], ['id', 'tenant_id', 'start', 'end', 'machine_id']);

// Format 2 : Tableaux associatifs (colonnes inferees des cles)
clickhouse_insert('events', [
    ['id' => $id, 'tenant_id' => $tenantId, 'start' => $start],
    ['id' => $id2, 'tenant_id' => $tenantId2, 'start' => $start2],
]);

// Format 3 : Flat array (valeurs a plat)
clickhouse_insert('events',
    [$id, $tenantId, $start, $id2, $tenantId2, $start2],
    ['id', 'tenant_id', 'start']
);
```

> **Colonnes partielles** : L'extension supporte les inserts avec un sous-ensemble de colonnes. Les colonnes manquantes utilisent leur valeur DEFAULT.

### 4. WRITE / EXEC (DDL, TRUNCATE, DELETE)

**smi2 :**
```php
$client->write('TRUNCATE TABLE IF EXISTS {table}', ['table' => $tableName]);
$client->write('CREATE TABLE IF NOT EXISTS ...');
$client->write('DELETE FROM events WHERE id = :id', ['id' => $id]);
```

**Extension native :**
```php
// Sans bindings
clickhouse_exec('TRUNCATE TABLE IF EXISTS ' . $tableName);
clickhouse_exec('CREATE TABLE IF NOT EXISTS ...');

// Avec bindings (syntaxe ClickHouse native)
clickhouse_exec(
    'DELETE FROM events WHERE id = {id:String}',
    ['id' => $id]
);
```

> **Note** : smi2 supporte les bindings de noms de tables (`{table}`). L'extension native ne fait pas de substitution — utiliser la concatenation PHP pour les noms de tables (pas de risque d'injection car ce sont des constantes cote code).

### 5. Resultats

**smi2 retourne un objet `Statement` :**
```php
$stmt = $client->select('SELECT ...');
$stmt->rows();              // array de rows associatifs
$stmt->fetchOne('col');     // valeur d'une colonne de la 1ere row
$stmt->fetchOne();          // 1ere valeur de la 1ere row
$stmt->count();             // nombre de rows
```

**L'extension retourne directement un `array` :**
```php
$rows = clickhouse_query_array('SELECT ...');
// $rows = [['col1' => val, 'col2' => val, ...], ...]

count($rows);                        // nombre de rows
$rows[0]['col'] ?? null;             // equivalent fetchOne('col')
$rows[0][array_key_first($rows[0])]; // equivalent fetchOne() sans arg
```

### 6. Gestion d'erreurs

**smi2 :**
```php
use ClickHouseDB\Exception\DatabaseException;

try {
    $stmt = $client->select('...');
} catch (DatabaseException $e) {
    // Erreur ClickHouse
}
```

**Extension native :**
```php
try {
    $rows = clickhouse_query_array('...');
} catch (RuntimeException $e) {
    // $e->getMessage() contient le detail de l'erreur ClickHouse
    // Ex: "code: 60, message: Table default.events doesn't exist"
}
```

> Remplacer `DatabaseException` par `RuntimeException` partout.

### 7. Format des dates

**smi2 (HTTP)** retourne les dates au format ClickHouse standard :
```
2024-01-15 08:00:00
2024-01-15 08:00:00.123456
```

**L'extension native** retourne le meme format :
```
2024-01-15 08:00:00           // DateTime
2024-01-15 08:00:00.123456    // DateTime64(6)
```

Pas de changement necessaire dans le code applicatif pour le parsing des dates.

---

## Plan de migration pour le wrapper Laravel

### Architecture cible

Creer un service `NativeClickhouseClientService` qui remplace `ClickhouseClientService` avec la meme interface publique :

```
app/Services/Clickhouse/
├── ClickhouseClientInterface.php    # Interface commune (nouveau)
├── ClickhouseClientService.php      # Implementation smi2 (existant, garde pour fallback)
└── NativeClickhouseClientService.php # Implementation native (nouveau)
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

### Implementation native

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
     * SELECT avec conversion automatique des bindings smi2 -> natif.
     *
     * Supporte la syntaxe :paramName (comme smi2) en la convertissant
     * vers {paramName:String} pour le protocole natif.
     */
    public static function select(string $sql, array $bindings = []): array
    {
        if (empty($bindings)) {
            return clickhouse_query_array($sql);
        }

        // Conversion des bindings smi2 (:param) -> natif ({param:String})
        [$nativeSql, $nativeBindings] = self::convertBindings($sql, $bindings);

        return clickhouse_query_array($nativeSql, $nativeBindings);
    }

    public static function insert(string $table, array $values, array $columns = []): void
    {
        if (empty($values)) {
            return;
        }

        if (empty($columns)) {
            clickhouse_insert($table, $values);
        } else {
            clickhouse_insert($table, $values, $columns);
        }
    }

    public static function write(string $sql, array $bindings = []): void
    {
        if (empty($bindings)) {
            clickhouse_exec($sql);
            return;
        }

        // Pour write(), les bindings smi2 incluent souvent des noms de tables
        // qu'on ne peut pas parametriser avec le protocole natif.
        // On fait une substitution directe pour les identifiants.
        $resolved = self::resolveIdentifierBindings($sql, $bindings);
        clickhouse_exec($resolved);
    }

    public static function truncate(string $table): void
    {
        clickhouse_exec('TRUNCATE TABLE IF EXISTS ' . $table);
    }

    // ── Helpers de conversion ────────────────────────────────────────────

    /**
     * Convertit les bindings smi2 (:param) vers le format natif ({param:Type}).
     *
     * Infere le type ClickHouse depuis le type PHP de la valeur :
     * - string -> String
     * - int    -> Int64
     * - float  -> Float64
     * - bool   -> UInt8
     * - null   -> pas de binding (remplace par 'NULL' dans la query)
     * - array  -> non supporte (construire la clause manuellement)
     */
    private static function convertBindings(string $sql, array $bindings): array
    {
        $nativeBindings = [];

        foreach ($bindings as $key => $value) {
            if ($value === null) {
                // Remplacer :key par NULL directement
                $sql = str_replace(':' . $key, 'NULL', $sql);
                continue;
            }

            if (is_array($value)) {
                // IN (:values) -> IN ('v1','v2','v3')
                $escaped = array_map(fn($v) => "'" . addslashes((string)$v) . "'", $value);
                $sql = str_replace(':' . $key, implode(',', $escaped), $sql);
                continue;
            }

            $chType = match (true) {
                is_int($value)    => 'Int64',
                is_float($value)  => 'Float64',
                is_bool($value)   => 'UInt8',
                default           => 'String',
            };

            $sql = str_replace(':' . $key, '{' . $key . ':' . $chType . '}', $sql);
            $nativeBindings[$key] = $value;
        }

        return [$sql, $nativeBindings];
    }

    /**
     * Remplace les bindings d'identifiants ({table}, {fieldName}) par substitution directe.
     * Utilise pour write() ou les noms de tables/colonnes sont passes en binding.
     */
    private static function resolveIdentifierBindings(string $sql, array $bindings): array
    {
        foreach ($bindings as $key => $value) {
            $sql = str_replace('{' . $key . '}', (string)$value, $sql);
            $sql = str_replace(':' . $key, "'" . addslashes((string)$value) . "'", $sql);
        }

        return $sql;
    }
}
```

### Mise a jour du ServiceProvider

```php
// app/Providers/ClickhouseServiceProvider.php

$this->app->singleton('clickhouse', function () {
    // Detecter si l'extension native est disponible
    if (function_exists('clickhouse_connect')) {
        return new NativeClickhouseClientService(
            config('clickhouse')
        );
    }
    // Fallback sur smi2
    return new ClickhouseClientService(
        config('clickhouse')
    );
});
```

### Mise a jour de la config

```php
// config/clickhouse.php — ajouter le port natif

'credentials' => [
    'host'     => env('CLICKHOUSE_HOST', 'clickhouse'),
    'port'     => env('CLICKHOUSE_NATIVE_PORT', '9000'),  // Port natif
    'username' => env('CLICKHOUSE_USER', 'default'),
    'password' => env('CLICKHOUSE_PASSWORD', ''),
    'https'    => env('CLICKHOUSE_HTTPS', false),
],
```

---

## Checklist de migration

- [ ] Ajouter `CLICKHOUSE_NATIVE_PORT=9000` dans `.env`
- [ ] Creer `ClickhouseClientInterface.php`
- [ ] Creer `NativeClickhouseClientService.php` avec conversion de bindings
- [ ] Mettre a jour `ClickhouseServiceProvider` pour detecter l'extension
- [ ] Remplacer `DatabaseException` par `RuntimeException` dans les catches
- [ ] Remplacer `$stmt->rows()` par acces direct au tableau
- [ ] Remplacer `$stmt->fetchOne('col')` par `$rows[0]['col'] ?? null`
- [ ] Remplacer `$stmt->fetchOne()` par `$rows[0][array_key_first($rows[0])] ?? null`
- [ ] Adapter les `Dockerfile` pour utiliser `frankenphp php-cli` au lieu de `php`
- [ ] Adapter les `Makefile` pour utiliser `frankenphp php-cli artisan` au lieu de `php artisan`
- [ ] Verifier que le service ClickHouse expose le port 9000 dans `docker-compose.yml`
- [ ] Lancer les tests existants et verifier la compatibilite

## Points d'attention

1. **`frankenphp php-cli`** : Les fonctions de l'extension ne sont disponibles que via le binaire `frankenphp`, pas via `php`. Tous les scripts (`artisan`, cron, workers) doivent etre lances avec `frankenphp php-cli`.

2. **IN avec tableaux** : smi2 supporte `IN (:values)` avec un tableau en binding. L'extension native ne le supporte pas directement — le wrapper `convertBindings()` gere ce cas en inlinant les valeurs.

3. **Noms de tables en binding** : smi2 supporte `{table}` dans les bindings. L'extension native ne fait pas de substitution de noms — le wrapper `resolveIdentifierBindings()` gere ce cas.

4. **Connexion persistante** : L'extension maintient un pool de 4 connexions. Un seul `clickhouse_connect()` suffit au demarrage — pas besoin de reconnecter a chaque requete.

5. **Readonly** : L'extension ne supporte pas le mode `readonly` directement. Si necessaire, creer un utilisateur ClickHouse avec des permissions limitees.
