# Contributing

Thanks for your interest in improving frankenphp-clickhouse.

## Project layout

The extension is a Go package (`clickhouse-ext/`) compiled into a FrankenPHP
binary via [xcaddy](https://github.com/caddyserver/xcaddy). The Go side is
split by concern — see the *Project Structure* section of the README. A small
hand-written C bridge exposes the Go `//export` functions as PHP functions.

## Building

The reproducible path is Docker:

```bash
make up        # build + start ClickHouse and FrankenPHP
make restart   # rebuild after a change
```

For a local (macOS) build you need xcaddy, Go ≥ 1.26, PHP ≥ 8.2 with dev
headers, and a C toolchain:

```bash
make build     # compile the FrankenPHP binary with the extension
```

## Testing

```bash
make test      # PHP integration tests (web/test.php) — needs a running ClickHouse
make test_go   # Go unit tests
```

Run the Go concurrency test under the race detector when touching the handle
registries or the reaper:

```bash
go test -race -run TestRegistryConcurrency ./clickhouse-ext
```

New behaviour should come with a test in `web/test.php` (integration) and/or
`clickhouse-ext/clickhousetypes_test.go` (unit).

## ⚠️ The C bridge is hand-maintained — never run `make ext`

`clickhousephp.c`, `clickhousephp.h`, `clickhousephp_arginfo.h`, and
`clickhousephp.stub.php` are edited by hand. The FrankenPHP extension
generator (`make ext`) would **overwrite** them and wipe the custom
exception / ClickHouse-error-code logic. If you add or change an exported
function, update these four files manually to match the Go `//export`
signature.

## Conventions

- Every exported function has a panic guard (`phpPanicGuard` / `nullPanicGuard`
  / `idPanicGuard`) — a Go panic must never crash the FrankenPHP process.
- Errors reach PHP as `RuntimeException`; server errors carry the ClickHouse
  code via `getCode()`. String-returning functions use the `"ERROR[code]: …"`
  protocol; NULL/-1-returning functions use the per-thread last-error channel.
- Always close cursors and batches; they hold a pooled connection until
  `close`/`send`/`abort`.
- Run `gofmt` before committing. `gopls` may report cgo diagnostics that are
  false positives in this environment — the build is the source of truth.

## Pull requests

- Keep changes focused and describe the motivation.
- Make sure `make test` and `make test_go` pass, and benchmarks show no
  regression for changes on the hot path.
