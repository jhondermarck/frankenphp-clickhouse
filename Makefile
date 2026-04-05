.PHONY: all build build_local ext bench bench_local serve bench_worker test test_go up down restart clean

all: build

# ── Docker (production / CI) ──────────────────────────────────────────────────

up:
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose down
	docker-compose up -d

# ── Local dev (macOS, xcaddy) ─────────────────────────────────────────────────

build:
	CGO_ENABLED=1 \
	GOPATH=$(HOME)/go \
	XCADDY_GO_BUILD_FLAGS='-ldflags "-w -s" -tags nobadger,nomysql,nopgx -p 2' \
	CGO_CFLAGS="$$(php-config --includes) -I/opt/homebrew/include" \
	CGO_LDFLAGS="-L/opt/homebrew/opt/icu4c@77/lib \
	              -L/opt/homebrew/opt/pcre2/lib \
	              -L/opt/homebrew/opt/curl/lib \
	              -L/opt/homebrew/opt/libsodium/lib \
	              -L/opt/homebrew/opt/libzip/lib \
	              -L/opt/homebrew/opt/unixodbc/lib \
	              $$(php-config --ldflags) \
	              $$(php-config --libs) \
	              -L/opt/homebrew/lib -lbrotlienc -lbrotlicommon -lbrotlidec -lwatcher-c" \
	xcaddy build \
	  --output frankenphp-clickhouse \
	  --with github.com/dunglas/frankenphp/caddy \
	  --with github.com/jhondermarck/frankenphp-clickhouse/clickhouse-ext/build=./clickhouse-ext/build

ext:
	cd clickhouse-ext \
	  && rm -rf build \
	  && ../frankenphp extension-init clickhousephp.go \
	  && cp clickhousetypes.go build/ \
	  && cp clickhousearray.go build/ \
	  && cp clickhousetypes_test.go build/ \
	  && cd build && go mod init github.com/jhondermarck/frankenphp-clickhouse/clickhouse-ext/build && go mod tidy

bench:
	./frankenphp-clickhouse php-cli web/bench.php

serve:
	./frankenphp-clickhouse run --config web/Caddyfile

bench_worker:
	./frankenphp-clickhouse php-cli web/bench_http.php

test:
	./frankenphp-clickhouse php-cli web/test.php

test_go:
	CGO_ENABLED=1 \
	GOPATH=$(HOME)/go \
	CGO_CFLAGS="$$(php-config --includes) -I/opt/homebrew/include" \
	CGO_LDFLAGS="-L/opt/homebrew/opt/icu4c@77/lib \
	              -L/opt/homebrew/opt/pcre2/lib \
	              -L/opt/homebrew/opt/curl/lib \
	              -L/opt/homebrew/opt/libsodium/lib \
	              -L/opt/homebrew/opt/libzip/lib \
	              -L/opt/homebrew/opt/unixodbc/lib \
	              $$(php-config --ldflags) \
	              $$(php-config --libs) \
	              -L/opt/homebrew/lib -lbrotlienc -lbrotlicommon -lbrotlidec -lwatcher-c" \
	go test -C clickhouse-ext/build -v -count=1 .
