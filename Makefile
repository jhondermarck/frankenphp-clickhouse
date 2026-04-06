.PHONY: all build ext bench bench_worker_docker bench_docker up down restart test test_go frankenphp

all: build

# ── Docker (production / CI) ──────────────────────────────────────────────────

up:
	docker-compose up -d

down:
	docker-compose down

bench_docker:
	docker-compose exec frankenphp frankenphp php-cli /app/bench.php

restart:
	docker-compose down
	docker-compose up -d --build

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
	  --with github.com/jhondermarck/frankenphp-clickhouse/clickhouse-ext=./clickhouse-ext

frankenphp:
	@if [ ! -f frankenphp ]; then \
	  ARCH=$$(uname -m); \
	  case "$$ARCH" in \
	    arm64)  FILE=frankenphp-mac-arm64 ;; \
	    x86_64) FILE=frankenphp-mac-x86_64 ;; \
	    *)      echo "Unsupported arch: $$ARCH" && exit 1 ;; \
	  esac; \
	  echo "Downloading $$FILE…"; \
	  curl -fsSL "https://github.com/dunglas/frankenphp/releases/latest/download/$$FILE" -o frankenphp; \
	  chmod +x frankenphp; \
	  echo "Downloaded frankenphp"; \
	fi

ext: frankenphp
	cd clickhouse-ext \
	  && ../frankenphp extension-init clickhousephp.go \
	  && rm -f clickhousephp_generated.go \
	  && sed -i '' 's/go_clickhouse_/clickhouse_/g' clickhousephp.c

bench:
	./frankenphp-clickhouse php-cli web/bench.php

bench_worker:
	docker-compose exec frankenphp php /app/bench_http.php

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
	go test -C clickhouse-ext -v -count=1 .
