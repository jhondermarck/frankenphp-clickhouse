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

# Chemins homebrew résolus dynamiquement (icu4c est keg-only et versionné :
# un chemin en dur casse à chaque bump de version).
BREW_PREFIX  := $(shell brew --prefix)
BREW_LIBS    := icu4c pcre2 curl libsodium libzip unixodbc
BREW_LDPATHS := $(foreach lib,$(BREW_LIBS),-L$(shell brew --prefix $(lib))/lib)

CGO_CFLAGS_LOCAL  = $$(php-config --includes) -I$(BREW_PREFIX)/include
CGO_LDFLAGS_LOCAL = $(BREW_LDPATHS) \
	$$(php-config --ldflags) \
	$$(php-config --libs) \
	-L$(BREW_PREFIX)/lib -lbrotlienc -lbrotlicommon -lbrotlidec -lwatcher-c

build:
	CGO_ENABLED=1 \
	GOPATH=$(HOME)/go \
	XCADDY_GO_BUILD_FLAGS='-ldflags "-w -s" -tags nobadger,nomysql,nopgx -p 2' \
	CGO_CFLAGS="$(CGO_CFLAGS_LOCAL)" \
	CGO_LDFLAGS="$(CGO_LDFLAGS_LOCAL)" \
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
	CGO_CFLAGS="$(CGO_CFLAGS_LOCAL)" \
	CGO_LDFLAGS="$(CGO_LDFLAGS_LOCAL)" \
	go test -C clickhouse-ext -v -count=1 .
