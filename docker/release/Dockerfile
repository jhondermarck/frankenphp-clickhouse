# Distributable runtime image: FrankenPHP with the clickhousephp extension
# baked in and NO application code. Mount or COPY your own app into /app.
#
#   docker run -v "$PWD/app:/app" ghcr.io/jhondermarck/frankenphp-clickhouse
#
# Built and pushed by .github/workflows/release.yml on a version tag.

FROM dunglas/frankenphp:builder AS builder

COPY --from=caddy:builder /usr/bin/xcaddy /usr/bin/xcaddy

# The Go extension source; xcaddy compiles it into the FrankenPHP binary.
COPY clickhouse-ext /ext/build

RUN CGO_ENABLED=1 \
    XCADDY_SETCAP=1 \
    XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=nobadger,nomysql,nopgx" \
    CGO_CFLAGS="$(php-config --includes) -D_GNU_SOURCE" \
    CGO_LDFLAGS="$(php-config --ldflags) $(php-config --libs)" \
    xcaddy build \
    --output /frankenphp \
    --with github.com/dunglas/frankenphp=./ \
    --with github.com/dunglas/caddy-cbrotli \
    --with github.com/dunglas/frankenphp/caddy=./caddy/ \
    --with github.com/jhondermarck/frankenphp-clickhouse/clickhouse-ext=/ext/build


FROM dunglas/frankenphp

COPY --from=builder /frankenphp /usr/local/bin/frankenphp

WORKDIR /app
