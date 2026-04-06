FROM dunglas/frankenphp:builder AS builder

COPY --from=caddy:builder /usr/bin/xcaddy /usr/bin/xcaddy

RUN CGO_ENABLED=1 \
    XCADDY_SETCAP=1 \
    XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=nobadger,nomysql,nopgx" \
    CGO_CFLAGS="$(php-config --includes) -D_GNU_SOURCE" \
    CGO_LDFLAGS="$(php-config --ldflags) $(php-config --libs)" \
    GOPROXY=direct \
    GONOSUMDB=* \
    xcaddy build \
    --output /frankenphp \
    --with github.com/dunglas/frankenphp=./ \
    --with github.com/dunglas/caddy-cbrotli \
    --with github.com/dunglas/frankenphp/caddy=./caddy/ \
    --with github.com/jhondermarck/frankenphp-clickhouse/clickhouse-ext


FROM dunglas/frankenphp

COPY --from=builder /frankenphp /usr/local/bin/frankenphp

WORKDIR /app
COPY . /app
