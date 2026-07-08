FROM dunglas/frankenphp:builder AS builder

COPY --from=caddy:builder /usr/bin/xcaddy /usr/bin/xcaddy

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
    --with github.com/jhondermarck/frankenphp-clickhouse/clickhouse-ext


FROM dunglas/frankenphp

COPY --from=builder /frankenphp /usr/local/bin/frankenphp

WORKDIR /app
COPY . /app

# Production hardening: drop root. This assumes the app listens on a
# non-privileged port (e.g. :8080) behind a TLS-terminating proxy. If you
# need FrankenPHP to bind :443 directly, grant the binary
# CAP_NET_BIND_SERVICE (setcap 'cap_net_bind_service=+ep' /usr/local/bin/frankenphp)
# rather than running as root.
RUN groupadd -r app && useradd -r -g app -d /app app \
    && chown -R app:app /app
USER app
