FROM dunglas/frankenphp:builder AS builder

COPY --from=caddy:builder /usr/bin/xcaddy /usr/bin/xcaddy

# CGO must be enabled to build FrankenPHP
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
    --with github.com/jhondermarck/frankenphp-clickhouse/clickhouse-ext/build


FROM dunglas/frankenphp AS php-base

COPY --from=builder /frankenphp /usr/local/bin/frankenphp

RUN apt update && apt install -y libnss3-tools
RUN install-php-extensions \
    intl \
    zip \
    opcache \
    apcu \
    pcntl \
    sockets \
    mbstring


FROM php-base AS php-dev

COPY --from=composer /usr/bin/composer /usr/bin/composer

COPY docker/franken/Caddyfile /etc/caddy/Caddyfile

WORKDIR /app
COPY docker/franken/php.ini /usr/local/etc/php/php.ini

COPY web /app
RUN composer install --no-dev
RUN composer dump-autoload --optimize --apcu
