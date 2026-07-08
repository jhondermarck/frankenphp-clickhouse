# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately via GitHub's
[Security Advisories](https://github.com/jhondermarck/frankenphp-clickhouse/security/advisories/new)
rather than opening a public issue. You can expect an initial response within
a few days.

## Trust model

A few properties are important when embedding this extension:

- **The DSN is trusted configuration.** `ca_cert` / `client_cert` / `client_key`
  are read from the host filesystem, so a DSN built from untrusted input would
  be an arbitrary-file-read vector. Never construct a DSN from user data.
- **SQL parameters are safe; identifiers are not parameterized.** Values use
  native `{name:Type}` bindings. Table and column names are validated but
  concatenated into SQL — pass them as code constants, not user input.
- The bundled `docker-compose.yml` is a development/benchmark stack (default
  password, containers as root). See `sample/Dockerfile` for a non-root
  production build.

See the *Security Notes* section of the README for details.
