---
name: Bug report
about: Report incorrect behaviour, a crash, or a data issue
title: ""
labels: bug
assignees: ""
---

## Description

A clear description of what went wrong.

## Reproduction

Minimal PHP snippet and, if relevant, the `CREATE TABLE` / data involved:

```php
clickhouse_connect('clickhouse://…');
// …
```

## Expected vs actual

- **Expected**:
- **Actual** (include the full `RuntimeException` message and `getCode()` if any):

## Environment

- Extension / release version or commit:
- FrankenPHP + PHP version:
- ClickHouse server version:
- OS / architecture:
- Build method (Docker / local xcaddy):

## Additional context

Logs, stack traces, or anything else that helps.
