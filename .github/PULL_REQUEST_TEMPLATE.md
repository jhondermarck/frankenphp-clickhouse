## What & why

Describe the change and the motivation.

## Checklist

- [ ] `make test` passes (PHP integration tests against a running ClickHouse)
- [ ] `make test_go` passes; `-race` run if touching the handle registries or reaper
- [ ] New behaviour has a test (`web/test.php` and/or `clickhousetypes_test.go`)
- [ ] No hot-path performance regression (ran `make bench` if relevant)
- [ ] The hand-maintained C bridge was updated manually if an exported function
      changed (did **not** run `make ext`)
- [ ] `gofmt` clean

## Notes

Anything reviewers should know — trade-offs, follow-ups, benchmarks.
