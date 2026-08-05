# 2.3.1 - 2026-08-05

## Summary

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| cryptography | PYSEC-2026-3552 | 49.0.0 | 50.0.0 |
| gitpython | GHSA-3rp5-jjmw-4wv2 | 3.1.52 | 3.1.53 |
| gitpython | GHSA-fjr4-x663-mwxc | 3.1.52 | 3.1.54 |
| gitpython | GHSA-6p8h-3wgx-97gf | 3.1.52 | 3.1.54 |
| gitpython | GHSA-r9mr-m37c-5fr3 | 3.1.52 | 3.1.54 |
| gitpython | GHSA-94p4-4cq8-9g67 | 3.1.52 | 3.1.55 |
| gitpython | GHSA-3f7w-8rr8-f37f | 3.1.52 | 3.1.57 |
| gitpython | GHSA-p538-c434-8v24 | 3.1.52 | 3.1.56 |

## Documentation

* #363: Fixed link to changelog in pypi

## Refactoring

* #354: Updated to `exasol-toolbox` 10.4.0
* #723: Added integration-test coverage for Exasol 2025.1.11 and 2026.1.0
* #238: Updated tests to run with Python 3.14 as exasol-integration-test-docker-environment supports it
* #357: Enacted mypy checks for examples and tests

## Dependency Updates

### `main`

* Updated dependency `cryptography:49.0.0` to `50.0.0`
* Updated dependency `packaging:26.2` to `26.3`
* Updated dependency `polars:1.42.1` to `1.43.2`

### `dev`

* Updated dependency `exasol-integration-test-docker-environment:6.4.1` to `6.5.1`
* Updated dependency `exasol-toolbox:10.3.0` to `10.4.0`
