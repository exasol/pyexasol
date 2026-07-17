# 2.3.0 - 2026-07-17

## Summary

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| setuptools | PYSEC-2026-3447 | 82.0.1 | 83.0.0 |

## Features

* #348: Added `ExaConnection.execute_sql_script()` for executing multi-statement SQL
  scripts, including Exasol script bodies terminated by a standalone `/` line.

## Dependency Updates

### `main`

* Updated dependency `polars:1.41.2` to `1.42.1`
* Updated dependency `pyarrow:24.0.0` to `25.0.0`

### `dev`

* Updated dependency `exasol-integration-test-docker-environment:6.2.0` to `6.4.1`
* Updated dependency `exasol-toolbox:10.0.0` to `10.3.0`
