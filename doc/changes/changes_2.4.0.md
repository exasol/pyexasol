# 2.4.0 - 2026-09-07

## Summary

This update refactors CSV `IMPORT` and `EXPORT` query construction around dedicated
builders and a shared clause formatter. Parameters are validated before data transfer
starts, and the same construction path is now used by the callback and convenience
APIs. This reduces duplicated formatting logic, makes generated queries more
consistent, and provides clearer errors for invalid parameters while preserving the
existing public `ImportQuery` and `ExportQuery` classes.

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| gitpython | PYSEC-2026-3785 | 3.1.58 | 3.1.59 |
| gitpython | PYSEC-2026-3786 | 3.1.58 | 3.1.59 |
| gitpython | PYSEC-2026-3787 | 3.1.58 | 3.1.59 |
| gitpython | PYSEC-2026-3788 | 3.1.58 | 3.1.59 |
| tornado | GHSA-wwv5-g3v4-889x | 6.5.7 | 6.5.8 |
| tornado | GHSA-8423-8fgw-73vq | 6.5.7 | 6.5.8 |
| tornado | CVE-2026-82397 | 6.5.7 | 6.5.8 |

## Refactorings

* #380: Extracted four private methods from `SqlQuery` to `TransportEndpoint` for generating a common endpoint clause
* #382: Refactored CSV import and export query construction into `ImportBuilder` and `ExportBuilder` with early Pydantic parameter validation. Moved Exasol API clause formatting into `ClauseFormatter` while preserving the existing public `ImportQuery` and `ExportQuery` classes.
* #384: Switched `export_to_callback` and `import_from_callback` to directly use `ImportBuilder` and `ExportBuilder`

## Bugfixes

* #353: Fixed EXPORT `query_or_table` and IMPORT `table` type annotations to include tuple table identifiers (`tuple[str, ...]`)
* #398: Fixed Exasol version parsing for `2025.1.3-p.2`

## Feature

* #393: Added `ImportBuilder` for native parquet import

## Dependency Updates

### `main`

* Updated dependency `cryptography:50.0.0` to `50.0.1`
* Updated dependency `orjson:3.11.9` to `3.12.0`
* Updated dependency `polars:1.43.2` to `1.44.1`
* Updated dependency `pyarrow:25.0.0` to `25.0.1`
* Added dependency `pydantic:2.13.5`
* Updated dependency `pytest-benchmark:5.2.3` to `5.3.0`
* Updated dependency `python-rapidjson:1.23` to `1.25`
* Updated dependency `websocket-client:1.9.0` to `1.9.2`

### `dev`

* Updated dependency `exasol-toolbox:10.4.0` to `10.5.0`
