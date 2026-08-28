# Unreleased

## Refactorings

* #380: Extracted four private methods from `SqlQuery` to `TransportEndpoint` for generating a common endpoint clause
* #382: Refactored CSV import and export query construction into `ImportBuilder` and `ExportBuilder` with early Pydantic parameter validation. Moved Exasol API clause formatting into `ClauseFormatter` while preserving the existing public `ImportQuery` and `ExportQuery` classes.

## Bugfixes

* #353: Fixed EXPORT `query_or_table` and IMPORT `table` type annotations to include tuple table identifiers (`tuple[str, ...]`)
