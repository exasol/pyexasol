# Unreleased

## Summary

This update refactors CSV `IMPORT` and `EXPORT` query construction around dedicated
builders and a shared clause formatter. Parameters are validated before data transfer
starts, and the same construction path is now used by the callback and convenience
APIs. This reduces duplicated formatting logic, makes generated queries more
consistent, and provides clearer errors for invalid parameters while preserving the
existing public `ImportQuery` and `ExportQuery` classes.

## Refactorings

* #380: Extracted four private methods from `SqlQuery` to `TransportEndpoint` for generating a common endpoint clause
* #382: Refactored CSV import and export query construction into `ImportBuilder` and `ExportBuilder` with early Pydantic parameter validation. Moved Exasol API clause formatting into `ClauseFormatter` while preserving the existing public `ImportQuery` and `ExportQuery` classes.
  * Comments containing the opening SQL block-comment delimiter (`/*`) are now rejected during validation; previously only the closing delimiter (`*/`) was rejected.
* #384: Switched `export_to_callback` and `import_from_callback` to directly use `ImportBuilder` and `ExportBuilder`

## Bugfixes

* #353: Fixed EXPORT `query_or_table` and IMPORT `table` type annotations to include tuple table identifiers (`tuple[str, ...]`)
