# Unreleased

## Summary

In this major release, there are the following breaking changes:
* Replaced the specialized `ExaSQLExportThread` and `ExaSQLImportThread`
  with a generic `ExaSQLThread` that receives a query builder.
* Moved `ExportQuery` and `ImportQuery` to
  `pyexasol.query_builders.csv_builders`. Their interfaces changed so the source table
  or query is provided when constructing the builder, and `build_query()` now only
  receives the Exasol address list.

## Refactoring

* #251: Simplified local runs of the integration tests to only run tests for a certificate when `--with-cert` is specified
* #377: Refactored SQL thread execution to use composition instead of inheritance for import and export operations
