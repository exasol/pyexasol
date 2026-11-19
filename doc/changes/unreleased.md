# Unreleased

## Summary

`import_from_parquet()` now has a native import path. The path is chosen by database version:

* **Exasol 2026.1.0 or newer:** Native import. PyExasol serves each local Parquet file to the database through its own tunnel, and the database reads and loads it directly.
* **Older Exasol versions:** The existing callback-based import. PyArrow reads the Parquet file and writes it as CSV text, which the database parses.

## Breaking behavior for `import_from_parquet()` on Exasol 2026.1.0 or newer

Code that works today can fail after a database upgrade to 2026.1.0 if it relies on any of the following, which only the callback-based import supports:

* **Implicit type conversion:** The native import does not convert between types, so the Parquet column types must match the table. For example, a Parquet `double` cannot be imported into a `DECIMAL` column. The callback-based import writes CSV text, so the database converts the values to the column types.
* **`callback_params`:** The native import ignores them, except for `max_files_per_batch`.
* **Atomicity:** If autocommit is on, the native import runs all batches in one transaction, so a failure rolls back every batch. The callback-based import commits as it goes.
* **Generic `import_params`:** The native import accepts only the parameters of `pyexasol.query_builders.parquet.builders.ImportBuilder`.

To resolve this, either:

* Adapt the code, for example by making the Parquet column types match the table (e.g. a `DOUBLE` column instead of `DECIMAL`; `examples/b08_parquet.py` shows this), or
* Pass `native=False` to keep using the callback-based import.

The minimum supported Python version is now 3.11.5, as later `soupsieve` versions, needed to resolve a reported vulnerability, no longer support older versions.

### Behavior change in `import_parallel` and `export_parallel`

An unknown key in `import_params` / `export_params` of `ExaConnection.import_parallel` and `export_parallel` now raises `pydantic.ValidationError` (a `ValueError`) instead of `TypeError`, because the parameters are now validated by `ImportBuilder` / `ExportBuilder`. Code that catches `TypeError` for this case must catch `pydantic.ValidationError` instead. Invalid values already raised `pydantic.ValidationError`. Keys that collide with arguments the method passes to the builder itself (such as `compression`, `table` or `query_or_table`) still raise `TypeError`.

### Deprecations

The following classes in `pyexasol/http_transport.py` are deprecated. Each emits a `PyexasolDeprecationWarning` when instantiated; as for any warning, Python's warning filters decide whether and how often it is shown. They remain available as thin wrappers around the new builders for backwards compatibility but will be removed in a future release:

| Deprecated           | Replacement                                                                |
|----------------------|----------------------------------------------------------------------------|
| `SqlQuery`           | `ImportBuilder` / `ExportBuilder` (`pyexasol.query_builders.csv.builders`) |
| `ImportQuery`        | `ImportBuilder` (`pyexasol.query_builders.csv.builders`)                   |
| `ExportQuery`        | `ExportBuilder` (`pyexasol.query_builders.csv.builders`)                   |
| `ExaSQLImportThread` | `ExaSQLThread(query_builder=ImportBuilder(...))`                           |
| `ExaSQLExportThread` | `ExaSQLThread(query_builder=ExportBuilder(...))`                           |

The builders perform the same function with additional Pydantic validation.

## Features

* #394:
  * Extracted the tunnel server code from `http_transport.py` into the new `ExaTunnelServer` in `pyexasol/transport/tunnel_server.py`, so other transports can share it with the existing one. It provides the `exa_address` property and the `serve_first_client()` method, which `ExaHttpThread` now uses
  * Added `ExaParquetHttpThread`, which serves a single local Parquet file to Exasol through an HTTP tunnel, including HTTP range requests
  * Added native Parquet import to `import_from_parquet()` for Exasol >= 2026.1.0.

## Refactorings

* #385: Deprecated `SqlQuery`, `ImportQuery`, `ExportQuery`, `ExaSQLImportThread` and `ExaSQLExportThread` in favor of the builders and `ExaSQLThread`
* #389:
  * `ExaConnection.import_parallel` and `export_parallel` now use `ImportBuilder` and `ExportBuilder` directly, instead of the deprecated `ExaSQLImportThread` and `ExaSQLExportThread`
  * An unknown key in `import_params` / `export_params` of these methods now raises `pydantic.ValidationError` instead of `TypeError`, see Behavior change above
  * `export_parallel` no longer fails when `query_or_table` is a `(schema, table)` tuple and `query_params` is given. Like `export_to_callback`, it only formats `query_or_table` with `query_params` when it is a string
* #484: Dropped support for Python versions < 3.11.5

## Documentation

* #370: Documented the row separator required when importing files with `CRLF` line endings
