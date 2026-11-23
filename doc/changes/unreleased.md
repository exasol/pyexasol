# Unreleased

## Summary

In this major release, the following changes were made:

* The minimum supported Python version is now 3.11.5, as later `soupsieve` versions, needed to resolve a reported vulnerability, no longer support older versions.
* An unknown key in `import_params` / `export_params` of `ExaConnection.import_parallel` and `export_parallel` now raises `pydantic.ValidationError` (a `ValueError`) instead of `TypeError`, because the parameters are now validated by `ImportBuilder` / `ExportBuilder`. Code that catches `TypeError` for this case must catch `pydantic.ValidationError` instead. Invalid values already raised `pydantic.ValidationError`. Keys that collide with arguments the method passes to the builder itself (such as `compression`, `table` or `query_or_table`) still raise `TypeError`.
* `import_from_parquet()` now resolves the source files and checks them for nested columns before starting the import, for both import paths. These problems now raise a `ValueError` directly instead of an `ExaImportError` wrapping it, so code that catches `ExaImportError` for them must also catch `ValueError`. The benefit is that they are found before any data is imported.
* `import_from_parquet()` now has a native import path, chosen by database version:
  * **Exasol 2026.1.0 or newer:** Native import. PyExasol serves each local Parquet file to the database through its own tunnel, and the database reads and loads it directly.
  * **Older Exasol versions:** The existing callback-based import. PyArrow reads the Parquet file and writes it as CSV text, which the database parses.

### Behavior of `import_from_parquet()` on Exasol 2026.1.0 or newer

#### Breaking behavior

Code that works today can fail after a database upgrade to 2026.1.0 if it relies on any of the following, which only the callback-based import supports:

* **Implicit type conversion:** The native import does not convert between types, so the Parquet column types must match the table. For example, a Parquet `double` cannot be imported into a `DECIMAL` column. The callback-based import writes CSV text, so the database converts the values to the column types.
* **`callback_params`:** The native import supports only `max_files_per_batch` and raises `pydantic.ValidationError` for any other key.
* **Generic `import_params`:** The native import accepts only the parameters of `pyexasol.query_builders.parquet.builders.ImportBuilder`.

To resolve this, either:

* Adapt the code, for example by making the Parquet column types match the table (e.g. a `DOUBLE` column instead of `DECIMAL`; `examples/b08_parquet.py` shows this), or
* Pass `native=False` to keep using the callback-based import.

#### Changed behavior

* **Partial import with autocommit off:** The callback-based import loads all files with a single `IMPORT`, which fails as a whole. The native import runs one `IMPORT` per batch (by default one per file). If autocommit is off and a later batch fails, the earlier batches stay in your open transaction, so roll back to discard them. With autocommit on, the native import rolls back all batches itself. Setting `max_files_per_batch` to at least the number of files uses a single `IMPORT`, or pass `native=False` to keep using the callback-based import.

### Deprecations

The following are deprecated and each emits a `PyexasolDeprecationWarning`. They remain available for backwards compatibility but will be removed in a future release:

| Deprecated                                             | Replacement                                                                |
|--------------------------------------------------------|----------------------------------------------------------------------------|
| `pyexasol.http_transport.SqlQuery`                     | `ImportBuilder` / `ExportBuilder` (`pyexasol.query_builders.csv.builders`) |
| `pyexasol.http_transport.ImportQuery`                  | `ImportBuilder` (`pyexasol.query_builders.csv.builders`)                   |
| `pyexasol.http_transport.ExportQuery`                  | `ExportBuilder` (`pyexasol.query_builders.csv.builders`)                   |
| `pyexasol.http_transport.ExaSQLImportThread`           | `ExaSQLThread(query_builder=ImportBuilder(...))`                           |
| `pyexasol.http_transport.ExaSQLExportThread`           | `ExaSQLThread(query_builder=ExportBuilder(...))`                           |
| `pyexasol.callback.get_parquet_files`                  | `pyexasol.transport.parquet.importer.get_parquet_files`                    |

The classes in `pyexasol/http_transport.py` are thin wrappers around the new builders. The builders perform the same function with additional Pydantic validation.

## Features

* #394:
  * Extracted the tunnel server code from `http_transport.py` into the new `ExaTunnelServer` in `pyexasol/transport/tunnel_server.py`, so other transports can share it with the existing one. It provides the `exa_address` property and the `serve_first_client()` method, which `ExaHttpThread` now uses
  * Added `ExaParquetHttpThread`, which serves a single local Parquet file to Exasol through an HTTP tunnel, including HTTP range requests
  * Added native Parquet import to `import_from_parquet()` for Exasol >= 2026.1.0.
  * Added `ExaNativeParquetImportError`, a subclass of `ExaImportError` raised when a native Parquet import fails. Its message names the files of the failed batch.

## Refactorings

* #385: Deprecated `SqlQuery`, `ImportQuery`, `ExportQuery`, `ExaSQLImportThread` and `ExaSQLExportThread` in favor of the builders and `ExaSQLThread`
* #389:
  * `ExaConnection.import_parallel` and `export_parallel` now use `ImportBuilder` and `ExportBuilder` directly, instead of the deprecated `ExaSQLImportThread` and `ExaSQLExportThread`
  * An unknown key in `import_params` / `export_params` of these methods now raises `pydantic.ValidationError` instead of `TypeError`, see the Summary
  * `export_parallel` no longer fails when `query_or_table` is a `(schema, table)` tuple and `query_params` is given. Like `export_to_callback`, it only formats `query_or_table` with `query_params` when it is a string
* #484: Dropped support for Python versions < 3.11.5
* #394:
  * Moved `get_parquet_files` from `pyexasol/callback.py` to `pyexasol/transport/parquet/importer.py`, where `ensure_no_nested_columns` is also defined. The old import path still works but is deprecated.
  * `import_from_parquet()` now resolves the source files and checks them for nested columns before the transport starts.

## Documentation

* #370: Documented the row separator required when importing files with `CRLF` line endings
* #474: Added links to import/export parameters in user guide for all callback variants
