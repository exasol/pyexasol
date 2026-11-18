# Unreleased

## Summary

The minimum supported Python version is now 3.11.5, as later `soupsieve` versions, needed to resolve a reported vulnerability, no longer support older versions.

### Deprecations

The following classes in `pyexasol/http_transport.py` are deprecated. Each emits a `PyexasolDeprecationWarning` the first time it is instantiated after `pyexasol` is loaded. They remain available as thin wrappers around the new builders for backwards compatibility but will be removed in a future release:

| Deprecated                | Replacement                                                             |
|---------------------------|-------------------------------------------------------------------------|
| `SqlQuery`                | `ImportBuilder` / `ExportBuilder` (`pyexasol.query_builders.csv.builders`) |
| `ImportQuery`             | `ImportBuilder` (`pyexasol.query_builders.csv.builders`)                |
| `ExportQuery`             | `ExportBuilder` (`pyexasol.query_builders.csv.builders`)                |
| `ExaSQLImportThread`      | `ExaSQLThread(query_builder=ImportBuilder(...))`                        |
| `ExaSQLExportThread`      | `ExaSQLThread(query_builder=ExportBuilder(...))`                        |

The builders perform the same function with additional Pydantic validation.

## Features

* #394:
  * Extracted the tunnel server code from `http_transport.py` into the new `ExaTunnelServer` in `pyexasol/transport/tunnel_server.py`, so other transports can share it with the existing one. It provides the `exa_address` property and the `serve_first_client()` method, which `ExaHttpThread` now uses
  * Added `ExaParquetHttpThread`, which serves a single local Parquet file to Exasol through an HTTP tunnel, including HTTP range requests

## Refactorings

* #385: Deprecated `SqlQuery`, `ImportQuery`, `ExportQuery`, `ExaSQLImportThread` and `ExaSQLExportThread` in favor of the builders and `ExaSQLThread`, see Deprecations above
* #389: `ExaConnection.import_parallel` and `export_parallel` now use `ImportBuilder` and `ExportBuilder` via `ExaSQLThread` internally, instead of the deprecated `ExaSQLImportThread` and `ExaSQLExportThread`; their external behavior is unchanged
* #484: Dropped support for Python versions < 3.11.5

## Documentation

* #370: Documented the row separator required when importing files with `CRLF` line endings
