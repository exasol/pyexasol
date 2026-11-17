# Unreleased

## Summary

The minimum supported Python version is now 3.11.5, as later `soupsieve` versions, needed to resolve a reported vulnerability, no longer support older versions.

`import_from_parquet()` now has a native import path. The path is chosen by database version:

* **Exasol 2026.1.0 or newer:** Native import. PyExasol serves each local Parquet file to the database through a tunnel, and the database reads and loads it directly.
* **Older Exasol versions:** The existing callback-based import. PyArrow reads the Parquet file and writes it as CSV text, which the database parses.

## Breaking behavior for `import_from_parquet()` on Exasol 2026.1.0 or newer

Code that works today can fail after a database upgrade to 2026.1.0 if it relies on any of the following, which only the callback-based import supports:

* **Implicit type conversion:** The native import does not convert between types, so the Parquet column types must match the table. For example, a Parquet `double` cannot be imported into a `DECIMAL` column. The callback-based import writes CSV text, so the database converts the values to the column types.
* **`callback_params`:** The native import raises `ValueError` if they are passed.
* **Generic `import_params`:** The native import accepts only the parameters of `pyexasol.query_builders.parquet.builders.ImportBuilder`.

To resolve this, either:

* Adapt the code, for example by making the Parquet column types match the table (e.g. a `DOUBLE` column instead of `DECIMAL`; `examples/b08_parquet.py` shows this), or
* Pass `native=False` to keep using the callback-based import.

## Features

* #394:
  * Extracted the tunnel server code from `http_transport.py` into the new `ExaTunnelServer` in `pyexasol/transport/tunnel_server.py`, so other transports can share it with the existing one. It provides the `exa_address` property and the `serve_first_client()` method, which `ExaHttpThread` now uses
  * Added `ExaParquetHttpThread`, which serves a single local Parquet file to Exasol through an HTTP tunnel, including HTTP range requests
  * Added native Parquet import to `import_from_parquet()` for Exasol >= 2026.1.0

## Refactorings

* #484: Dropped support for Python versions < 3.11.5
