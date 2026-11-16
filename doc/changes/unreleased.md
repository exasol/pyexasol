# Unreleased

## Summary

`import_from_parquet()` now has a native import path, which is used automatically when the database supports it:

* **Exasol 2026.1.0 or newer:** The Parquet files are imported natively. PyExasol serves each local file to the database through a tunnel, and the database reads and loads it directly.
* **Older Exasol versions:** The existing callback-based import is used.

The method signature and the way you call it are the same on all versions, so existing code does not need to change unless it passes `callback_params`, or `import_params` that the native import does not accept (see below), to a database that now takes the native path.

## Features

* #394:
  * Added native Parquet import to `import_from_parquet()` for Exasol >= 2026.1.0
    * Each file is served through its own tunnel and imported with its own `IMPORT` statement. Files are not imported atomically: if one fails, earlier files stay imported (subject to `autocommit`).
    * On the native path, `callback_params` is not supported and raises a `ValueError`; use `import_params` instead.
  * Added `ExaParquetHttpThread`, which serves a single local Parquet file, including HTTP range requests, through an Exasol tunnel
  * Extracted the tunnel server code from `http_transport.py` into the new `ExaTunnelServer` in `pyexasol/transport/tunnel_server.py`, so the Parquet server and the existing transport share it
  * Moved the "does not match any files" check into `get_parquet_files`, so it now raises a `ValueError` for any source type that resolves to no files
