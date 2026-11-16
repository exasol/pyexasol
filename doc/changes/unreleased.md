# Unreleased

## Summary

`import_from_parquet()` now has a native import path, which is used automatically when
the database supports it:

* **Exasol 2026.1.0 or newer:** The Parquet files are imported natively. PyExasol serves
  each local file to the database through a tunnel, and the database reads and loads it
  directly.
* **Older Exasol versions:** The existing callback-based import is used.

The method signature and the way you call it are the same on all versions, so existing
code does not need to change unless it passes `callback_params`, or `import_params` that
the native import does not accept (see below), to a database that now takes the native
path.

## Features

* #394:
    * Extracted the tunnel server code from `http_transport.py` into the new
      `ExaTunnelServer` in `pyexasol/transport/tunnel_server.py`, so other transports
      can share it with the existing one
    * Added `ExaParquetHttpRequestHandler`, which serves a single local Parquet file
      over HTTP, including range requests, through the tunnel
    * Added `ExaParquetHttpThread`, which serves a single local Parquet file, including
      HTTP range requests, through an Exasol tunnel
    * Added native Parquet import to `import_from_parquet()` for Exasol >= 2026.1.0
