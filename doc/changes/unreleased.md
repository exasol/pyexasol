# Unreleased

## Summary

The minimum supported Python version is now 3.11.5, as later `soupsieve` versions, needed to resolve a reported vulnerability, no longer support older versions.

## Features

* #394:
  * Extracted the tunnel server code from `http_transport.py` into the new `ExaTunnelServer` in `pyexasol/transport/tunnel_server.py`, so other transports can share it with the existing one. It provides the `exa_address` property and the `serve_first_client()` method, which `ExaHttpThread` now uses
  * Added `ExaParquetHttpThread`, which serves a single local Parquet file to Exasol through an HTTP tunnel, including HTTP range requests

## Refactorings

* #484: Dropped support for Python versions < 3.11.5
