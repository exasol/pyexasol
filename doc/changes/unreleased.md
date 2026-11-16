# Unreleased

## Summary

## Features

* #394:
  * Extracted the tunnel server code from `http_transport.py` into the new `ExaTunnelServer` in `pyexasol/transport/tunnel_server.py`, so other transports can share it with the existing one
  * Added `ExaParquetHttpRequestHandler`, which serves a single local Parquet file over HTTP, including range requests, through the tunnel
  * Added `ExaParquetHttpThread`, which serves a single local Parquet file, including HTTP range requests, through an Exasol tunnel
