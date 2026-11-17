# Unreleased

## Summary

This release adds support for option `BOOLEAN` in `export_to_file()`. See the section "Exasol Parameters" in the user guide for supported values.

The minimum supported Python version is now 3.11.5, as later `soupsieve` versions, needed to resolve a reported vulnerability, no longer support older versions.

## Features

* #264: Added support for option `BOOLEAN` in `export_to_file`
* #394:
    * Extracted the tunnel server code from `http_transport.py` into the new `ExaTunnelServer` in `pyexasol/transport/tunnel_server.py`, so other transports can share it with the existing one

## Refactorings

* #484: Dropped support for Python versions < 3.11.5

## Bugfixes

* #473: Fixed DSN when using environment variable `EXAHOST` in integration tests
