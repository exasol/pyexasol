# 2.3.2 - 2026-08-25

## Summary

This update improves error reporting for ``import_from_callback`` and
``export_to_callback`` by coordinating the completion of the callback, HTTP, and SQL
threads.

## Bugfix

* #349: Modified exception reporting in `import_from_*` and `export_to_*` methods by coordinating completion of the HTTP and SQL import threads with a threading event

## Refactoring

* #251: Simplified local runs of the integration tests to only run tests for a certificate when `--with-cert` is specified
