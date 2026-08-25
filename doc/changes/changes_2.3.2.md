# 2.3.2 - 2026-08-25

## Summary

In this patch release, error reporting for ``import_from_callback`` and
``export_to_callback`` is improved by coordinating the completion of the callback,
HTTP, and SQL threads.

## Bugfixes

* #349: Modified exception reporting in `import_from_*` and `export_to_*` methods by coordinating completion of the HTTP and SQL import threads with a threading event

## Refactorings

* #251: Simplified local runs of the integration tests to only run tests for a certificate when `--with-cert` is specified
