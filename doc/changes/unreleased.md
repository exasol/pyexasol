# Unreleased

## Summary

## Bugfix

* #349: Modified exception reporting in `import_from_*` methods by coordinating completion of the HTTP and SQL import threads with a threading event

## Refactoring

* #251: Simplified local runs of the integration tests to only run tests for a certificate when `--with-cert` is specified
