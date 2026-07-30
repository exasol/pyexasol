# Unreleased

## Summary

## Bugfixes

* #237: Fixed the DSN parser accepting a fingerprint placed after the port (e.g. `localhost:8563/1234`) as part of the hostname

## Refactoring

* #354: Updated to `exasol-toolbox` 10.4.0
* #723: Added integration-test coverage for Exasol 2025.1.11 and 2026.1.0
* #238: Updated tests to run with Python 3.14 as exasol-integration-test-docker-environment supports it
* #357: Enacted mypy checks for examples and tests
