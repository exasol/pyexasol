# Unreleased

This release drops support for Python 3.9, which reached its end-of-life in 2025-10.

## Bugfix

* #265: Improved usage of `with_column_names` in `export_params` from being a flag to being strictly boolean

## Refactoring

* #287: Removed unused imports (primarily in test files)
* #289: Dropped support for Python 3.9
* #294: Updated the exasol-integration-test-docker-environment to 5.0.0
* #298: Switched rsa (deprecated) with cryptography, added py.typed file, and widened constraints for pyarrow

## Internal

* #283: Updated the exasol-toolbox to 3.0.0
* #294: Updated the exasol-toolbox to 4.0.0
