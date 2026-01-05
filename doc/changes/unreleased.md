# Unreleased

This major release:
* drops support for Python 3.9, which reached its end-of-life in 2025-10.
* adds a py.typed file, so that type hints can be propagated from PyExasol
* fixes how `with_column_names` in `export_params` behaves.  Prior to this version,
if `with_column_names` was passed in the `export_params`, then it was interpreted as
`True` regardless of what value was paired with it. In this version, this has been
altered so that the expected value must be a boolean. For further information
on the current usage, see the [export_params](https://exasol.github.io/pyexasol/master/user_guide/exploring_features/import_and_export/parameters.html#export-params)
documentation.

## Bugfix

* #265: Improved usage of `with_column_names` in `export_params` from being a flag to being strictly boolean

## Refactoring

* #287: Removed unused imports (primarily in test files)
* #289: Dropped support for Python 3.9
* #294: Updated the exasol-integration-test-docker-environment to 5.0.0
* #298: Switched rsa (deprecated) with cryptography, added py.typed file, and widened constraints for pyarrow
* #117: Restricted versions for packaging, python-rapidjson, ujson, and orjson from without bounds to be reasonably bounded.

## Internal

* #283: Updated the exasol-toolbox to 3.0.0
* #294: Updated the exasol-toolbox to 4.0.0
