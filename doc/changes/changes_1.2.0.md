# 1.2.0 - 2025-09-26

This release adds support to import from & export to local parquet file(s). For more details,
check out their example usage on the [Importing and Exporting Data](https://exasol.github.io/pyexasol/master/user_guide/exploring_features/import_and_export/index.html#parquet>)
page of the User Guide.

## Features

* #208: Added `ExaConnection.import_from_parquet` which can import data from local parquet file(s)
* #234: Added `ExaConnection.export_to_parquet` which can export data to local parquet file(s)
* #254: Added performance tests for http_transport export & import methods

## Dependency Updates

### `main`
* Removed dependency `numpy:1.26.4`
* Added dependency `pyarrow:20.0.0`

### `performance`
* Added dependency `pytest-benchmark:5.1.0`
