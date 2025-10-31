# 1.2.1 - 2025-10-31
This release fixes the `export_to_*` functions to properly handle delimit in the
generated EXPORT statement. Previously, this would have resulted in an exception being
tossed due to improper formatting.

Additionally, the `export_to_parquet` has been modified to handle new line characters,
as well as to preserve the order of the data. Previously, new line characters would
have resulted in an exception being tossed, and the order of exported data could
have differed depending upon pyarrow's processing of the data.

## Bugfix

* #263: Fixed usage of `delimit` in `export_to_*` functions
* #267: Fixed `export_to_parquet` to handle new lines & preserve order

## Documentation

* #260: Fixed examples for `import_from_parquet` to have balanced brackets

## Internal

* #271: Re-locked dependencies to resolve CVE-2025-8869 for transitive dependency pip

## Dependency Updates

### `main`
* Updated dependency `cryptography:45.0.7` to `46.0.3`
* Updated dependency `orjson:3.11.2` to `3.11.4`
* Updated dependency `pandas:2.3.1` to `2.3.3`
* Updated dependency `polars:1.32.2` to `1.34.0`
* Updated dependency `pyarrow:20.0.0` to `21.0.0`
* Updated dependency `python-rapidjson:1.21` to `1.22`
* Updated dependency `ujson:5.10.0` to `5.11.0`

### `dev`
* Updated dependency `exasol-integration-test-docker-environment:4.2.0` to `4.3.0`
* Updated dependency `exasol-toolbox:1.9.0` to `1.12.0`
* Updated dependency `pytest:8.4.1` to `8.4.2`
* Added dependency `pytest-repeat:0.9.4`
* Updated dependency `types-ujson:5.10.0.20250326` to `5.10.0.20250822`
