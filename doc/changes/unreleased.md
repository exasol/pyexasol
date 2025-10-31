# Unreleased

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
