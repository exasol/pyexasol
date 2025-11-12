# Unreleased

## Documentation

* 279: Adapted documentation relating to `export_to_parquet` and HTTP transport methods:
   * Added example exceptions from using `export_to_parquet` when the default is not overridden and the specified destination directory is not empty
   * Clarified that the HTTP transport methods utilize three threads so users may see different exceptions based on timing
