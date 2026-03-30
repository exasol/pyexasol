# 2.2.0 - 2026-03-30

## Summary

This update adds a method to create prepared statements. For more details,
check out their example usage on the [Examples](https://exasol.github.io/pyexasol/master/user_guide/exploring_features/examples.html#prepared-statements)
page of the User Guide.

The `poetry.lock` was updated to handle vulnerable main & transitive dependencies.
To ensure usage of secure packages, it is up to the user to similarly relock their
dependencies.

## Feature

* #324: Added `ExaConnection.create_prepared_statement`

## Refactoring

* Relocked `poetry.lock` to resolve vulnerable main dependencies:
  * `cryptography:46.0.5` to `46.0.6` for CVE-2026-34073
  * `orjson:3.11.5` to `3.11.7` for CVE-2025-67221
  * `ujson:5.11.0` to `5.12.0` for CVE-2026-32874 and CVE-2026-32875
* Relocked `poetry.lock` to resolve vulnerable transitive dependency:
  * `requests:2.32.5` to `2.33.0` for CVE-2026-25645
  * `tornado:6.5.4` to `6.5.5` for GHSA-78cv-mqj4-43f7 and CVE-2026-25645
* Changed `exasol-toolbox:6.0.0` to `6.1.1` to resolve vulnerable transitive dependency:
  * `black:25.12.0` to `26.3.1` for CVE-2026-32274

## Dependency Updates

### `main`

* Updated dependency `cryptography:46.0.5` to `46.0.6`
* Updated dependency `orjson:3.11.5` to `3.11.7`
* Updated dependency `ujson:5.11.0` to `5.12.0`

### `dev`

* Updated dependency `exasol-toolbox:6.0.0` to `6.1.1`
