# 4.0.0 - 2026-11-11

## Summary

In this major release, the following changes were made:

* The WebSocket DBAPI `executemany` now converts and validates parameter values
  based on the target column type via the new `insert_mapper` parameter of
  `Connection`. The default, `convert_python_to_websocket`, accepts only
  specific Python types per column type. See the accepted types in the
  ["Inserting Data" section](https://exasol.github.io/pyexasol/master/user_guide/exploring_features/advanced_topics/dbapi.html#inserting-data) of the DB-API documentation.
  * A value whose type is not accepted now raises a `DataError` that names the
    row, column and allowed types. Previously, the database rejected such values,
    but it was not always clear which entry caused the error.
  * The default `insert_mapper` requires the session's `NLS_DATE_FORMAT` and
    `NLS_TIMESTAMP_FORMAT` to be the supported ISO-compatible formats, and
    validates them in `executemany`.
  * Customers can provide their own `insert_mapper` to `Connection` to change how
    input values are converted, including which Python input types are accepted.
    * The mapper's output must still be a type accepted by
      `validate_value_for_websocket` for the column (e.g. `float` for `DOUBLE`,
      `str` for `VARCHAR`), otherwise a `DataError` is raised. This validation gives
      more specific type errors than the database would.
    * With a custom `insert_mapper`, `executemany` no longer validates the session's
      date and timestamp formats. `execute` still validates them unless a custom
      `fetch_mapper` is also provided, so both mappers must be custom for the
      formats to be changeable. It is then the customer's responsibility to ensure
      that the custom mappers support the session's formats.
  * **Breaking change:** Values bound to a `VARCHAR` column must now be `str`
    (or `None`). Previously, `executemany` silently converted any value, such as
    `int`, `Decimal`, etc., to `str`. Such values now raise a `DataError`. This is
    intentional: stringifying arbitrary values can hide errors, such as a mismatch
    between the values and the columns they are inserted into. Convert values
    explicitly with `str(...)` where a string is desired.

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| black | CVE-2026-114134 | 26.10.0 | 26.10.1 |
| certifi | PYSEC-2026-4320 | 2026.7.22 | 2026.10.18 |
| filelock | CVE-2026-112907 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112908 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112909 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112910 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112911 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112912 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112913 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112915 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112934 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112935 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112938 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112939 | 4.0.12 | 4.2.1 |
| filelock | CVE-2026-112940 | 4.0.12 | 4.2.1 |
| gitpython | CVE-2026-114145 | 3.2.0 | 3.2.1 |
| setuptools | GHSA-xmj3-9gf7-h52w | 84.0.0 | 84.1.0 |
| tornado | CVE-2026-115927 | 6.5.10 | 6.6 |
| tornado | CVE-2026-116577 | 6.5.10 | 6.6 |
| tornado | CVE-2026-116578 | 6.5.10 | 6.6 |
| virtualenv | CVE-2026-113714 | 21.14.5 | 21.14.9 |
| virtualenv | CVE-2026-113715 | 21.14.5 | 21.14.9 |
| virtualenv | CVE-2026-113717 | 21.14.5 | 21.14.9 |
| virtualenv | CVE-2026-113718 | 21.14.5 | 21.14.9 |
| virtualenv | CVE-2026-113720 | 21.14.5 | 21.14.9 |
| virtualenv | CVE-2026-113722 | 21.14.5 | 21.14.9 |
| virtualenv | CVE-2026-113723 | 21.14.5 | 21.14.9 |
| virtualenv | CVE-2026-113725 | 21.14.5 | 21.14.7 |

## Bugfixes

* #371: Fixed import_from_file returning misleading error message if wrong object type was passed
* #464: Fixed the DB API session date/time format validation depending on `fetch_mapper` when inserting. `execute` now validates only for the default `fetch_mapper`, and `executemany` only for the default `insert_mapper`

## Documentation

* #463: Documented the DB API `insert_mapper` and its default behavior, and grouped the `fetch_mapper` and `insert_mapper` documentation under "Fetching Data" and "Inserting Data"

## Features

* #448: Added `timestamp_to_websocket` to convert Python values for Websocket insertion
* #452: Added `interval_day_to_second_to_websocket` to convert Python `timedelta` and `ExaTimeDelta` values for Websocket insertion, and `ExaTimeDelta.to_interval_unpadded` to format intervals without zero-padding
* #454: Added `date_to_websocket` to convert Python `date` values for Websocket insertion
* #457: Added `decimal_to_websocket` and `double_to_websocket` to convert Python values for Websocket insertion
* #459: Added `boolean_to_websocket` to convert Python values for Websocket insertion
* #461: Added `validate_value_for_websocket` to check that Python values have a type the WebSocket API accepts for the target column before Websocket insertion
* #464:
  * Added `convert_python_to_websocket`, which converts Python values for Websocket insertion based on the target column type.
  * Added an `insert_mapper` parameter to the DB API `Connection` (defaulting to `convert_python_to_websocket`), which `executemany` uses to convert parameter values before insertion

## Refactorings

* #444: Removed support for Python 3.10 and updated to exasol-toolbox version 12.0.0
* #451: Resolved Sonar findings by using literals instead of `list()` and `dict()` calls, and `list()` instead of list comprehensions in `fetchmany` and `fetchall`
* #452: Deprecated `ExaTimeDelta.to_interval`; use `ExaTimeDelta.to_interval_unpadded` instead. The string representation of `ExaTimeDelta` (`str()`) now uses `ExaTimeDelta.to_interval_unpadded` instead of the zero-padded format.

## Dependency Updates

### `main`

* Updated dependency `orjson:3.12.0` to `3.13.0`
* Updated dependency `pyarrow:25.0.1` to `26.0.0`
* Updated dependency `pydantic:2.13.5` to `2.14.1`

### `dev`

* Updated dependency `exasol-toolbox:11.0.0` to `12.0.0`
