# 3.0.0 - 2026-10-16

## Summary

In this major release, the following changes were made:

* The Exasol telemetry tracking -- anonymized usage tracking of the PyExasol library --
  was added.
* To align more closely with PEP 249, the WebSocket DBAPI now returns values as
  Python types by default whenever possible. Some values were already returned
  as Python types before version 3.0; version 3.0 made this behavior more
  consistent:

  | Exasol type                        | Version < 3.0     | Version >= 3.0        |
  |------------------------------------|-------------------|-----------------------|
  | ``BOOLEAN``                        | ``bool``          | ``bool``              |
  | ``CHAR``                           | ``str``           | ``str``               |
  | ``DATE``                           | ``datetime.date`` | ``datetime.date``     |
  | ``DECIMAL(p, 0)``                  | ``int``           | ``int``               |
  | ``DECIMAL(p, s)`` where ``s > 0``  | ``str``           | ``decimal.Decimal``   |
  | ``DOUBLE``                         | ``float``         | ``float``             |
  | ``GEOMETRY``                       | ``str``           | ``str``               |
  | ``HASHTYPE``                       | ``str``           | ``str``               |
  | ``INTERVAL DAY TO SECOND``         | ``str``           | ``ExaTimeDelta``      |
  | ``INTERVAL YEAR TO MONTH``         | ``str``           | ``str``               |
  | ``TIMESTAMP``                      | ``str``           | ``datetime.datetime`` |
  | ``TIMESTAMP WITH LOCAL TIME ZONE`` | ``str``           | ``datetime.datetime`` |
  | ``VARCHAR``                        | ``str``           | ``str``               |

    * Nanosecond precision in ``TIMESTAMP`` values is truncated to Python
      microsecond precision.
    * ``NULL`` values continue to be returned as ``None`` for all types.
* WebSocket DBAPI executions now validate the ``NLS_DATE_FORMAT`` and
  ``NLS_TIMESTAMP_FORMAT`` EXA_PARAMETERS against the ISO-compatible formats
  supported by PyExasol, reporting all invalid parameters together with
  corrective ``ALTER SESSION`` guidance.
* WebSocket DBAPI connection operations that require an active connection now
  consistently reject calls made without one and translate underlying Exasol
  errors into the appropriate DBAPI exceptions.
* WebSocket result metadata types are now exposed as
  ``pyexasol.data_types.WebSocketDataType``; the public ``TypeCode`` alias
  remains available but displays using the canonical name.
* ``convert_websocket_to_python`` is the preferred name for converting
  WebSocket values to Python types; ``exasol_mapper`` remains available as a
  deprecated compatibility alias. ``ExaTimeDelta`` is also available from
  ``pyexasol.data_types``, while the old ``pyexasol.mapper`` compatibility
  module emits a deprecation warning on import.
* The deprecated ``pyexasol.db2`` compatibility layer was removed; the
  maintained ``exasol.driver.websocket.dbapi2`` interface remains available.

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| tornado | GHSA-chx6-46f5-w4vp | 6.5.8 | 6.5.9 |
| tornado | GHSA-c2m8-h5v5-343r | 6.5.8 | 6.5.9 |
| tornado | GHSA-3hv7-mjh2-fv65 | 6.5.8 | 6.5.9 |
| urllib3 | PYSEC-2026-4177 | 2.7.0 | 2.8.0 |
| urllib3 | PYSEC-2026-4176 | 2.7.0 | 2.8.0 |
| urllib3 | PYSEC-2026-4175 | 2.7.0 | 2.8.0 |
| virtualenv | PYSEC-2026-4011 | 21.7.8 | 21.7.12 |
| virtualenv | PYSEC-2026-4012 | 21.7.8 | 21.7.11 |
| virtualenv | PYSEC-2026-4014 | 21.7.8 | 21.7.12 |
| virtualenv | PYSEC-2026-4013 | 21.7.8 | 21.7.13 |
| virtualenv | PYSEC-2026-4011 | 21.7.8 | 21.7.12 |
| virtualenv | PYSEC-2026-4012 | 21.7.8 | 21.7.11 |
| virtualenv | PYSEC-2026-4013 | 21.7.8 | 21.7.13 |
| virtualenv | PYSEC-2026-4014 | 21.7.8 | 21.7.12 |

## Features

* #116: Added conversion support for ``TIMESTAMP WITH LOCAL TIME ZONE`` values
  in ``convert_websocket_to_python``.
* #375: Integrated telemetry

## Bugfixes

* #353: Fixed documentation inconsistencies in type hints and docstrings
* #409: Restricted allowed formats for TIMESTAMP and DATE to ISO-formats
* #415: Fixed a ``KeyError`` when accessing ``cursor.description`` for result
  sets containing ``HASHTYPE`` columns by adding ``HASHTYPE`` to ``TypeCode``
* #419: Fixed exact negative-day intervals in ``ExaTimeDelta`` being formatted with a
  day count one too low
* #429: Fixed ``Cursor.executemany`` silently accepting rows with more parameters
  than the prepared statement columns
* #436: Fixed WebSocket DBAPI result conversion by setting the default
  ``fetch_mapper`` to ``convert_websocket_to_python``

## Refactorings

* #409: Refactored connection method handling around ``_requires_connection``
  so connection checks and Exasol-to-DBAPI exception translation are applied
  consistently
* #415: Removed the ``pyexasol.db2`` compatibility module, which was
  [deprecated in July 2024](https://github.com/exasol/pyexasol/commit/8e3b361)
* #417: Moved DECIMAL, DATE, and TIMESTAMP conversion helpers into
  ``pyexasol.data_types.to_python`` and improved unit test coverage
* #238: Moved DSN parsing and connection tests from integration tests to unit tests, so
  they no longer require a running Docker database
* #419: Centralized WebSocket metadata types and moved mapper implementations
  into ``pyexasol.data_types``.
* #429: Centralized data-type test cases across unit and integration tests and
  renamed ``exasol_mapper`` to ``convert_websocket_to_python``
* #434: Refactored data-type test cases to share SQL templates and expected
  values across fetch-mapper and cursor integration tests

## Dependency Updates

### `main`

* Updated dependency `cryptography:50.0.1` to `50.0.2`
* Added dependency `exasol-telemetry-client:0.2.1`
* Updated dependency `polars:1.44.1` to `1.44.2`

### `dev`

* Updated dependency `exasol-toolbox:10.5.0` to `11.0.0`
