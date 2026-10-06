# Unreleased

## Summary

In this major release, the following changes were made:

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

## Bugfixes

* #353: Fixed documentation inconsistencies in type hints and docstrings
* #409: Restricted allowed formats for TIMESTAMP and DATE to ISO-formats
* #415: Fixed a ``KeyError`` when accessing ``cursor.description`` for result
  sets containing ``HASHTYPE`` columns by adding ``HASHTYPE`` to ``TypeCode``
* #419: Fixed exact negative-day intervals in ``ExaTimeDelta`` being formatted with a day count one too low

## Refactorings

* #409: Refactored connection method handling around ``_requires_connection``
  so connection checks and Exasol-to-DBAPI exception translation are applied
  consistently
* #415: Removed the ``pyexasol.db2`` compatibility module, which was
  [deprecated in July 2024](https://github.com/exasol/pyexasol/commit/8e3b361)
* #417: Moved DECIMAL, DATE, and TIMESTAMP conversion helpers into
  ``pyexasol.data_types.to_python`` and improved unit test coverage
* #238: Moved DSN parsing and connection tests from integration tests to unit tests, so they no longer require a running Docker database
* #419: Centralized WebSocket metadata types and moved mapper implementations
  into ``pyexasol.data_types``.
* #429: Centralized data-type test cases across unit and integration tests and
  renamed ``exasol_mapper`` to ``convert_websocket_to_python``
