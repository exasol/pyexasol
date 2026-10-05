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
* The deprecated ``pyexasol.mapper`` compatibility module now emits a
  deprecation warning on import; use ``pyexasol.data_types`` instead.
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
* #238: Moved DSN parsing tests from integration tests to unit tests, so they no longer require a running Docker database
* #419: Centralized WebSocket result metadata type names in
  ``pyexasol.data_types.websocket_types`` as ``WebSocketDataType``, retained
  the public ``TypeCode`` compatibility alias, moved ``exasol_mapper`` and
  ``ExaTimeDelta`` into ``pyexasol.data_types`` with compatibility exports
