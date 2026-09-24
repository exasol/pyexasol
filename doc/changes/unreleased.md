# Unreleased

## Summary

In this major release, the WebSocket DBAPI improves session date/time format
handling and connection error behavior:

* WebSocket DBAPI executions now validate the ``NLS_DATE_FORMAT`` and
  ``NLS_TIMESTAMP_FORMAT`` EXA_PARAMETERS against the ISO-compatible formats
  supported by PyExasol, reporting all invalid parameters together with
  corrective ``ALTER SESSION`` guidance.
* WebSocket DBAPI connection operations that require an active connection now
  consistently reject calls made without one and translate underlying Exasol
  errors into the appropriate DBAPI exceptions.

## Bugfixes

* #353: Fixed documentation inconsistencies in type hints and docstrings
* #409: Restricted allowed formats for TIMESTAMP and DATE to ISO-formats

## Refactorings

* #409: Refactored connection method handling around ``_requires_connection``
  so connection checks and Exasol-to-DBAPI exception translation are applied
  consistently.
