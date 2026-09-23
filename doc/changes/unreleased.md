# Unreleased

## Summary

In this major release, the WebSocket DBAPI improves session date/time format
handling and connection error behavior:

* WebSocket DBAPI executions now validate the active ``NLS_DATE_FORMAT`` and
  ``NLS_TIMESTAMP_FORMAT`` values against the supported ISO-compatible formats,
  reporting all invalid parameters together with corrective ``ALTER SESSION``
  guidance.
* Connection methods consistently use the ``_requires_connection`` wrapper to
  reject operations without an active connection and translate underlying
  Exasol errors into the appropriate DBAPI exceptions.

## Bugfixes

* #353: Fixed documentation inconsistencies in type hints and docstrings
* #409: Restricted allowed formats for TIMESTAMP and DATE to ISO-formats

## Refactorings

* #409: Refactored connection method handling around ``_requires_connection``
  so connection checks and Exasol-to-DBAPI exception translation are applied
  consistently.
