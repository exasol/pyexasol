# Unreleased

## Summary

This release fixes WebSocket DBAPI fetching of ``INTERVAL DAY TO SECOND``
values whose declared day precision is below nine, including the default
``DAY(2)``. ``ExaTimeDelta.from_interval``, used by the default
``convert_websocket_to_python`` fetch mapper, now parses the variable-width day
field instead of assuming a nine-digit field. Interval values at all supported
day precisions are consequently returned as ``ExaTimeDelta`` instances rather
than raising ``ValueError``.

## Bugfixes

* #441: Fixed parsing of ``INTERVAL DAY TO SECOND`` values with variable day precision, preventing failures when fetching default-precision interval columns through the DBAPI.

## Refactorings

* #439: Resolved Sonar warnings for various tests
