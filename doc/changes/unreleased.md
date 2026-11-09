# Unreleased

## Summary


## Bugfixes

* #371: Fixed import_from_file returning misleading error message if wrong object type was passed

## Features

* #448: Added default `timestamp_to_websocket` to convert Python values for Websocket insertion
* #452: Added default `interval_day_to_second_to_websocket` to convert Python `timedelta` and `ExaTimeDelta` values for Websocket insertion, and `ExaTimeDelta.to_interval_unpadded` to format intervals without zero-padding
* #454: Added default `date_to_websocket` to convert Python `date` values for Websocket insertion
* #457: Added default `decimal_to_websocket` and `double_to_websocket` to convert Python values for Websocket insertion
* #461: Added `validate_value_for_websocket` to check that Python values have a type the WebSocket API accepts for the target column before Websocket insertion

## Refactorings

* #444: Removed support for Python 3.10 and updated to exasol-toolbox version 12.0.0
* #451: Resolved Sonar findings by using literals instead of `list()` and `dict()` calls, and `list()` instead of list comprehensions in `fetchmany` and `fetchall`
* #452: Deprecated `ExaTimeDelta.to_interval`; use `ExaTimeDelta.to_interval_unpadded` instead. The string representation of `ExaTimeDelta` (`str()`) now uses `ExaTimeDelta.to_interval_unpadded` instead of the zero-padded format.
