# Unreleased

## Summary

In this major release, the following changes were made:

* The WebSocket DBAPI `executemany` now converts and validates parameter values
  based on the target column type via the new `insert_mapper` parameter of
  `Connection`. The default, `convert_python_to_websocket`, accepts the Python
  types below per column type:

  | Exasol type                        | Version < 4.0                                           | Version >= 4.0                                                  |
  |------------------------------------|---------------------------------------------------------|-----------------------------------------------------------------|
  | ``BOOLEAN``                        | ``bool``                                                | ``bool``, ``int`` (``0``/``1``), ``str`` (e.g. ``'true'``)      |
  | ``CHAR``                           | ``str``                                                 | ``str``                                                         |
  | ``DATE``                           | ``datetime.date``, ``datetime.datetime``, ``str``       | ``datetime.date``, naive ``datetime.datetime``, ``str``         |
  | ``DECIMAL(p, s)``                  | ``decimal.Decimal``, ``float``, ``int``, ``str``        | ``decimal.Decimal``, ``float``, ``int``, ``str``                |
  | ``DOUBLE``                         | any value accepted by ``float()``                       | ``float``, ``int``, ``decimal.Decimal``, ``str`` (parsable)     |
  | ``GEOMETRY``                       | ``str``                                                 | ``str``                                                         |
  | ``HASHTYPE``                       | ``str``                                                 | ``str``                                                         |
  | ``INTERVAL DAY TO SECOND``         | ``str``                                                 | ``datetime.timedelta``, ``ExaTimeDelta``, ``str``               |
  | ``INTERVAL YEAR TO MONTH``         | ``str``                                                 | ``str``                                                         |
  | ``TIMESTAMP``                      | ``datetime.datetime``, ``str``                          | naive ``datetime.datetime``, ``str``                            |
  | ``TIMESTAMP WITH LOCAL TIME ZONE`` | ``datetime.datetime``, ``str``                          | naive ``datetime.datetime``, ``str``                            |
  | ``VARCHAR``                        | any value (converted with ``str()``)                    | ``str``                                                         |

    * ``None`` is accepted for all types.
    * With the default `insert_mapper`, values of a type that is not accepted raise
      a ``DataError``. Previously, the database rejected such values, but it was
      not always clear which row and column entry caused the error.
    * Only the types are validated. Precision, scale and length are not checked
      and remain the responsibility of the database.
    * With the default `insert_mapper`, a naive ``datetime.datetime`` is assumed to
      be exactly the time the database should store and is inserted in ISO format.
      Timezone-aware ``datetime.datetime`` values for ``DATE``, ``TIMESTAMP`` and
      ``TIMESTAMP WITH LOCAL TIME ZONE`` columns raise a ``DataError``, because
      these columns have no timezone and the offset cannot be sent. Convert the
      value explicitly first, e.g. with
      ``value.astimezone(datetime.UTC).replace(tzinfo=None)`` for a ``TIMESTAMP``.
    * The default `insert_mapper` also requires the session's ``NLS_DATE_FORMAT``
      and ``NLS_TIMESTAMP_FORMAT`` to be the ISO-compatible formats, and validates
      them in ``executemany``.
    * Customers can provide their own `insert_mapper` to `Connection` to change any
      of this behavior. With a custom `insert_mapper`, the session's date and
      timestamp formats are no longer validated, so they can be changed. It is then
      the customer's responsibility to ensure that the custom `insert_mapper`
      produces strings in the session's formats.
  * **Breaking change:** Values bound to a `VARCHAR` column must now be `str`
    (or `None`). Previously, `executemany` silently converted any value, such as
    `int`, `Decimal`, etc., to `str`. Such values now raise a `DataError`. This is
    intentional: stringifying arbitrary values can hide errors, such as a mismatch
    between the values and the columns they are  inserted into. Convert values
    explicitly with `str(...)` where a string is desired.

## Bugfixes

* #371: Fixed import_from_file returning misleading error message if wrong object type was passed
* #464: Fixed the DB API session date/time format validation depending on `fetch_mapper` when inserting. `execute` now validates only for the default `fetch_mapper`, and `executemany` only for the default `insert_mapper`

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
