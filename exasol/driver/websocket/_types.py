"""
This module provides `PEP-249`_ a DBAPI compliant types and type conversion definitions.
(see also `PEP-249-types`_)

.. _PEP-249-types: https://peps.python.org/pep-0249/#type-objects-and-constructors
"""

from datetime import (
    date,
    datetime,
    time,
)
from time import localtime

from pyexasol.data_types.websocket_types import WebSocketDataType

TypeCode = WebSocketDataType

Date = date
Time = time
Timestamp = datetime


def DateFromTicks(ticks: int) -> date:  # pylint: disable=C0103
    """
    This function constructs an object holding a date value from the given ticks value
    (number of seconds since the epoch; see the documentation of the standard
    Python time module for details).
    """
    year, month, day = localtime(ticks)[:3]
    return Date(year, month, day)


def TimeFromTicks(ticks: int) -> time:  # pylint: disable=C0103
    """
    This function constructs an object holding a time value from the given ticks value
    (number of seconds since the epoch; see the documentation of the standard
    Python time module for details).
    """
    hour, minute, second = localtime(ticks)[3:6]
    return Time(hour, minute, second)


def TimestampFromTicks(ticks: int) -> datetime:  # pylint: disable=C0103
    """
    This function constructs an object holding a time stamp value from the
    given ticks value (number of seconds since the epoch; see the documentation
    of the standard Python time module for details).
    """
    year, month, day, hour, minute, second = localtime(ticks)[:6]
    return Timestamp(year, month, day, hour, minute, second)


class _DBAPITypeObject:
    def __init__(self, *type_codes) -> None:
        self.type_codes = type_codes

    def __eq__(self, other):
        return other in self.type_codes


STRING = _DBAPITypeObject(TypeCode.String)
# A binary type is not natively supported by Exasol
BINARY = _DBAPITypeObject(None)
NUMBER = _DBAPITypeObject(TypeCode.Decimal, TypeCode.Double)
DATETIME = _DBAPITypeObject(
    TypeCode.Date,
    TypeCode.Timestamp,
    TypeCode.TimestampTz,
    TypeCode.IntervalDayToSecond,
    TypeCode.IntervalYearToMonth,
)
# Exasol does manage indexes internally
ROWID = _DBAPITypeObject(None)
