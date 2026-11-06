import datetime

from pyexasol.data_types.converters import ExaTimeDelta


def _reject_timezone_aware(value: datetime.datetime, target: str, hint: str) -> None:
    """
    Raise ValueError if ``value`` is timezone-aware.

    TIMESTAMP and DATE have no timezone and the WebSocket string can't carry an
    offset. Which instant or calendar date is meant depends on the timezone, so the
    caller must choose the conversion explicitly. ``hint`` is the target-specific
    expression suggested in the error message.
    """
    if value.utcoffset() is not None:
        raise ValueError(
            f"Timezone-aware datetime.datetime is not supported for {target}. "
            f"Convert it explicitly first, e.g. with {hint} for the UTC value."
        )


def timestamp_to_websocket(value: datetime.datetime | str | None) -> str | None:
    """
    Convert a Python value to a type that the WebSocket API allows for a TIMESTAMP.

    The WebSocket API parses TIMESTAMP input with the session's ``NLS_TIMESTAMP_FORMAT``
    EXA_PARAMETERS value. The ISO format used here, ``YYYY-MM-DD HH24:MI:SS.FF[1..9]``,
    is based on the Exasol default. Exasol supports optional fractional seconds with
    nanosecond precision; Python datetime values have microsecond precision.

    The stored value is governed by the target column's ``TIMESTAMP(p)`` precision.
    If the string has more fractional digits than ``p``, the database truncates the
    extra digits silently. If it has fewer, the database pads the value with zeros.
    This function does not truncate or validate precision on the Python side; any
    truncation is left to the database. A string with more than 9 fractional digits
    is not truncated but rejected by the database with an error.

    Passed through unchanged:
     * None -> None
     * str  -> str. The database rejects strings in an unsupported format.

    Converted types:
    * datetime.datetime  -> str in the format ``YYYY-MM-DD HH:MM:SS[.ffffff]``.
                            Only naive datetimes are supported.

    Raises:
    * ValueError -> timezone-aware datetime.datetime. A TIMESTAMP has no timezone and
                    the WebSocket string can't carry an offset, so the caller must
                    choose how to convert, e.g. ``value.astimezone(datetime.UTC)
                    .replace(tzinfo=None)``.
    * TypeError  -> any other type
    """
    if value is None or isinstance(value, str):
        return value
    if not isinstance(value, datetime.datetime):
        raise TypeError(
            f"Expected datetime.datetime, str, or None, got {type(value).__name__}."
        )

    _reject_timezone_aware(
        value, "TIMESTAMP", "value.astimezone(datetime.UTC).replace(tzinfo=None)"
    )
    return value.isoformat(sep=" ")


def date_to_websocket(value: datetime.date | str | None) -> str | None:
    """
    Convert a Python value to a type that the WebSocket API allows for a DATE.

    The WebSocket API parses DATE input with the session's ``NLS_DATE_FORMAT``
    EXA_PARAMETERS value. The ISO format used here, ``YYYY-MM-DD``, is the Exasol
    default. If the session format was changed, pass a string in that format instead.

    The stored and rejected values are determined by the database. The database rejects
    incorrect dates such as 2026-02-30. It accepts a string with a trailing time part
    and silently drops that part.

    Passed through unchanged:
     * None -> None
     * str  -> str. The database rejects strings in an unsupported format.

    Converted types:
    * datetime.date      -> str in the format ``YYYY-MM-DD``
    * datetime.datetime  -> str in the format ``YYYY-MM-DD HH:MM:SS[.ffffff]``. The
                            database drops the time part. Only naive values are
                            supported, see Raises.

    Raises:
    * TypeError  -> any other type
    * ValueError -> timezone-aware datetime.datetime. The intended date depends on the
                    timezone, so the caller must convert the value first, e.g.
                    ``value.astimezone(datetime.UTC).date()``.
    """
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, datetime.datetime):
        _reject_timezone_aware(value, "DATE", "value.astimezone(datetime.UTC).date()")
        return value.isoformat(sep=" ")
    if not isinstance(value, datetime.date):
        raise TypeError(
            f"Expected datetime.date, str, or None, got {type(value).__name__}."
        )
    return value.isoformat()


def interval_day_to_second_to_websocket(
    value: datetime.timedelta | ExaTimeDelta | str | None,
) -> str | None:
    """
    Convert a Python value to a type that the WebSocket API allows for an
    INTERVAL DAY TO SECOND.

    The value is converted with ``ExaTimeDelta.to_interval_unpadded()`` to the
    format ``[+-]D HH:MM:SS[.f..f]``.

    The database accepts the value only if it fits the ``INTERVAL DAY(p) TO SECOND(f)``
    column. Here, ``p`` is the number of digits in the day part and ``f`` the number
    of fractional digits. The total length of the string is ``p + f + 11`` characters
    with fractional seconds and ``p + 10`` characters without. The database also
    truncates INTERVAL DAY TO SECOND values to milliseconds, even if the column is
    declared with a higher fractional-second precision. For example,
    ``1 02:03:04.1234567`` is stored as ``+000000001 02:03:04.123000000``. See
    https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm#Intervaldatatypes

    Passed through unchanged:
     * None -> None
     * str  -> str. The database rejects strings in an unsupported format.

    Converted types:
    * datetime.timedelta  -> str in the format ``[+-]D HH:MM:SS[.f..f]``
    * ExaTimeDelta        -> str in the same format.

    Raises:
    * TypeError -> any other type
    """
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, ExaTimeDelta):
        return value.to_interval_unpadded()
    if not isinstance(value, datetime.timedelta):
        raise TypeError(
            "Expected datetime.timedelta, ExaTimeDelta, str, or None, "
            f"got {type(value).__name__}."
        )
    return ExaTimeDelta.from_timedelta(value).to_interval_unpadded()
