import datetime

from pyexasol.data_types.converters import ExaTimeDelta


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
    * datetime.datetime  -> str in the format ``YYYY-MM-DD HH:MM:SS[.ffffff]``
                            (timezone-aware values are converted to UTC and written
                            without an offset, because the WebSocket string can't
                            carry one)

    Raises:
    * TypeError -> any other type
    """
    if value is None or isinstance(value, str):
        return value
    if not isinstance(value, datetime.datetime):
        raise TypeError(
            f"Expected datetime.datetime, str, or None, got {type(value).__name__}."
        )

    if value.utcoffset() is not None:
        value = value.astimezone(datetime.UTC).replace(tzinfo=None)
    return value.isoformat(sep=" ")


def interval_day_to_second_to_websocket(
    value: datetime.timedelta | ExaTimeDelta | str | None,
) -> str | None:
    """
    Convert a Python value to a type that the WebSocket API allows for an
    INTERVAL DAY TO SECOND.

    The value is converted with ``ExaTimeDelta.to_interval_unpadded()`` to the
    format ``[+-]D HH:MM:SS[.f..f]``. This function otherwise does not round, truncate
    or check the column's precision; whether the value fits is left to the database.

    The database truncates INTERVAL DAY TO SECOND values to milliseconds, even if the
    column is declared with a higher fractional-second precision. For example,
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
            f"Expected datetime.timedelta, str, ExaTimeDelta, or None, got {type(value).__name__}."
        )
    return ExaTimeDelta.from_timedelta(value).to_interval_unpadded()
