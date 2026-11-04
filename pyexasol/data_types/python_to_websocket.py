import datetime


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
