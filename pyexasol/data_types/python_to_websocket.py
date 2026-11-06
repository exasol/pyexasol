import datetime
import decimal

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
            f"Expected datetime.timedelta, str, ExaTimeDelta, or None, got {type(value).__name__}."
        )
    return ExaTimeDelta.from_timedelta(value).to_interval_unpadded()


def decimal_to_websocket(
    value: decimal.Decimal | int | float | str | None,
) -> str | None:
    """
    Convert a Python value to a type that the WebSocket API allows for a DECIMAL.

    We send numbers as strings in plain fixed-point notation because the server
    truncates a JSON number with many digits while handling it (for instance, a
    35-digit ``int`` keeps only 17 significant digits), but keeps every digit of
    a string.

    The stored value is governed by the target column's ``DECIMAL(p,s)``. If the
    value has more fractional digits than ``s``, the database truncates the extra
    digits silently. A value whose integer part does not fit ``p - s`` digits
    (precision minus scale) is rejected by the database with an error. Furthermore,
    the default database rejects strings like ``"abc"``, ``"1,5"``, ``"1_000"``,
    ``"NaN"`` and ``"Infinity"``.  The session's ``NLS_NUMERIC_CHARACTERS``
    EXA_PARAMETERS may change which decimal and group separators are accepted.

    Passed through unchanged:
     * None -> None
     * str  -> str

    Converted types:
    * decimal.Decimal  -> str
    * int              -> str
    * float            -> str

    Raises:
    * TypeError -> any other type
    """
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, decimal.Decimal):
        return f"{value:f}"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(int(value))
    if isinstance(value, float):
        # float.__repr__ also works for float subclasses such as numpy.float64,
        # whose own repr() is "np.float64(0.1)" and cannot be parsed by Decimal.
        return f"{decimal.Decimal(float.__repr__(value)):f}"
    raise TypeError(
        "Expected decimal.Decimal, int, float, str, or None, "
        f"got {type(value).__name__}."
    )


def double_to_websocket(
    value: float | int | decimal.Decimal | None,
) -> float | None:
    """
    Convert a Python value to a type that the WebSocket API allows for a DOUBLE.

    We send a JSON number, because the database rejects every non-empty string for
    a DOUBLE, even one such as ``"1.5"``. An ``int`` or ``decimal.Decimal`` is
    converted to a ``float`` first. Otherwise, the database keeps only the first 17
    significant digits of a large ``int``.

    Unlike DECIMAL, the session's ``NLS_NUMERIC_CHARACTERS`` has no effect.
    ``NaN`` and ``Infinity`` are passed through unchanged.

    Passed through unchanged:
    * None -> None

    Converted types:
    * float            -> float
    * int              -> float
    * decimal.Decimal  -> float

    Raises:
    * TypeError      -> any other type
    * OverflowError  -> an int too large for a double, such as ``10**400``
    """
    if value is None:
        return None
    if not isinstance(value, bool):
        if isinstance(value, (float, int, decimal.Decimal)):
            return float(value)
    raise TypeError(
        f"Expected float, int, decimal.Decimal, or None, got {type(value).__name__}."
    )
