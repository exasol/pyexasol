import datetime
import decimal
import math

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


def _reject_non_finite(value: float | decimal.Decimal, target: str) -> None:
    """
    Raise ValueError if ``value`` is NaN or infinite.

    The database does not accept these values for ``target``. Rejecting them here
    gives a clear error that names the value, and avoids a result that depends on the
    configured JSON library (e.g. orjson serializes NaN as ``null``).
    """
    finite = (
        value.is_finite()
        if isinstance(value, decimal.Decimal)
        else math.isfinite(value)
    )
    if not finite:
        raise ValueError(
            f"{value!r} is not supported for {target}. NaN and Infinity cannot be "
            "stored, so replace the value first, e.g. with None."
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
            "Expected datetime.datetime (if not timezone-aware), str, or None, "
            f"got {type(value).__name__}."
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
            "Expected datetime.date, datetime.datetime (if not timezone-aware), "
            f"str, or None, got {type(value).__name__}."
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
    ``"NaN"`` and ``"Infinity"``. The session's ``NLS_NUMERIC_CHARACTERS``
    EXA_PARAMETERS may change which decimal and group separators are accepted.

    Passed through unchanged:
     * None -> None
     * str  -> str

    Converted types:
    * decimal.Decimal  -> str
    * int              -> str
    * float            -> str

    Raises:
    * TypeError  -> any other type
    * ValueError -> a non-finite decimal.Decimal or float (NaN, sNaN, Infinity).
    """
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, decimal.Decimal):
        _reject_non_finite(value, "DECIMAL")
        return f"{value:f}"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(int(value))
    if isinstance(value, float):
        # float.__repr__ also works for float subclasses such as numpy.float64,
        # whose own repr() is "np.float64(0.1)" and cannot be parsed by Decimal.
        _reject_non_finite(value, "DECIMAL")
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

    Passed through unchanged:
    * None -> None

    Converted types:
    * float            -> float
    * int              -> float
    * decimal.Decimal  -> float

    Raises:
    * TypeError      -> any other type
    * OverflowError  -> an int too large for a double, such as ``10**400``
    * ValueError     -> NaN, sNaN or Infinity, including a decimal.Decimal too large
                        for a double, such as ``Decimal("1E+400")``
    """
    if value is None:
        return None
    if isinstance(value, decimal.Decimal):
        # Checked before float(), which raises its own ValueError for sNaN.
        _reject_non_finite(value, "DOUBLE")

    if not isinstance(value, bool) and isinstance(value, (float, int, decimal.Decimal)):
        result = float(value)
        _reject_non_finite(result, "DOUBLE")
        return result
    raise TypeError(
        f"Expected float, int, decimal.Decimal, or None, got {type(value).__name__}."
    )
