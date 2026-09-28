"""
Helpers for converting values returned by Exasol into Python types.

PyExasol communicates with Exasol over the WebSocket protocol. WebSocket
responses are JSON-encoded and can be decoded into basic Python values, while
some Exasol values—such as DECIMAL, DATE, TIMESTAMP, and INTERVAL—require an
additional conversion to preserve their appropriate Python representation.
"""

import datetime
import decimal
from typing import Final

# These formats are strictly enforced by the WebSocket DBAPI. They also describe
# the ISO-only values that are parsed through these helpers. Keep them next to the
# parsers because this limitation applies to both consumers.
# Start: supported session date/time formats included in the DBAPI documentation.
SUPPORTED_DATE_FORMATS: Final[tuple[str, ...]] = ("YYYY-MM-DD",)
SUPPORTED_TIMESTAMP_FORMATS: Final[tuple[str, ...]] = (
    "YYYY-MM-DD HH24:MI:SS",
    "YYYY-MM-DD HH24:MI:SS.FF1",
    "YYYY-MM-DD HH24:MI:SS.FF2",
    "YYYY-MM-DD HH24:MI:SS.FF3",
    "YYYY-MM-DD HH24:MI:SS.FF4",
    "YYYY-MM-DD HH24:MI:SS.FF5",
    "YYYY-MM-DD HH24:MI:SS.FF6",
    "YYYY-MM-DD HH24:MI:SS.FF7",
    "YYYY-MM-DD HH24:MI:SS.FF8",
    "YYYY-MM-DD HH24:MI:SS.FF9",
)
# End: supported session date/time formats included in the DBAPI documentation.


def convert_decimal(value: str, scale: int) -> int | decimal.Decimal:
    """
    Convert an Exasol DECIMAL value to the appropriate Python type.

    DECIMAL(p,0) -> int
    DECIMAL(p,s) -> decimal.Decimal
    """
    if scale == 0:
        return int(value)
    return decimal.Decimal(value)


def convert_date(value: str) -> datetime.date:
    """
    Convert an Exasol DATE value to ``datetime.date``.

    The expected value format is ``YYYY-MM-DD``. The value of the incoming string
    depends on the Exasol session's ``NLS_DATE_FORMAT`` EXA_PARAMETERS value.
    """
    return datetime.date.fromisoformat(value)


def convert_timestamp(value: str) -> datetime.datetime:
    """
    Convert an Exasol TIMESTAMP value to ``datetime.datetime``.

    The expected value format is ``YYYY-MM-DD HH24:MI:SS`` with optional
    fractional seconds from ``FF1`` through ``FF9``. The value depends on the
    session's ``NLS_TIMESTAMP_FORMAT`` EXA_PARAMETERS value.
    Exasol supports optional fractional seconds with nanosecond precision;
    Python datetime values are truncated to microsecond precision.
    """
    # Python 3.10 is stricter about the fractional-second portion accepted by
    # datetime.fromisoformat(), while Exasol can return between 1 and 9 digits.
    # Normalize shorter values to six digits and truncate nanoseconds because
    # Python datetime only supports microsecond precision.
    timestamp_value = value
    if len(timestamp_value) > 19:
        date_and_time = timestamp_value[:19]
        fractional_seconds = timestamp_value[20:]
        microseconds = fractional_seconds[:6].ljust(6, "0")
        timestamp_value = f"{date_and_time}.{microseconds}"

    return datetime.datetime.fromisoformat(timestamp_value)
