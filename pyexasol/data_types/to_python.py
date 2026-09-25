"""
Helpers for converting values returned by Exasol into Python types.

PyExasol communicates with Exasol over the WebSocket protocol. WebSocket
responses are JSON-encoded and can be decoded into basic Python values, while
some Exasol values—such as DECIMAL, DATE, TIMESTAMP, and INTERVAL—require an
additional conversion to preserve their appropriate Python representation.
"""

import datetime
import decimal as decimal_module
from typing import Final

SUPPORTED_DATE_FORMATS: Final[list[str]] = ["YYYY-MM-DD"]


def convert_decimal(value: str, scale: int) -> int | decimal_module.Decimal:
    """
    Convert an Exasol DECIMAL value to the appropriate Python type.

    DECIMAL(p,0) -> int
    DECIMAL(p,s) -> decimal.Decimal
    """
    if scale == 0:
        return int(value)
    return decimal_module.Decimal(value)


def convert_date(value: str) -> datetime.date:
    """
    Convert an Exasol DATE value to ``datetime.date``.

    The expected value format is ``YYYY-MM-DD``. The value of the incoming string
    depends on the Exasol session's ``NLS_DATE_FORMAT`` EXA_PARAMETERS value.
    """
    return datetime.date.fromisoformat(value)
