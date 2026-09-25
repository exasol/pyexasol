"""
Helpers for converting values returned by Exasol into Python types.

PyExasol communicates with Exasol over the WebSocket protocol. WebSocket
responses are JSON-encoded and can be decoded into basic Python values, while
some Exasol values—such as DECIMAL, DATE, TIMESTAMP, and INTERVAL—require an
additional conversion to preserve their appropriate Python representation.
"""

import decimal as decimal_module


def convert_decimal(value: str, scale: int) -> int | decimal_module.Decimal:
    """
    Convert an Exasol DECIMAL value to the appropriate Python type.

    DECIMAL(p,0) -> int
    DECIMAL(p,s) -> decimal.Decimal
    """
    if scale == 0:
        return int(value)
    return decimal_module.Decimal(value)
