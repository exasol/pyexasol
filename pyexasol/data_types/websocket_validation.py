from __future__ import annotations

from pyexasol.data_types.websocket_types import WebSocketDataType


def _require_types(
    value: object,
    data_type: str,
    *allowed: type,
    is_unwanted_bool: bool = True,
) -> None:
    """
    Raise TypeError unless ``value`` is an instance of one of the ``allowed`` types.

    ``bool`` is a subclass of ``int``, so it is rejected by default, even if ``int`` is
    allowed. Only the BOOLEAN case sets ``is_unwanted_bool`` to False.
    """
    if (is_unwanted_bool and isinstance(value, bool)) or not isinstance(value, allowed):
        names = ", ".join(t.__name__ for t in allowed)
        raise TypeError(
            f"Expected {names} or None for {data_type} got {type(value).__name__}."
        )


def validate_value_for_websocket(value: object, data_type: str) -> None:
    """
    Check that a value has a type the WebSocket API accepts as input for a column.

    Only the Python type is checked. The content, such as the format, length or range,
    is left to the database.

    ``None`` and the empty string are accepted for every type, as the database stores
    both as NULL. The list below shows the other accepted types.

    BOOLEAN                          -> bool
    CHAR                             -> str
    DATE                             -> str
    DECIMAL                          -> str, int, float
    DOUBLE                           -> float
    GEOMETRY                         -> str
    HASHTYPE                         -> str
    INTERVAL DAY TO SECOND           -> str
    INTERVAL YEAR TO MONTH           -> str
    TIMESTAMP                        -> str
    TIMESTAMP WITH LOCAL TIME ZONE   -> str
    VARCHAR                          -> str
    <others>                         -> not validated

    Args:
        value: the value as it would be sent to the database.
        data_type: the ``dataType["type"]`` string the database reported for the
                   parameter, a ``WebSocketDataType`` value. A string that is not a
                   ``WebSocketDataType`` value is not validated, so a type that Exasol
                   adds later keeps working.

    Raises:
        TypeError -> ``data_type`` is not a ``str``
                  -> the value is neither ``None`` nor of an accepted type.
    """
    if not isinstance(data_type, str):
        raise TypeError(
            "Expected data_type to be a str, such as dataType['type'], "
            f"got {type(data_type).__name__}."
        )
    if value is None or (isinstance(value, str) and value == ""):
        return
    elif data_type == WebSocketDataType.Bool.value:
        _require_types(value, data_type, bool, is_unwanted_bool=False)
    elif data_type == WebSocketDataType.Decimal.value:
        _require_types(value, data_type, str, int, float)
    elif data_type == WebSocketDataType.Double.value:
        _require_types(value, data_type, float)
    elif data_type in (
        WebSocketDataType.Char.value,
        WebSocketDataType.Date.value,
        WebSocketDataType.Geometry.value,
        WebSocketDataType.Hashtype.value,
        WebSocketDataType.IntervalDayToSecond.value,
        WebSocketDataType.IntervalYearToMonth.value,
        WebSocketDataType.Timestamp.value,
        WebSocketDataType.TimestampTz.value,
        WebSocketDataType.String.value,
    ):
        _require_types(value, data_type, str)
