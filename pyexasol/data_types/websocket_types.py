"""Data type names used by the Exasol WebSocket protocol."""

from enum import Enum


class WebSocketDataType(Enum):
    """
    Exasol data type names returned in WebSocket result-set metadata.

    These are protocol type names, not the complete set of SQL type aliases.
    Type parameters such as precision, scale, and size are returned as
    separate metadata properties.

    See: https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV3.md#data-types-type-names-and-properties
    """

    Bool = "BOOLEAN"
    Char = "CHAR"
    Date = "DATE"
    Decimal = "DECIMAL"
    Double = "DOUBLE"
    Geometry = "GEOMETRY"
    Hashtype = "HASHTYPE"
    IntervalDayToSecond = "INTERVAL DAY TO SECOND"
    IntervalYearToMonth = "INTERVAL YEAR TO MONTH"
    Timestamp = "TIMESTAMP"
    TimestampTz = "TIMESTAMP WITH LOCAL TIME ZONE"
    String = "VARCHAR"
