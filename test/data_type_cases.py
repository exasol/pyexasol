import datetime
import decimal
from typing import (
    Any,
    NamedTuple,
)

from pyexasol.data_types import WebSocketDataType
from pyexasol.data_types.converters import ExaTimeDelta


class DataTypeCase(NamedTuple):
    websocket_data_type: WebSocketDataType
    websocket_value: Any
    python_value: Any
    scale: int | None = None


class DataTypeCases:
    @property
    def boolean(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.Bool,
            websocket_value=True,
            python_value=True,
        )

    @property
    def char(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.Char,
            websocket_value="text",
            python_value="text",
        )

    @property
    def date(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.Date,
            websocket_value="2026-09-11",
            python_value=datetime.date(2026, 9, 11),
        )

    @property
    def decimal_scale_0(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.Decimal,
            websocket_value=123,
            python_value=123,
            scale=0,
        )

    @property
    def decimal_scale_2(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.Decimal,
            websocket_value="123.45",
            python_value=decimal.Decimal("123.45"),
            scale=2,
        )

    @property
    def double(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.Double,
            websocket_value=1.25,
            python_value=1.25,
        )

    @property
    def geometry(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.Geometry,
            websocket_value="POINT (10 20)",
            python_value="POINT (10 20)",
        )

    @property
    def hashtype(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.Hashtype,
            websocket_value="550e8400e29b11d4a716446655440000",
            python_value="550e8400e29b11d4a716446655440000",
        )

    @property
    def interval_day_to_second(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.IntervalDayToSecond,
            websocket_value="+000000003 10:59:59.123000000",
            python_value=ExaTimeDelta(
                days=3,
                hours=10,
                minutes=59,
                seconds=59,
                microseconds=123000,
            ),
        )

    @property
    def interval_year_to_month(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.IntervalYearToMonth,
            websocket_value="+000000001-02",
            python_value="+000000001-02",
        )

    @property
    def timestamp(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.Timestamp,
            websocket_value="2026-09-11 12:34:56",
            python_value=datetime.datetime(2026, 9, 11, 12, 34, 56),
        )

    @property
    def timestamp_with_local_time_zone(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.TimestampTz,
            websocket_value="2026-09-11 12:34:56",
            # This will be changed with https://github.com/exasol/pyexasol/issues/116
            python_value="2026-09-11 12:34:56",
        )

    @property
    def varchar(self) -> DataTypeCase:
        return DataTypeCase(
            websocket_data_type=WebSocketDataType.String,
            websocket_value="text",
            python_value="text",
        )

    def all_cases(self) -> tuple[DataTypeCase, ...]:
        return tuple(
            getattr(self, name)
            for name, member in vars(type(self)).items()
            if isinstance(member, property)
        )
