import datetime
import decimal
from typing import (
    Any,
    NamedTuple,
)

import pytest

from pyexasol.data_types import WebSocketDataType
from pyexasol.data_types.converters import ExaTimeDelta


class DataTypeCase(NamedTuple):
    id: str
    websocket_data_type: WebSocketDataType
    websocket_value: Any
    python_value: Any
    scale: int | None = None


def to_pytest_params(
    cases: tuple[DataTypeCase, ...],
) -> tuple:
    """Convert data type cases into pytest parameters using their IDs."""
    return tuple(pytest.param(case, id=case.id) for case in cases)


class DataTypeCases:
    @property
    def boolean(self) -> DataTypeCase:
        return DataTypeCase(
            id="boolean",
            websocket_data_type=WebSocketDataType.Bool,
            websocket_value=True,
            python_value=True,
        )

    @property
    def char(self) -> DataTypeCase:
        return DataTypeCase(
            id="char",
            websocket_data_type=WebSocketDataType.Char,
            websocket_value="text",
            python_value="text",
        )

    @property
    def date(self) -> DataTypeCase:
        return DataTypeCase(
            id="date",
            websocket_data_type=WebSocketDataType.Date,
            websocket_value="2026-09-11",
            python_value=datetime.date(2026, 9, 11),
        )

    @property
    def decimal_scale_0(self) -> DataTypeCase:
        return DataTypeCase(
            id="decimal_scale_0",
            websocket_data_type=WebSocketDataType.Decimal,
            websocket_value=123,
            python_value=123,
            scale=0,
        )

    @property
    def decimal_scale_2(self) -> DataTypeCase:
        return DataTypeCase(
            id="decimal_scale_2",
            websocket_data_type=WebSocketDataType.Decimal,
            websocket_value="123.45",
            python_value=decimal.Decimal("123.45"),
            scale=2,
        )

    @property
    def double(self) -> DataTypeCase:
        return DataTypeCase(
            id="double",
            websocket_data_type=WebSocketDataType.Double,
            websocket_value=1.25,
            python_value=1.25,
        )

    @property
    def geometry(self) -> DataTypeCase:
        return DataTypeCase(
            id="geometry",
            websocket_data_type=WebSocketDataType.Geometry,
            websocket_value="POINT (10 20)",
            python_value="POINT (10 20)",
        )

    @property
    def hashtype(self) -> DataTypeCase:
        return DataTypeCase(
            id="hashtype",
            websocket_data_type=WebSocketDataType.Hashtype,
            websocket_value="550e8400e29b11d4a716446655440000",
            python_value="550e8400e29b11d4a716446655440000",
        )

    @property
    def interval_day_to_second(self) -> DataTypeCase:
        return DataTypeCase(
            id="interval_day_to_second",
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
            id="interval_year_to_month",
            websocket_data_type=WebSocketDataType.IntervalYearToMonth,
            websocket_value="+000000001-02",
            python_value="+000000001-02",
        )

    @property
    def timestamp(self) -> DataTypeCase:
        return DataTypeCase(
            id="timestamp",
            websocket_data_type=WebSocketDataType.Timestamp,
            websocket_value="2026-09-11 12:34:56",
            python_value=datetime.datetime(2026, 9, 11, 12, 34, 56),
        )

    @property
    def timestamp_with_local_time_zone(self) -> DataTypeCase:
        return DataTypeCase(
            id="timestamp_with_local_time_zone",
            websocket_data_type=WebSocketDataType.TimestampTz,
            websocket_value="2026-09-11 12:34:56.000000",
            # TIMESTAMP WITH LOCAL TIME ZONE is currently not supported and is
            # passed through as a string. This will change with:
            # https://github.com/exasol/pyexasol/issues/116
            python_value="2026-09-11 12:34:56.000000",
        )

    @property
    def varchar(self) -> DataTypeCase:
        return DataTypeCase(
            id="varchar",
            websocket_data_type=WebSocketDataType.String,
            websocket_value="text",
            python_value="text",
        )

    def all_cases(self) -> tuple[DataTypeCase, ...]:
        return (
            self.boolean,
            self.char,
            self.date,
            self.decimal_scale_0,
            self.decimal_scale_2,
            self.double,
            self.geometry,
            self.hashtype,
            self.interval_day_to_second,
            self.interval_year_to_month,
            self.timestamp,
            self.timestamp_with_local_time_zone,
            self.varchar,
        )
