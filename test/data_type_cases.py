import datetime
import decimal
from dataclasses import (
    dataclass,
    field,
)
from typing import Any

import pytest

from pyexasol.data_types import WebSocketDataType
from pyexasol.data_types.converters import ExaTimeDelta


@dataclass(frozen=True)
class DataTypeCase:
    id: str
    websocket_data_type: WebSocketDataType
    websocket_value: Any
    python_value: Any
    scale: int | None = field(default=None, kw_only=True)


@dataclass(frozen=True)
class SqlDataTypeCase(DataTypeCase):
    sql_value: Any
    sql_template: str


def to_pytest_params(
    cases: tuple[DataTypeCase, ...],
) -> tuple:
    """Convert data type cases into pytest parameters using their IDs."""
    return tuple(pytest.param(case, id=case.id) for case in cases)


class DataTypeCases:
    @property
    def boolean(self) -> SqlDataTypeCase:
        shared_value = True
        return SqlDataTypeCase(
            id="boolean",
            websocket_data_type=WebSocketDataType.Bool,
            websocket_value=shared_value,
            python_value=shared_value,
            sql_value="TRUE",
            sql_template="SELECT CAST({value} AS BOOLEAN);",
        )

    @property
    def char(self) -> SqlDataTypeCase:
        shared_value = "text"
        return SqlDataTypeCase(
            id="char",
            websocket_data_type=WebSocketDataType.Char,
            websocket_value=shared_value,
            python_value=shared_value,
            sql_value=shared_value,
            sql_template="SELECT CAST({value} AS CHAR(4));",
        )

    @property
    def date(self) -> SqlDataTypeCase:
        shared_value = "2026-09-11"
        return SqlDataTypeCase(
            id="date",
            websocket_data_type=WebSocketDataType.Date,
            websocket_value=shared_value,
            python_value=datetime.date(2026, 9, 11),
            sql_value=shared_value,
            sql_template="SELECT DATE {value};",
        )

    @property
    def decimal_scale_0(self) -> SqlDataTypeCase:
        shared_value = 123
        return SqlDataTypeCase(
            id="decimal_scale_0",
            websocket_data_type=WebSocketDataType.Decimal,
            websocket_value=shared_value,
            python_value=shared_value,
            scale=0,
            sql_value=shared_value,
            sql_template="SELECT CAST({value} AS DECIMAL(18, 0));",
        )

    @property
    def decimal_scale_2(self) -> SqlDataTypeCase:
        shared_value = "123.45"
        return SqlDataTypeCase(
            id="decimal_scale_2",
            websocket_data_type=WebSocketDataType.Decimal,
            websocket_value=shared_value,
            python_value=decimal.Decimal("123.45"),
            scale=2,
            sql_value=shared_value,
            sql_template="SELECT CAST({value} AS DECIMAL(18, 2));",
        )

    @property
    def double(self) -> SqlDataTypeCase:
        shared_value = 1.25
        return SqlDataTypeCase(
            id="double",
            websocket_data_type=WebSocketDataType.Double,
            websocket_value=shared_value,
            python_value=shared_value,
            sql_value=shared_value,
            sql_template="SELECT CAST({value} AS DOUBLE PRECISION);",
        )

    @property
    def geometry(self) -> SqlDataTypeCase:
        shared_value = "POINT (10 20)"
        return SqlDataTypeCase(
            id="geometry",
            websocket_data_type=WebSocketDataType.Geometry,
            websocket_value=shared_value,
            python_value=shared_value,
            sql_value=shared_value,
            sql_template="SELECT CAST({value} AS GEOMETRY);",
        )

    @property
    def hashtype(self) -> SqlDataTypeCase:
        shared_value = "550e8400e29b11d4a716446655440000"
        return SqlDataTypeCase(
            id="hashtype",
            websocket_data_type=WebSocketDataType.Hashtype,
            websocket_value=shared_value,
            python_value=shared_value,
            sql_value=shared_value,
            sql_template="SELECT CAST({value} AS HASHTYPE(16 BYTE));",
        )

    @property
    def interval_day_to_second(self) -> SqlDataTypeCase:
        shared_value = "+000000003 10:59:59.123000000"
        return SqlDataTypeCase(
            id="interval_day_to_second",
            websocket_data_type=WebSocketDataType.IntervalDayToSecond,
            websocket_value=shared_value,
            python_value=ExaTimeDelta(
                days=3,
                hours=10,
                minutes=59,
                seconds=59,
                microseconds=123000,
            ),
            sql_value=shared_value,
            sql_template="SELECT INTERVAL {value} DAY(9) TO SECOND(9);",
        )

    @property
    def interval_year_to_month(self) -> SqlDataTypeCase:
        shared_value = "+000000001-02"
        return SqlDataTypeCase(
            id="interval_year_to_month",
            websocket_data_type=WebSocketDataType.IntervalYearToMonth,
            websocket_value=shared_value,
            python_value=shared_value,
            sql_value=shared_value,
            sql_template="SELECT INTERVAL {value} YEAR(9) TO MONTH;",
        )

    @property
    def timestamp(self) -> SqlDataTypeCase:
        shared_value = "2026-09-11 12:34:56.000001"
        return SqlDataTypeCase(
            id="timestamp",
            websocket_data_type=WebSocketDataType.Timestamp,
            websocket_value=shared_value,
            python_value=datetime.datetime(2026, 9, 11, 12, 34, 56, 1),
            sql_value=shared_value,
            sql_template="SELECT TIMESTAMP {value};",
        )

    @property
    def timestamp_with_local_time_zone(self) -> SqlDataTypeCase:
        shared_value = "2026-09-11 12:34:56.000000"
        return SqlDataTypeCase(
            id="timestamp_with_local_time_zone",
            websocket_data_type=WebSocketDataType.TimestampTz,
            websocket_value=shared_value,
            # TIMESTAMP WITH LOCAL TIME ZONE is currently not supported and is
            # passed through as a string. This will change with:
            # https://github.com/exasol/pyexasol/issues/116
            python_value=shared_value,
            sql_value=shared_value,
            sql_template=("SELECT CAST({value} AS TIMESTAMP WITH LOCAL TIME ZONE);"),
        )

    @property
    def varchar(self) -> SqlDataTypeCase:
        shared_value = "text"
        return SqlDataTypeCase(
            id="varchar",
            websocket_data_type=WebSocketDataType.String,
            websocket_value=shared_value,
            python_value=shared_value,
            sql_value=shared_value,
            sql_template="SELECT CAST({value} AS VARCHAR(4));",
        )

    def all_cases(self) -> tuple[SqlDataTypeCase, ...]:
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


DATA_TYPE_CASES = DataTypeCases()
