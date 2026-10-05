import datetime
import decimal

import pytest

from pyexasol.data_types import WebSocketDataType
from pyexasol.data_types.converters import ExaTimeDelta
from pyexasol.data_types.websocket_to_python import (
    SUPPORTED_TIMESTAMP_FORMATS,
    convert_date,
    convert_decimal,
    convert_timestamp,
    exasol_mapper,
)


@pytest.mark.parametrize(
    "value, scale, expected",
    [
        pytest.param("123", 0, 123, id="zero-scale"),
        pytest.param(
            "123.45",
            2,
            decimal.Decimal("123.45"),
            id="non-zero-scale",
        ),
    ],
)
def test_convert_decimal(value, scale, expected):
    assert convert_decimal(value, scale) == expected


def test_convert_date():
    expected_date = datetime.date(2026, 9, 11)
    value = expected_date.strftime("%Y-%m-%d")

    assert convert_date(value) == expected_date


def test_convert_timestamp_without_fractional_seconds():
    expected_timestamp = datetime.datetime(2026, 9, 11, 12, 34, 56)
    value = expected_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    assert convert_timestamp(value) == expected_timestamp


@pytest.mark.parametrize("timestamp_format", SUPPORTED_TIMESTAMP_FORMATS)
def test_convert_timestamp_supports_all_formats(timestamp_format):
    digits = int(timestamp_format.partition(".FF")[2] or 0)
    fraction_digits = "1" * digits
    fraction = f".{fraction_digits}" if digits else ""

    assert convert_timestamp(f"2026-09-11 12:34:56{fraction}")


@pytest.mark.parametrize(
    "fractional_value",
    [
        pytest.param(fractional_value, id=f"FF{len(fractional_value)}")
        for fractional_value in ("1", "12", "123", "1234", "12345", "123456")
    ],
)
def test_convert_timestamp_with_fractional_seconds(fractional_value):
    value = f"2026-09-11 12:34:56.{fractional_value}"

    assert convert_timestamp(value).time() == datetime.time(
        12, 34, 56, int(fractional_value.ljust(6, "0"))
    )


@pytest.mark.parametrize(
    "fractional_value",
    [
        pytest.param(fractional_value, id=f"FF{len(fractional_value)}")
        for fractional_value in ("1234567", "12345678", "123456789")
    ],
)
def test_convert_timestamp_truncates_nanoseconds(fractional_value):
    value = f"2026-09-11 12:34:56.{fractional_value}"

    assert convert_timestamp(value).time() == datetime.time(12, 34, 56, 123456)


MAPPER_CASES = [
    ("DECIMAL", "123", 0, 123),
    ("DECIMAL", "123.45", 2, decimal.Decimal("123.45")),
    (
        "DATE",
        "2026-09-11",
        0,
        datetime.date(2026, 9, 11),
    ),
    (
        "TIMESTAMP",
        "2026-09-11 12:34:56",
        0,
        datetime.datetime(2026, 9, 11, 12, 34, 56),
    ),
    (
        "INTERVAL DAY TO SECOND",
        "+000000003 10:59:59.123000000",
        0,
        ExaTimeDelta(days=3, hours=10, minutes=59, seconds=59, microseconds=123000),
    ),
    ("DOUBLE", 1.25, 0, 1.25),
    ("BOOLEAN", True, 0, True),
    ("VARCHAR", "text", 0, "text"),
    ("CHAR", "text", 0, "text"),
    ("HASHTYPE", "hash", 0, "hash"),
    ("GEOMETRY", "POINT (10 20)", 0, "POINT (10 20)"),
    ("INTERVAL YEAR TO MONTH", "+000000001-02", 0, "+000000001-02"),
    (
        "TIMESTAMP WITH LOCAL TIME ZONE",
        "2026-09-11 12:34:56",
        0,
        "2026-09-11 12:34:56",
    ),
]


class TestExasolMapper:
    @staticmethod
    @pytest.mark.parametrize(
        "type_name,value,scale,expected",
        MAPPER_CASES,
    )
    def test_maps_types(type_name, value, scale, expected):
        data_type = {"type": type_name, "scale": scale}
        assert exasol_mapper(value, data_type) == expected

    @staticmethod
    def test_maps_all_type_codes():
        tested_type_names = {type_name for type_name, _, _, _ in MAPPER_CASES}
        expected_type_names = {data_type.value for data_type in WebSocketDataType}

        assert tested_type_names == expected_type_names

    @staticmethod
    def test_maps_none():
        assert exasol_mapper(None, {"type": "DECIMAL", "scale": 0}) is None
