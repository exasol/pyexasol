import datetime
import decimal

import pytest

from pyexasol.data_types.to_python import (
    convert_date,
    convert_decimal,
    convert_timestamp,
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
