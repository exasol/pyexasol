import datetime
import decimal
import subprocess
import sys

import pytest

from pyexasol.data_types import WebSocketDataType
from pyexasol.data_types.websocket_to_python import (
    SUPPORTED_TIMESTAMP_FORMATS,
    convert_date,
    convert_decimal,
    convert_timestamp,
    convert_websocket_to_python,
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

    result = convert_timestamp(f"2026-09-11 12:34:56{fraction}")

    expected_microsecond = int(fraction_digits[:6].ljust(6, "0")) if digits else 0
    assert result.microsecond == expected_microsecond


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


class TestConvertWebsocketToPython:
    @staticmethod
    def test_maps_types(data_type_case):
        data_type = {
            "type": data_type_case.websocket_data_type.value,
            "scale": data_type_case.scale,
        }
        assert (
            convert_websocket_to_python(data_type_case.websocket_value, data_type)
            == data_type_case.python_value
        )

    @staticmethod
    @pytest.mark.parametrize(
        "data_type",
        WebSocketDataType,
        ids=lambda data_type: data_type.value,
    )
    def test_maps_none(data_type):
        result = convert_websocket_to_python(
            None, {"type": data_type.value, "scale": 0}
        )
        assert result is None


def test_exasol_mapper_emits_one_deprecation_warning():
    import_script = """
import warnings

from pyexasol.data_types.websocket_to_python import exasol_mapper
from pyexasol.warnings import PyexasolDeprecationWarning

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    assert exasol_mapper(
        val="123", data_type={"type": "DECIMAL", "scale": 0}
    ) == 123
    assert exasol_mapper(
        val="456", data_type={"type": "DECIMAL", "scale": 0}
    ) == 456

assert len(caught) == 1
assert issubclass(caught[0].category, PyexasolDeprecationWarning)
"""
    subprocess.run(
        [sys.executable, "-c", import_script],
        check=True,
        capture_output=True,
        text=True,
    )
