import datetime
import decimal

import pytest

from pyexasol.data_types.to_python import (
    SUPPORTED_DATE_FORMATS,
    convert_date,
    convert_decimal,
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


@pytest.mark.parametrize(
    "format_definition",
    [
        pytest.param(format_definition, id=format_definition)
        for format_definition in SUPPORTED_DATE_FORMATS
    ],
)
def test_convert_date(format_definition):
    expected_date = datetime.date(2026, 9, 11)
    value = expected_date.strftime("%Y-%m-%d")

    assert convert_date(value) == expected_date
