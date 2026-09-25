import decimal

import pytest

from pyexasol.data_types.to_python import convert_decimal


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
