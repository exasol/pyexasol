import datetime

import pytest

from exasol.driver.websocket._connection import (
    SUPPORTED_DATE_FORMATS,
    SUPPORTED_TIMESTAMP_FORMATS,
)
from pyexasol.mapper import exasol_mapper


class TestExasolMapper:
    @pytest.mark.parametrize(
        "format_definition",
        [
            pytest.param(format_definition, id=format_definition)
            for format_definition in SUPPORTED_DATE_FORMATS
        ],
    )
    def test_maps_supported_date_formats(self, format_definition):
        expected_date = datetime.date(2026, 9, 11)
        value = expected_date.strftime("%Y-%m-%d")

        assert exasol_mapper(value, {"type": "DATE"}) == expected_date

    @pytest.mark.parametrize(
        "format_definition",
        [
            pytest.param(
                SUPPORTED_TIMESTAMP_FORMATS[0],
                id=SUPPORTED_TIMESTAMP_FORMATS[0],
            )
        ],
    )
    def test_maps_supported_timestamp_formats_without_fractional_seconds(
        self, format_definition
    ):
        expected_timestamp = datetime.datetime(2026, 9, 11, 12, 34, 56)
        value = expected_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        assert exasol_mapper(value, {"type": "TIMESTAMP"}) == expected_timestamp

    @pytest.mark.parametrize(
        "format_definition, fractional_value",
        [
            pytest.param(
                format_definition,
                fractional_value,
                id=format_definition,
            )
            for format_definition, fractional_value in zip(
                SUPPORTED_TIMESTAMP_FORMATS[1:],
                (
                    "1",
                    "12",
                    "123",
                    "1234",
                    "12345",
                    "123456",
                    "1234567",
                    "12345678",
                    "123456789",
                ),
            )
        ],
    )
    def test_maps_supported_timestamp_formats_with_fractional_seconds(
        self, format_definition, fractional_value
    ):
        expected_timestamp = datetime.datetime(
            2026,
            9,
            11,
            12,
            34,
            56,
            int(fractional_value[:6].ljust(6, "0")),
        )
        value = f"2026-09-11 12:34:56.{fractional_value}"
        assert exasol_mapper(value, {"type": "TIMESTAMP"}) == expected_timestamp
