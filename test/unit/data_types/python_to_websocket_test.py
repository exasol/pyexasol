import datetime

import pytest

from pyexasol.data_types.python_to_websocket import convert_timestamp


class TestConvertTimestamp:
    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(None, id="none"),
            pytest.param("2026-09-11 12:34:56.123", id="string"),
        ],
    )
    def test_pass_through_values_unchanged(value):
        assert convert_timestamp(value) is value

    @staticmethod
    def test_raises_type_error_for_other_types():
        with pytest.raises(TypeError, match="Expected datetime.datetime"):
            convert_timestamp(datetime.date(2026, 9, 11))

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(
                datetime.datetime(2026, 9, 11, 12, 34, 56),
                "2026-09-11 12:34:56",
                id="whole-second",
            ),
            pytest.param(
                datetime.datetime(2026, 9, 11, 12, 34, 56, 123456),
                "2026-09-11 12:34:56.123456",
                id="microseconds",
            ),
        ],
    )
    def test_formats_datetime(value, expected):
        assert convert_timestamp(value) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(
                datetime.datetime(2026, 9, 11, 12, 34, 56, 123456, tzinfo=datetime.UTC),
                "2026-09-11 12:34:56.123456",
                id="utc-offset-dropped",
            ),
            pytest.param(
                datetime.datetime(
                    2026,
                    9,
                    11,
                    14,
                    34,
                    56,
                    123000,
                    tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
                ),
                "2026-09-11 12:34:56.123000",
                id="positive-offset-converted-to-utc",
            ),
            pytest.param(
                datetime.datetime(
                    2026,
                    9,
                    11,
                    8,
                    34,
                    56,
                    tzinfo=datetime.timezone(datetime.timedelta(hours=-4)),
                ),
                "2026-09-11 12:34:56",
                id="negative-offset-converted-to-utc",
            ),
        ],
    )
    def test_converts_timezone_aware_datetime_to_utc(value, expected):
        assert convert_timestamp(value) == expected
