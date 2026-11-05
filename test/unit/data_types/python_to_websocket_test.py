import datetime

import pytest

from pyexasol.data_types import ExaTimeDelta
from pyexasol.data_types.python_to_websocket import (
    date_to_websocket,
    interval_day_to_second_to_websocket,
    timestamp_to_websocket,
)


class TestDateToWebsocket:
    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(None, id="none"),
            pytest.param("2026-09-11", id="string"),
        ],
    )
    def test_pass_through_values_unchanged(value):
        assert date_to_websocket(value) is value

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(datetime.time(12, 34), id="time"),
            pytest.param(20260911, id="int"),
        ],
    )
    def test_raises_type_error_for_other_types(value):
        with pytest.raises(TypeError, match="Expected datetime.date"):
            date_to_websocket(value)

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(datetime.date(2026, 9, 11), "2026-09-11", id="date"),
            pytest.param(datetime.date(2024, 2, 29), "2024-02-29", id="leap-day"),
        ],
    )
    def test_formats_date(value, expected):
        assert date_to_websocket(value) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(
                datetime.datetime(2026, 9, 11, 12, 34, 56, 123456),
                "2026-09-11 12:34:56.123456",
                id="naive",
            ),
            pytest.param(
                datetime.datetime(
                    2026,
                    9,
                    11,
                    14,
                    34,
                    tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
                ),
                "2026-09-11 12:34:00",
                id="aware-converted-to-utc",
            ),
        ],
    )
    def test_formats_datetime_as_timestamp(value, expected):
        assert date_to_websocket(value) == expected

    @staticmethod
    def test_ignores_str_override_of_date_subclass():
        class SlashStrDate(datetime.date):
            def __str__(self):
                return self.strftime("%d/%m/%Y")

        assert date_to_websocket(SlashStrDate(2026, 9, 11)) == "2026-09-11"


class TestTimestampToWebsocket:
    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(None, id="none"),
            pytest.param("2026-09-11 12:34:56.123", id="string"),
        ],
    )
    def test_pass_through_values_unchanged(value):
        assert timestamp_to_websocket(value) is value

    @staticmethod
    def test_raises_type_error_for_other_types():
        date_value = datetime.date(2026, 9, 11)
        with pytest.raises(TypeError, match="Expected datetime.datetime"):
            timestamp_to_websocket(date_value)

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
        assert timestamp_to_websocket(value) == expected

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
        assert timestamp_to_websocket(value) == expected

    @staticmethod
    def test_treats_tzinfo_without_utcoffset_as_naive():
        class NoOffsetTz(datetime.tzinfo):
            def utcoffset(self, dt):
                return None

            def dst(self, dt):
                return None

        value = datetime.datetime(2026, 9, 11, 12, 34, 56, tzinfo=NoOffsetTz())
        assert timestamp_to_websocket(value) == "2026-09-11 12:34:56"

    @staticmethod
    def test_ignores_str_override_of_datetime_subclass():
        class IsoStrDatetime(datetime.datetime):
            def __str__(self):
                return self.isoformat()

        value = IsoStrDatetime(2026, 9, 11, 12, 34, 56, 123456)
        assert timestamp_to_websocket(value) == "2026-09-11 12:34:56.123456"


class TestIntervalDayToSecondToWebsocket:
    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(None, id="none"),
            pytest.param("+1 02:03:04.5", id="string"),
        ],
    )
    def test_pass_through_values_unchanged(value):
        assert interval_day_to_second_to_websocket(value) is value

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(datetime.datetime(2026, 9, 11), id="datetime"),
            pytest.param(1, id="int"),
        ],
    )
    def test_raises_type_error_for_other_types(value):
        with pytest.raises(TypeError, match="Expected datetime.timedelta"):
            interval_day_to_second_to_websocket(value)

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(datetime.timedelta(0), "+0 00:00:00", id="zero"),
            pytest.param(
                datetime.timedelta(days=1, seconds=4, microseconds=123456),
                "+1 00:00:04.123456",
                id="positive-fraction",
            ),
            pytest.param(
                datetime.timedelta(seconds=-1.5),
                "-0 00:00:01.5",
                id="negative-fraction",
            ),
        ],
    )
    def test_formats_timedelta(value, expected):
        assert interval_day_to_second_to_websocket(value) == expected

    @staticmethod
    def test_formats_exa_timedelta():
        value = ExaTimeDelta(days=1, hours=2, microseconds=123456)

        assert interval_day_to_second_to_websocket(value) == "+1 02:00:00.123456"
