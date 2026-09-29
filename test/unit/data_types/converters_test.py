import datetime

import pytest

from pyexasol.data_types.converters import ExaTimeDelta


class TestExaTimeDelta:
    @staticmethod
    def test_from_timedelta():
        days = 3
        hours = 10
        minutes = 59
        seconds = 15
        microseconds = 12345

        actual = ExaTimeDelta.from_timedelta(
            datetime.timedelta(
                days=days,
                hours=hours,
                minutes=minutes,
                seconds=seconds,
                microseconds=microseconds,
            )
        )

        assert actual == ExaTimeDelta(
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            microseconds=microseconds,
        )

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(
                ExaTimeDelta(days=-3, seconds=1),
                "-000000002 23:59:59.000000000",
                id="negative-duration-with-seconds",
            ),
            pytest.param(
                ExaTimeDelta(days=-3),
                "-000000003 00:00:00.000000000",
                id="negative-duration-without-seconds",
            ),
            pytest.param(
                ExaTimeDelta(
                    days=3,
                    hours=10,
                    minutes=59,
                    seconds=59,
                    microseconds=123000,
                ),
                "+000000003 10:59:59.123000000",
                id="non-negative-duration-",
            ),
        ],
    )
    def test_to_interval(value, expected):
        assert value.to_interval() == expected

    @staticmethod
    def test_str_returns_interval_representation():
        value = ExaTimeDelta(days=1, seconds=2)
        assert str(value) == "+000000001 00:00:02.000000000"
