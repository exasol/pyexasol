import datetime

import pytest

from pyexasol.data_types.converters import ExaTimeDelta

INTERVAL_CASES = [
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
            microseconds=123456,
        ),
        "+000000003 10:59:59.123456000",
        id="positive-duration",
    ),
    pytest.param(
        ExaTimeDelta(days=-2, seconds=86398, microseconds=500000),
        "-000000001 00:00:01.500000000",
        id="negative-duration-with-microseconds",
    ),
    pytest.param(
        ExaTimeDelta(microseconds=-500000),
        "-000000000 00:00:00.500000000",
        id="negative-duration-under-one-day-with-microseconds",
    ),
    pytest.param(
        ExaTimeDelta(hours=-1),
        "-000000000 01:00:00.000000000",
        id="negative-duration-under-one-day",
    ),
]

FROM_INTERVAL_CASES = [
    pytest.param(
        ExaTimeDelta(seconds=1),
        "+000000000 00:00:00.999999999",
        id="nanoseconds-rounded-to-one-second",
    ),
    pytest.param(
        ExaTimeDelta(days=-3),
        "-000000003 00:00:00",
        id="without-fractional-part",
    ),
]


class TestExaTimeDelta:
    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(
                ExaTimeDelta(),
                (0, 0),
                id="zero-duration",
            ),
            pytest.param(
                ExaTimeDelta(seconds=1),
                (86399, 0),
                id="duration-with-seconds",
            ),
            pytest.param(
                ExaTimeDelta(seconds=1, microseconds=1),
                (86398, 999999),
                id="duration-with-seconds-and-microseconds",
            ),
        ],
    )
    def test_reverse_seconds(value, expected):
        assert value.reverse_seconds() == expected

    @staticmethod
    @pytest.mark.parametrize(
        "value, interval",
        INTERVAL_CASES,
    )
    def test_from_interval(value, interval):
        actual = ExaTimeDelta.from_interval(interval)

        assert actual == value

    @staticmethod
    @pytest.mark.parametrize(
        "value, interval",
        FROM_INTERVAL_CASES,
    )
    def test_from_interval_parsing_cases(value, interval):
        assert ExaTimeDelta.from_interval(interval) == value

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
        "value, interval",
        INTERVAL_CASES,
    )
    def test_to_interval(value, interval):
        assert value.to_interval() == interval

    @staticmethod
    def test_str_returns_interval_representation():
        value = ExaTimeDelta(days=1, seconds=2)
        assert str(value) == "+000000001 00:00:02.000000000"
