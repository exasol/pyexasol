import datetime
import decimal

import pytest

from pyexasol.data_types import ExaTimeDelta
from pyexasol.data_types.python_to_websocket import (
    date_to_websocket,
    decimal_to_websocket,
    double_to_websocket,
    interval_day_to_second_to_websocket,
    timestamp_to_websocket,
)

NON_FINITE_VALUES = [
    pytest.param(decimal.Decimal("NaN"), id="decimal-nan"),
    pytest.param(decimal.Decimal("sNaN"), id="decimal-snan"),
    pytest.param(decimal.Decimal("Infinity"), id="decimal-infinity"),
    pytest.param(decimal.Decimal("-Infinity"), id="decimal-negative-infinity"),
    pytest.param(float("nan"), id="float-nan"),
    pytest.param(float("inf"), id="float-infinity"),
    pytest.param(float("-inf"), id="float-negative-infinity"),
]


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
                id="naive-with-microseconds",
            ),
            pytest.param(
                datetime.datetime(2026, 9, 11, 23, 59, 59),
                "2026-09-11 23:59:59",
                id="naive-without-microseconds",
            ),
        ],
    )
    def test_formats_datetime_as_timestamp_string(value, expected):
        assert date_to_websocket(value) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "tz",
        [
            pytest.param(datetime.UTC, id="utc"),
            pytest.param(datetime.timezone(datetime.timedelta(hours=-5)), id="offset"),
        ],
    )
    def test_raises_value_error_for_aware_datetime(tz):
        value = datetime.datetime(2026, 1, 1, 23, 0, tzinfo=tz)
        with pytest.raises(ValueError, match=r"not supported for DATE.*\.date\(\)"):
            date_to_websocket(value)

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
        "tz",
        [
            pytest.param(datetime.UTC, id="utc"),
            pytest.param(datetime.timezone(datetime.timedelta(hours=2)), id="positive"),
            pytest.param(
                datetime.timezone(datetime.timedelta(hours=-4)), id="negative"
            ),
        ],
    )
    def test_raises_value_error_for_aware_datetime(tz):
        value = datetime.datetime(2026, 9, 11, 12, 34, 56, tzinfo=tz)
        with pytest.raises(
            ValueError, match=r"not supported for TIMESTAMP.*replace\(tzinfo=None\)"
        ):
            timestamp_to_websocket(value)

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


class TestDecimalToWebsocket:
    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(None, id="none"),
            pytest.param("12.5", id="string"),
        ],
    )
    def test_pass_through_values_unchanged(value):
        assert decimal_to_websocket(value) is value

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(decimal.Decimal("12.5"), "12.5", id="regular"),
            pytest.param(decimal.Decimal("-12.50"), "-12.50", id="negative"),
            pytest.param(decimal.Decimal("0.000001"), "0.000001", id="small"),
            pytest.param(decimal.Decimal("1E+3"), "1000", id="exponent"),
        ],
    )
    def test_formats_decimal(value, expected):
        assert decimal_to_websocket(value) == expected

    @staticmethod
    def test_ignores_str_override_of_decimal_subclass():
        class LabelStrDecimal(decimal.Decimal):
            def __str__(self):
                return "value=" + super().__str__()

        assert decimal_to_websocket(LabelStrDecimal("12.5")) == "12.5"

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(0, "0", id="zero"),
            pytest.param(-3, "-3", id="negative"),
            pytest.param(
                12345678901234567890123456789012345,
                "12345678901234567890123456789012345",
                id="35-digits",
            ),
        ],
    )
    def test_formats_int(value, expected):
        assert decimal_to_websocket(value) == expected

    @staticmethod
    def test_ignores_str_override_of_int_subclass():
        class LabelStrInt(int):
            def __str__(self):
                return "value=" + super().__str__()

        assert decimal_to_websocket(LabelStrInt(12)) == "12"

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(-0.25, "-0.25", id="negative"),
            pytest.param(2.675, "2.675", id="positive"),
            pytest.param(1e-7, "0.0000001", id="exponent"),
        ],
    )
    def test_formats_float(value, expected):
        assert decimal_to_websocket(value) == expected

    @staticmethod
    def test_ignores_repr_override_of_float_subclass():
        class LabelReprFloat(float):
            def __repr__(self):
                return "value=" + super().__repr__()

        assert decimal_to_websocket(LabelReprFloat(1.5)) == "1.5"

    @staticmethod
    @pytest.mark.parametrize("value", NON_FINITE_VALUES)
    def test_raises_value_error_for_non_finite(value):
        with pytest.raises(ValueError, match="not supported for DECIMAL"):
            decimal_to_websocket(value)

    @staticmethod
    @pytest.mark.parametrize("value", ["NaN", "Infinity"])
    def test_passes_through_non_finite_string(value):
        assert decimal_to_websocket(value) == value

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(True, id="bool-true"),
            pytest.param(datetime.date(2026, 9, 11), id="date"),
            pytest.param(b"1", id="bytes"),
        ],
    )
    def test_raises_type_error_for_other_types(value):
        with pytest.raises(TypeError, match="Expected decimal.Decimal"):
            decimal_to_websocket(value)


class TestDoubleToWebsocket:
    @staticmethod
    def test_passes_through_none():
        assert double_to_websocket(None) is None

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(1.5, id="regular"),
            pytest.param(-0.25, id="negative"),
            pytest.param(5e-324, id="smallest-subnormal"),
        ],
    )
    def test_keeps_float(value):
        result = double_to_websocket(value)

        assert type(result) is float
        assert result == value

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            *NON_FINITE_VALUES,
            pytest.param(decimal.Decimal("1E+400"), id="decimal-overflow"),
        ],
    )
    def test_raises_value_error_for_non_finite(value):
        with pytest.raises(ValueError, match="not supported for DOUBLE"):
            double_to_websocket(value)

    @staticmethod
    def test_raises_overflow_error_for_huge_int():
        with pytest.raises(OverflowError):
            double_to_websocket(10**400)

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(0, 0.0, id="zero"),
            pytest.param(-3, -3.0, id="negative"),
            # Sent as a float, so the database does not truncate the digits.
            pytest.param(
                12345678901234567890123456789012345,
                float(12345678901234567890123456789012345),
                id="35-digits",
            ),
        ],
    )
    def test_converts_int_to_float(value, expected):
        result = double_to_websocket(value)

        assert type(result) is float
        assert result == expected

    @staticmethod
    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param(decimal.Decimal("1.5"), 1.5, id="regular"),
            pytest.param(decimal.Decimal("-12.50"), -12.5, id="negative"),
        ],
    )
    def test_converts_decimal_to_float(value, expected):
        result = double_to_websocket(value)

        assert type(result) is float
        assert result == expected

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("1.5", id="string"),
            pytest.param(True, id="bool-true"),
            pytest.param(b"1", id="bytes"),
        ],
    )
    def test_raises_type_error_for_other_types(value):
        with pytest.raises(TypeError, match="Expected float"):
            double_to_websocket(value)
