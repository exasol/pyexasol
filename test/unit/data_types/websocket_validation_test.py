import decimal

import pytest

from pyexasol.data_types.websocket_types import WebSocketDataType
from pyexasol.data_types.websocket_validation import validate_value_for_websocket

ALL_DB_TYPES = [pytest.param(t.value, id=t.value) for t in WebSocketDataType]

STRING_DB_TYPES = [
    pytest.param(t.value, id=t.value)
    for t in (
        WebSocketDataType.Char,
        WebSocketDataType.Date,
        WebSocketDataType.Geometry,
        WebSocketDataType.Hashtype,
        WebSocketDataType.IntervalDayToSecond,
        WebSocketDataType.IntervalYearToMonth,
        WebSocketDataType.Timestamp,
        WebSocketDataType.TimestampTz,
        WebSocketDataType.String,
    )
]

NON_STRING_VALUES = [
    pytest.param(1, id="int"),
    pytest.param(1.5, id="float"),
    pytest.param(True, id="bool"),
    pytest.param(b"abc", id="bytes"),
    pytest.param(decimal.Decimal("1.5"), id="decimal"),
    pytest.param([], id="list"),
    pytest.param({}, id="dict"),
]


class TestValidateValueForWebsocket:
    @staticmethod
    @pytest.mark.parametrize("data_type", ALL_DB_TYPES)
    def test_validates_every_websocket_data_type(data_type):
        # Fails for a new WebSocketDataType until validate_value_for_websocket handles it.
        with pytest.raises(TypeError):
            validate_value_for_websocket(object(), data_type)

    @staticmethod
    @pytest.mark.parametrize("data_type", ALL_DB_TYPES)
    def test_accepts_none(data_type):
        validate_value_for_websocket(None, data_type)

    @staticmethod
    @pytest.mark.parametrize("data_type", ALL_DB_TYPES)
    def test_accepts_empty_string(data_type):
        validate_value_for_websocket("", data_type)

    @staticmethod
    @pytest.mark.parametrize("data_type", STRING_DB_TYPES)
    @pytest.mark.parametrize("value", ["", "abc"])
    def test_accepts_string(data_type, value):
        validate_value_for_websocket(value, data_type)

    @staticmethod
    @pytest.mark.parametrize("data_type", STRING_DB_TYPES)
    @pytest.mark.parametrize("value", NON_STRING_VALUES)
    def test_rejects_non_string(data_type, value):
        with pytest.raises(
            TypeError,
            match=f"Expected str or None for {data_type} got {type(value).__name__}.",
        ):
            validate_value_for_websocket(value, data_type)

    @staticmethod
    @pytest.mark.parametrize("value", ["1.5", 1, -3, 1.5, 2**70])
    def test_accepts_string_int_or_float_for_decimal(value):
        validate_value_for_websocket(value, "DECIMAL")

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(True, id="bool"),
            pytest.param(decimal.Decimal("1.5"), id="decimal"),
            pytest.param(b"1.5", id="bytes"),
        ],
    )
    def test_rejects_other_types_for_decimal(value):
        with pytest.raises(
            TypeError,
            match=f"Expected str, int, float or None for DECIMAL got {type(value).__name__}.",
        ):
            validate_value_for_websocket(value, "DECIMAL")

    @staticmethod
    def test_accepts_float_for_double():
        validate_value_for_websocket(1.5, "DOUBLE")

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(1, id="int"),
            pytest.param(True, id="bool"),
            pytest.param("1.5", id="string"),
            pytest.param(decimal.Decimal("1.5"), id="decimal"),
        ],
    )
    def test_rejects_other_types_for_double(value):
        with pytest.raises(
            TypeError,
            match=f"Expected float or None for DOUBLE got {type(value).__name__}.",
        ):
            validate_value_for_websocket(value, "DOUBLE")

    @staticmethod
    @pytest.mark.parametrize("value", [True, False])
    def test_accepts_bool_for_boolean(value):
        validate_value_for_websocket(value, "BOOLEAN")

    @staticmethod
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param(1, id="int"),
            pytest.param(0, id="int-zero"),
            pytest.param(1.0, id="float"),
            pytest.param("TRUE", id="string"),
        ],
    )
    def test_rejects_other_types_for_boolean(value):
        with pytest.raises(
            TypeError,
            match=f"Expected bool or None for BOOLEAN got {type(value).__name__}.",
        ):
            validate_value_for_websocket(value, "BOOLEAN")

    @staticmethod
    @pytest.mark.parametrize("value", [None, "a", 1, 1.5, True, b"a", []])
    def test_does_not_validate_unknown_data_type(value):
        validate_value_for_websocket(value, "SOME_FUTURE_TYPE")

    @staticmethod
    @pytest.mark.parametrize("value", [None, "abc", 123])
    @pytest.mark.parametrize(
        "data_type",
        [
            pytest.param({"type": "VARCHAR", "size": 5}, id="dict"),
            pytest.param(WebSocketDataType.String, id="enum_member"),
            pytest.param(None, id="none"),
        ],
    )
    def test_rejects_non_str_data_type(data_type, value):
        with pytest.raises(TypeError, match="data_type to be a str"):
            validate_value_for_websocket(value, data_type)
