import datetime
from test.data_type_cases import DataTypeCase

import pytest

from exasol.driver.websocket._cursor import Cursor
from pyexasol.data_types import WebSocketDataType


class TestAdaptToRequestedDbTypes:
    @staticmethod
    def test_adapts_data_type_case(data_type_case):
        if data_type_case.websocket_data_type is WebSocketDataType.IntervalDayToSecond:
            pytest.xfail(
                "ExaTimeDelta conversion is tracked in "
                "https://github.com/exasol/pyexasol/issues/428"
            )

        adapted_values = Cursor._adapt_to_requested_db_types(
            [data_type_case.python_value],
            {
                "columns": [
                    {"dataType": {"type": data_type_case.websocket_data_type.value}}
                ]
            },
        )

        assert adapted_values == [data_type_case.websocket_value]

    @staticmethod
    @pytest.mark.parametrize(
        "data_type_case",
        [
            pytest.param(
                DataTypeCase(WebSocketDataType.String, "123", 123),
                id="VARCHAR-int",
            ),
            pytest.param(
                DataTypeCase(WebSocketDataType.String, None, None),
                id="VARCHAR-none",
            ),
            pytest.param(
                DataTypeCase(WebSocketDataType.Double, 1.25, "1.25"),
                id="DOUBLE-string",
            ),
            pytest.param(
                DataTypeCase(WebSocketDataType.Double, None, None),
                id="DOUBLE-none",
            ),
        ],
    )
    def test_adapts_special_values(data_type_case):
        db_response = {
            "columns": [
                {"dataType": {"type": data_type_case.websocket_data_type.value}}
            ]
        }

        adapted_values = Cursor._adapt_to_requested_db_types(
            [data_type_case.python_value], db_response
        )

        assert adapted_values == [data_type_case.websocket_value]

    @staticmethod
    def test_adapts_multiple_columns_in_order():
        parameters = [
            datetime.date(2026, 9, 11),
            1.25,
            123,
            True,
        ]

        adapted_values = Cursor._adapt_to_requested_db_types(
            parameters,
            {
                "columns": [
                    {"dataType": {"type": "DATE"}},
                    {"dataType": {"type": "DOUBLE"}},
                    {"dataType": {"type": "VARCHAR"}},
                    {"dataType": {"type": "BOOLEAN"}},
                ]
            },
        )

        assert adapted_values == ["2026-09-11", 1.25, "123", True]
