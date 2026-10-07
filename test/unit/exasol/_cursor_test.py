import datetime
from test.data_type_cases import (
    DataTypeCase,
    to_pytest_params,
)

import pytest

from exasol.driver.websocket._cursor import (
    Cursor,
    _dbapi2pyexasol,
)
from pyexasol.data_types import WebSocketDataType


class TestAdaptToRequestedDbTypes:
    @staticmethod
    def adapt_parameters(parameters, db_response):
        converted_parameters = [_dbapi2pyexasol(value) for value in parameters]
        return Cursor._adapt_to_requested_db_types(converted_parameters, db_response)

    @staticmethod
    def test_adapts_data_type_case(data_type_case):
        if data_type_case.websocket_data_type is WebSocketDataType.IntervalDayToSecond:
            pytest.xfail(
                "ExaTimeDelta conversion is tracked in "
                "https://github.com/exasol/pyexasol/issues/428"
            )

        adapted_values = TestAdaptToRequestedDbTypes.adapt_parameters(
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
        to_pytest_params(
            (
                DataTypeCase("VARCHAR-int", WebSocketDataType.String, "123", 123),
                DataTypeCase("VARCHAR-none", WebSocketDataType.String, None, None),
                DataTypeCase("DOUBLE-string", WebSocketDataType.Double, 1.25, "1.25"),
                DataTypeCase("DOUBLE-none", WebSocketDataType.Double, None, None),
            )
        ),
    )
    def test_adapts_special_values(data_type_case):
        db_response = {
            "columns": [
                {"dataType": {"type": data_type_case.websocket_data_type.value}}
            ]
        }

        adapted_values = TestAdaptToRequestedDbTypes.adapt_parameters(
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

        adapted_values = TestAdaptToRequestedDbTypes.adapt_parameters(
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

    @staticmethod
    def test_rejects_too_many_columns():
        with pytest.raises(ValueError):
            Cursor._adapt_to_requested_db_types(
                [1, 2],
                {"columns": [{"dataType": {"type": "DECIMAL"}}] * 13},
            )

    @staticmethod
    def test_rejects_too_many_parameters():
        with pytest.raises(ValueError):
            Cursor._adapt_to_requested_db_types(
                list(range(14)),
                {"columns": [{"dataType": {"type": "DECIMAL"}}] * 13},
            )
