import datetime
from unittest.mock import (
    Mock,
    patch,
)

import pytest

from exasol.driver.websocket._cursor import (
    Cursor,
    _dbapi2pyexasol,
)
from exasol.driver.websocket.dbapi2 import (
    DataError,
    ProgrammingError,
)
from pyexasol.data_types import WebSocketDataType
from pyexasol.data_types.python_to_websocket import convert_python_to_websocket
from test.data_type_cases import (
    DataTypeCase,
    to_pytest_params,
)


@pytest.fixture
def mock_statement():
    statement = Mock()
    statement.parameter_data = {
        "columns": [
            {"dataType": {"type": "VARCHAR"}},
            {"dataType": {"type": "DECIMAL"}},
        ]
    }
    return statement


@pytest.fixture
def mock_connection(mock_statement):
    connection = Mock()
    connection.insert_mapper = lambda value, data_type: value
    connection.connection.cls_statement.return_value = mock_statement
    return connection


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

        assert adapted_values == [data_type_case.sql_value]

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
    def test_rejects_too_few_parameters():
        with pytest.raises(
            ProgrammingError,
            match="Input shape mismatch: received 2 parameters, 13 columns",
        ):
            Cursor._adapt_to_requested_db_types(
                [1, 2],
                {"columns": [{"dataType": {"type": "DECIMAL"}}] * 13},
            )

    @staticmethod
    def test_rejects_too_few_columns():
        with pytest.raises(
            ProgrammingError,
            match="Input shape mismatch: received 13 parameters, 2 columns",
        ):
            Cursor._adapt_to_requested_db_types(
                list(range(13)),
                {"columns": [{"dataType": {"type": "DECIMAL"}}] * 2},
            )


class TestExecutemany:
    STATEMENT = "INSERT INTO t VALUES (?, ?)"

    @staticmethod
    def test_valid_rows_are_mapped_validated_and_executed(
        mock_connection, mock_statement
    ):
        mock_connection.insert_mapper = Mock(
            side_effect=lambda value, data_type: f"{value}-mapped"
        )
        cursor = Cursor(mock_connection)

        with patch(
            "exasol.driver.websocket._cursor.validate_value_for_websocket"
        ) as validate:
            cursor.executemany(TestExecutemany.STATEMENT, [("a", 1)])

        assert [c.args for c in mock_connection.insert_mapper.call_args_list] == [
            ("a", {"type": "VARCHAR"}),
            (1, {"type": "DECIMAL"}),
        ]
        assert [c.args for c in validate.call_args_list] == [
            ("a-mapped", "VARCHAR"),
            ("1-mapped", "DECIMAL"),
        ]
        mock_statement.execute_prepared.assert_called_once_with(
            [["a-mapped", "1-mapped"]]
        )

    @staticmethod
    @pytest.mark.parametrize(
        "bad_row, parameter_count",
        [
            pytest.param(("b",), 1, id="too-few-parameters"),
            pytest.param(("b", 2, 3), 3, id="too-many-parameters"),
        ],
    )
    def test_row_with_wrong_parameter_count_raises_programming_error(
        mock_connection, mock_statement, bad_row, parameter_count
    ):
        cursor = Cursor(mock_connection)

        with pytest.raises(ProgrammingError) as exception_info:
            cursor.executemany(TestExecutemany.STATEMENT, [("a", 1), bad_row])

        assert (
            str(exception_info.value)
            == f"Input shape mismatch for row 2: received {parameter_count} parameters, 2 columns"
        )
        mock_statement.execute_prepared.assert_not_called()

    @staticmethod
    @pytest.mark.parametrize(
        "bad_row, expected_message",
        [
            pytest.param(
                ("b", float("nan")),
                "Invalid value in seq_of_parameters row 2:\n"
                "  * Column location: 2\n"
                "  * Column type: DECIMAL\n"
                "  * nan is not supported for DECIMAL. NaN and Infinity cannot be "
                "stored, so replace the value first, e.g. with None.",
                id="conversion-fails",
            ),
            pytest.param(
                (123, 2),
                "Invalid value in seq_of_parameters row 2:\n"
                "  * Column location: 1\n"
                "  * Column type: VARCHAR\n"
                "  * Allowed types for VARCHAR: str, None; got int.",
                id="validation-fails",
            ),
        ],
    )
    def test_row_failing_map_or_validation_raises_data_error(
        mock_connection, mock_statement, bad_row, expected_message
    ):
        mock_connection.insert_mapper = convert_python_to_websocket
        cursor = Cursor(mock_connection)

        with pytest.raises(DataError) as exception_info:
            cursor.executemany(TestExecutemany.STATEMENT, [("a", 1), bad_row])

        assert str(exception_info.value) == expected_message
        mock_statement.execute_prepared.assert_not_called()
