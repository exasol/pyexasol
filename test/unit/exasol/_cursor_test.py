from unittest.mock import (
    Mock,
    patch,
)

import pytest

from exasol.driver.websocket._cursor import (
    Cursor,
)
from exasol.driver.websocket.dbapi2 import (
    DataError,
    ProgrammingError,
)
from pyexasol.data_types.python_to_websocket import convert_python_to_websocket
from pyexasol.data_types.websocket_to_python import convert_websocket_to_python


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


class TestSessionDatetimeFormatValidation:
    @staticmethod
    @pytest.mark.parametrize(
        "fetch_mapper, expect_validation",
        [
            pytest.param(convert_websocket_to_python, True, id="default"),
            pytest.param(lambda value, data_type: value, False, id="custom"),
        ],
    )
    def test_execute_validates_only_for_default_fetch_mapper(
        mock_connection, fetch_mapper, expect_validation
    ):
        mock_connection.fetch_mapper = fetch_mapper

        Cursor(mock_connection).execute("SELECT 1")

        validate = mock_connection.validate_session_datetime_formats
        assert validate.called is expect_validation

    @staticmethod
    @pytest.mark.parametrize(
        "insert_mapper, expect_validation",
        [
            pytest.param(convert_python_to_websocket, True, id="default"),
            pytest.param(lambda value, data_type: value, False, id="custom"),
        ],
    )
    def test_executemany_validates_only_for_default_insert_mapper(
        mock_connection, insert_mapper, expect_validation
    ):
        mock_connection.insert_mapper = insert_mapper
        # A custom fetch_mapper must not affect the validation done for inserts.
        mock_connection.fetch_mapper = lambda value, data_type: value

        Cursor(mock_connection).executemany("INSERT INTO t VALUES (?, ?)", [("a", 1)])

        validate = mock_connection.validate_session_datetime_formats
        assert validate.called is expect_validation


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
