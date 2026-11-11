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
    def test_execute_delegates_validation_to_connection(mock_connection):
        Cursor(mock_connection).execute("SELECT 1")

        mock_connection.validate_session_datetime_formats.assert_called_once_with(
            "SELECT 1", for_insert=False
        )

    @staticmethod
    def test_executemany_delegates_validation_to_connection(mock_connection):
        operation = "INSERT INTO t VALUES (?, ?)"

        Cursor(mock_connection).executemany(operation, [("a", 1)])

        mock_connection.validate_session_datetime_formats.assert_called_once_with(
            operation, for_insert=True
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
        mock_statement.close.assert_called_once()
        assert cursor._cursor is None

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
        mock_statement.close.assert_called_once()
        assert cursor._cursor is None

    @staticmethod
    def test_failing_close_does_not_hide_original_error(
        mock_connection, mock_statement
    ):
        mock_statement.execute_prepared.side_effect = ProgrammingError("original")
        mock_statement.close.side_effect = RuntimeError("close failed")
        cursor = Cursor(mock_connection)

        with pytest.raises(ProgrammingError, match="original"):
            cursor.executemany(TestExecutemany.STATEMENT, [("a", 1)])

        mock_statement.close.assert_called_once()
        assert cursor._cursor is None

    @staticmethod
    def test_keyboard_interrupt_does_not_close_statement(
        mock_connection, mock_statement
    ):
        mock_statement.execute_prepared.side_effect = KeyboardInterrupt
        cursor = Cursor(mock_connection)

        with pytest.raises(KeyboardInterrupt):
            cursor.executemany(TestExecutemany.STATEMENT, [("a", 1)])

        mock_statement.close.assert_not_called()
