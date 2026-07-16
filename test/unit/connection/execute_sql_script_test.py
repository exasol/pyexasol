from unittest.mock import MagicMock

import pytest


def test_execute_sql_script_executes_all_statements_in_order(
    mock_exaconnection_factory,
):
    connection = mock_exaconnection_factory()
    statements = [MagicMock(name="statement_1"), MagicMock(name="statement_2")]
    connection.execute = MagicMock(side_effect=statements)

    actual = connection.execute_sql_script("SELECT 1; SELECT 2;")

    assert actual == statements
    assert connection.execute.call_args_list == [
        (("SELECT 1",),),
        (("SELECT 2",),),
    ]


def test_execute_sql_script_ignores_empty_and_comment_only_statements(
    mock_exaconnection_factory,
):
    connection = mock_exaconnection_factory()
    statement = MagicMock(name="statement")
    connection.execute = MagicMock(return_value=statement)

    actual = connection.execute_sql_script("-- comment\n;\nSELECT 1;\n/* comment */")

    assert actual == [statement]
    connection.execute.assert_called_once_with("SELECT 1")


def test_execute_sql_script_stops_after_first_error(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    error = RuntimeError("statement failed")
    connection.execute = MagicMock(side_effect=[MagicMock(name="statement"), error])

    with pytest.raises(RuntimeError, match="statement failed"):
        connection.execute_sql_script("SELECT 1; SELECT fail; SELECT 3;")

    assert connection.execute.call_args_list == [
        (("SELECT 1",),),
        (("SELECT fail",),),
    ]
