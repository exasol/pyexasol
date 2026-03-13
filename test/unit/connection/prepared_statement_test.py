from unittest.mock import MagicMock

import pytest


def _create_prepared_statement_response(num_params, result):
    return {
        "responseData": {
            "statementHandle": 1,
            "parameterData": {
                "numColumns": num_params,
                "columns": [{}] * num_params,
            },
            "results": [result],
        }
    }


def _result_set_result(*, num_columns, num_rows, columns, data=None):
    result_set = {
        "numColumns": num_columns,
        "numRows": num_rows,
        "numRowsInMessage": num_rows,
        "columns": columns,
    }
    if data is not None:
        result_set["data"] = data

    return {
        "resultType": "resultSet",
        "resultSet": result_set,
    }


def _assert_execute_prepared_request(connection, *, num_columns, num_rows, data):
    connection.req.assert_called_with(
        {
            "command": "executePreparedStatement",
            "statementHandle": 1,
            "numColumns": num_columns,
            "numRows": num_rows,
            "columns": [{}] * num_columns,
            "data": data,
        }
    )


def _assert_prepared_statement_state(
    prep_stmt,
    *,
    query,
    result_type,
    num_columns,
    num_rows_total,
    num_rows_chunk,
    row_count,
):
    assert prep_stmt.query == query
    assert prep_stmt.statement_handle is not None
    assert prep_stmt.result_type == result_type
    assert prep_stmt.num_columns == num_columns
    assert prep_stmt.num_rows_total == num_rows_total
    assert prep_stmt.num_rows_chunk == num_rows_chunk
    assert prep_stmt.row_count == row_count
    assert prep_stmt.pos_total == 0
    assert prep_stmt.pos_chunk == 0


@pytest.mark.parametrize(
    "sql,num_params",
    [
        ("INSERT INTO S.T VALUES (?, ?)", 2),
        ("UPDATE S.T SET name='Updated' WHERE id = ?", 1),
        ("DELETE FROM S.T WHERE ID = ? AND NAME = ?", 2),
    ],
)
def test_create_prepared_statement_with_dml(
    mock_exaconnection_factory, sql, num_params
):
    connection = mock_exaconnection_factory()
    connection.req = MagicMock(
        return_value=_create_prepared_statement_response(
            num_params=num_params,
            result={"resultType": "rowCount", "rowCount": 0},
        )
    )

    prep_stmt = connection.create_prepared_statement(sql)

    connection.req.assert_called_once_with(
        {
            "command": "createPreparedStatement",
            "sqlText": sql,
        }
    )
    _assert_prepared_statement_state(
        prep_stmt,
        query=sql,
        result_type="rowCount",
        num_columns=0,
        num_rows_total=0,
        num_rows_chunk=0,
        row_count=0,
    )


def test_create_prepared_statement_with_select(mock_exaconnection_factory):
    sql = "SELECT ID, NAME FROM S.T WHERE ID = ? AND NAME = ?"
    metadata_columns = [
        {"name": "ID", "dataType": ...},
        {"name": "NAME", "dataType": ...},
    ]
    connection = mock_exaconnection_factory()
    connection.req = MagicMock(
        return_value=_create_prepared_statement_response(
            num_params=2,
            result=_result_set_result(
                num_columns=2, num_rows=0, columns=metadata_columns
            ),
        )
    )

    prep_stmt = connection.create_prepared_statement(sql)

    connection.req.assert_called_once_with(
        {
            "command": "createPreparedStatement",
            "sqlText": sql,
        }
    )

    _assert_prepared_statement_state(
        prep_stmt,
        query=sql,
        result_type="resultSet",
        num_columns=2,
        num_rows_total=0,
        num_rows_chunk=0,
        row_count=0,
    )


def test_execute_prepared_dml(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    connection.req = MagicMock(
        return_value=_create_prepared_statement_response(
            num_params=2,
            result={"resultType": "rowCount", "rowCount": 0},
        )
    )
    sql = "INSERT INTO S.T VALUES (?, ?)"
    prep_stmt = connection.create_prepared_statement(sql)
    connection.req = MagicMock(
        return_value={
            "responseData": {
                "results": [{"resultType": "rowCount", "rowCount": 2}],
                "numResults": 1,
            }
        }
    )
    prep_stmt.execute_prepared([(0, "A"), (1, "B")])

    _assert_execute_prepared_request(
        connection,
        num_columns=2,
        num_rows=2,
        data=[(0, 1), ("A", "B")],
    )
    _assert_prepared_statement_state(
        prep_stmt,
        query=sql,
        result_type="rowCount",
        num_columns=0,
        num_rows_total=0,
        num_rows_chunk=0,
        row_count=2,
    )


def test_multiple_execute_prepared_select(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    metadata_columns = [
        {"name": "ID", "dataType": ...},
        {"name": "NAME", "dataType": ...},
    ]
    expected_data = [[0, 0, 0, 0], ["A", "A", "A", "A"]]
    connection.req = MagicMock(
        return_value=_create_prepared_statement_response(
            num_params=2,
            result=_result_set_result(
                num_columns=2,
                num_rows=0,
                columns=metadata_columns,
            ),
        )
    )
    sql = "SELECT ID, NAME FROM S.T WHERE ID = ? AND NAME = ?"
    prep_stmt = connection.create_prepared_statement(sql)
    connection.req = MagicMock(
        return_value={
            "responseData": {
                "results": [
                    _result_set_result(
                        num_columns=2,
                        num_rows=4,
                        data=expected_data,
                        columns=metadata_columns,
                    )
                ],
                "numResults": 1,
            }
        }
    )
    # calls twice execute_prepared to test reseting of counters to fetch data
    for i in range(2):
        prep_stmt.execute_prepared([(0, "A")])

        _assert_execute_prepared_request(
            connection, num_columns=2, num_rows=1, data=[(0,), ("A",)]
        )
        _assert_prepared_statement_state(
            prep_stmt,
            query=sql,
            result_type="resultSet",
            num_columns=2,
            num_rows_total=4,
            num_rows_chunk=4,
            row_count=0,
        )

        assert prep_stmt.fetchall() == [(0, "A"), (0, "A"), (0, "A"), (0, "A")]


def test_prepared_statement_close(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    connection.req = MagicMock(
        return_value=_create_prepared_statement_response(
            num_params=2,
            result={"resultType": "rowCount", "rowCount": 0},
        )
    )

    prep_stmt = connection.create_prepared_statement("INSERT INTO S.T VALUES (?, ?);")

    prep_stmt.close()
    connection.req.assert_called_with(
        {
            "command": "closePreparedStatement",
            "statementHandle": 1,
        }
    )
    assert prep_stmt.statement_handle is None
