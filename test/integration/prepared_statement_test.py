from inspect import cleandoc

import pytest

from pyexasol.exceptions import ExaRequestError


@pytest.fixture
def table_name():
    yield "PREPARED_STMT_USERS"


@pytest.fixture
def seeded_table(connection, table_name):
    ddl = cleandoc(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            ID      DECIMAL(18,0),
            NAME    VARCHAR(16)
        );
        """
    )
    connection.execute(ddl)
    connection.commit()

    connection.execute(f"TRUNCATE TABLE {table_name};")
    connection.execute(
        f"""
        INSERT INTO {table_name} VALUES
            (0, 'A'),
            (1, 'B'),
            (2, 'C');
        """
    )
    connection.commit()

    yield table_name

    connection.execute(f"DROP TABLE IF EXISTS {table_name};")
    connection.commit()


@pytest.mark.basic
def test_create_prepared_statement_insert_multiple_rows(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"INSERT INTO {seeded_table} VALUES (?, ?);"
    )

    prep_stmt.execute_prepared([(3, "D"), (4, "E"), (5, "F")])
    connection.commit()

    expected_rowcount = 3
    actual_rowcount = prep_stmt.rowcount()
    assert actual_rowcount == expected_rowcount

    result = connection.execute(
        f"SELECT ID, NAME FROM {seeded_table} ORDER BY ID, NAME;"
    )

    expected = [(0, "A"), (1, "B"), (2, "C"), (3, "D"), (4, "E"), (5, "F")]
    actual = result.fetchall()

    assert actual == expected


@pytest.mark.basic
def test_create_prepared_statement_select_all_without_params(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"SELECT ID, NAME FROM {seeded_table} ORDER BY ID, NAME;"
    )

    prep_stmt.execute_prepared()

    expected = [(0, "A"), (1, "B"), (2, "C")]
    actual = prep_stmt.fetchall()

    assert actual == expected


@pytest.mark.basic
def test_create_prepared_statement_select_all_without_params2(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"SELECT ID, NAME FROM {seeded_table} ORDER BY ID, NAME;"
    )

    prep_stmt.execute_prepared([])

    expected = [(0, "A"), (1, "B"), (2, "C")]
    actual = prep_stmt.fetchall()

    assert actual == expected


@pytest.mark.exceptions
def test_create_prepared_statement_update_raises_for_wrong_parameter_type(
    connection, seeded_table
):
    prep_stmt = connection.create_prepared_statement(
        f"UPDATE {seeded_table} SET NAME = ? WHERE ID = ?;"
    )

    with pytest.raises(
        ExaRequestError, match="invalid character value for cast; Value: 'not-a-number'"
    ):
        prep_stmt.execute_prepared([("Updated", "not-a-number")])


@pytest.mark.exceptions
def test_create_prepared_statement_delete_raises_for_missing_parameter(
    connection, seeded_table
):
    prep_stmt = connection.create_prepared_statement(
        f"DELETE FROM {seeded_table} WHERE ID = ? AND NAME = ?;"
    )

    with pytest.raises(
        ExaRequestError,
        match="the number of column metadata objects is not the same as the number of data colums",
    ):
        prep_stmt.execute_prepared([(1,)])


@pytest.mark.exceptions
def test_prepared_statement_close(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"SELECT ID, NAME FROM {seeded_table} ORDER BY ID, NAME;"
    )

    prep_stmt.execute_prepared()

    expected = [(0, "A"), (1, "B"), (2, "C")]
    actual = prep_stmt.fetchall()

    assert actual == expected

    prep_stmt.close()
    # Error message could be improved to report the already closed statement
    with pytest.raises(
        ExaRequestError, match="JSON value '/statementHandle' is not an integer"
    ):
        prep_stmt.execute_prepared()
