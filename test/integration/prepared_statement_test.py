from inspect import cleandoc

import pytest

from pyexasol.exceptions import (
    ExaRequestError,
    ExaRuntimeError,
)


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

    connection.execute(f"TRUNCATE TABLE {table_name};")
    connection.execute(
        f"""
        INSERT INTO {table_name} VALUES
            (0, 'A'),
            (1, 'B'),
            (2, 'C');
        """
    )

    yield table_name

    if not connection.is_closed:
        connection.execute(f"DROP TABLE IF EXISTS {table_name};")


@pytest.mark.basic
def test_insert_multiple_rows(connection, seeded_table):
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
def test_select_all_without_params(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"SELECT ID, NAME FROM {seeded_table} ORDER BY ID, NAME;"
    )

    prep_stmt.execute_prepared()

    expected = [(0, "A"), (1, "B"), (2, "C")]
    actual = prep_stmt.fetchall()

    assert actual == expected


@pytest.mark.basic
def test_multiple_executes_select_with_insert_in_between(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"SELECT ID, NAME FROM {seeded_table} ORDER BY ID, NAME;"
    )

    prep_stmt.execute_prepared()
    assert prep_stmt.fetchall() == [(0, "A"), (1, "B"), (2, "C")]

    connection.execute(f"INSERT INTO {seeded_table} VALUES (3, 'D');")
    connection.commit()

    prep_stmt.execute_prepared()
    assert prep_stmt.fetchall() == [(0, "A"), (1, "B"), (2, "C"), (3, "D")]


@pytest.mark.basic
def test_multiple_executes_dml(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"DELETE FROM {seeded_table} WHERE ID = ? AND NAME = ?;"
    )

    prep_stmt.execute_prepared([(0, "A")])
    prep_stmt.execute_prepared([(1, "B"), (2, "C")])
    assert prep_stmt.fetchall() == []


@pytest.mark.exceptions
def test_raises_wrong_parameter_type(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"UPDATE {seeded_table} SET NAME = ? WHERE ID = ?;"
    )

    with pytest.raises(
        ExaRequestError, match="invalid character value for cast; Value: 'not-a-number'"
    ):
        prep_stmt.execute_prepared([("Updated", "not-a-number")])


@pytest.mark.exceptions
def test_raises_missing_parameter(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"DELETE FROM {seeded_table} WHERE ID = ? AND NAME = ?;"
    )

    with pytest.raises(
        ExaRequestError,
        match="the number of column metadata objects is not the same as the number of data colums",
    ):
        prep_stmt.execute_prepared([(1,)])


@pytest.mark.exceptions
def test_prep_stmt_close(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"SELECT ID, NAME FROM {seeded_table} ORDER BY ID, NAME;"
    )

    prep_stmt.execute_prepared()

    expected = [(0, "A"), (1, "B"), (2, "C")]
    actual = prep_stmt.fetchall()

    assert actual == expected

    prep_stmt.close()

    with pytest.raises(ExaRuntimeError, match="Prepared statement is already closed"):
        prep_stmt.execute_prepared()


@pytest.mark.exceptions
def test_connection_close(connection, seeded_table):
    prep_stmt = connection.create_prepared_statement(
        f"SELECT ID, NAME FROM {seeded_table} ORDER BY ID, NAME;"
    )

    prep_stmt.execute_prepared()

    expected = [(0, "A"), (1, "B"), (2, "C")]
    actual = prep_stmt.fetchall()
    assert actual == expected

    # Teardown before closing the connection
    connection.execute(f"DROP TABLE IF EXISTS {seeded_table};")

    connection.close()

    with pytest.raises(ExaRuntimeError, match="Prepared statement is already closed"):
        prep_stmt.execute_prepared()
