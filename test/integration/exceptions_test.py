import pytest

import pyexasol
from pyexasol import (
    ExaAuthError,
    ExaCommunicationError,
    ExaConnectionError,
    ExaQueryError,
    ExaRequestError,
    ExaRuntimeError,
)


@pytest.mark.exceptions
def test_bad_dsn(connection):
    with pytest.raises(ExaConnectionError):
        pyexasol.connect(dsn="ThisIsBroken")


@pytest.mark.exceptions
@pytest.mark.parametrize(
    "credentials", [{"username": "wrongy"}, {"password": "no-tworking"}]
)
def test_bad_credentails(credentials, dsn, user, password):
    username = credentials.get("username", user)
    password = credentials.get("password", password)
    with pytest.raises(ExaAuthError):
        pyexasol.connect(dsn=dsn, user=username, password=password)


@pytest.mark.exceptions
def test_invalid_sql(connection):
    statement = "SELECT1;"
    with pytest.raises(ExaQueryError):
        connection.execute(statement)


@pytest.mark.exceptions
def test_read_from_closed_cursor(dsn, user, password, schema):
    connection = pyexasol.connect(
        dsn=dsn, user=user, password=password, schema=schema, fetch_size_bytes=1024
    )
    query = "SELECT * FROM users;"
    cursor = connection.execute(query)
    cursor.fetchone()
    cursor.close()
    with pytest.raises(ExaRequestError):
        cursor.fetchall()


@pytest.mark.exceptions
def test_fetch_without_result_set(connection):
    cursor = connection.execute("COMMIT;")
    with pytest.raises(ExaRuntimeError):
        cursor.fetchone()


@pytest.mark.exceptions
def test_duplicated_select_of_column(connection):
    query = "SELECT user_name, user_name FROM USERS;"
    with pytest.raises(ExaRuntimeError):
        connection.execute(query)


@pytest.mark.exceptions
def test_attempt_to_run_query_on_closed_connection(connection):
    connection.close()
    with pytest.raises(ExaRuntimeError):
        connection.execute("SELECT 1;")


@pytest.mark.exceptions
def test_close_closed_connection(connection, dsn, user, password, schema):
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
    )

    connection.execute(f"KILL SESSION {con.session_id()}")
    with pytest.raises(ExaCommunicationError):
        con.close()
