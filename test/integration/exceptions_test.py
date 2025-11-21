import pytest

from pyexasol import (
    ExaAuthError,
    ExaCommunicationError,
    ExaConnectionError,
    ExaQueryError,
    ExaRequestError,
    ExaRuntimeError,
)


@pytest.mark.exceptions
def test_bad_dsn(connection_factory):
    with pytest.raises(ExaConnectionError):
        connection_factory(dsn="ThisIsBroken")


@pytest.mark.exceptions
@pytest.mark.parametrize(
    "keyword,value",
    [
        pytest.param("user", "wrongy", id="user-is-incorrect"),
        pytest.param("password", "no-tworking", id="password-is-incorrect"),
    ],
)
def test_bad_credentials(connection_factory, keyword, value):
    with pytest.raises(ExaAuthError):
        connection_factory(**{keyword: value})


@pytest.mark.exceptions
def test_invalid_sql(connection):
    statement = "SELECT1;"
    with pytest.raises(ExaQueryError):
        connection.execute(statement)


@pytest.mark.exceptions
def test_read_from_closed_cursor(connection_factory):
    con = connection_factory(fetch_size_bytes=1024)
    query = "SELECT * FROM users;"
    cursor = con.execute(query)
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
def test_close_closed_connection(connection_factory, connection):
    con = connection_factory()
    connection.execute(f"KILL SESSION {con.session_id()}")
    with pytest.raises(ExaCommunicationError):
        con.close()
