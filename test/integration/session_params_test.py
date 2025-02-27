from importlib.metadata import version

import pytest

import pyexasol


@pytest.fixture
def pyexasol_version():
    yield version("pyexasol")


@pytest.fixture
def session_info_query():
    yield "SELECT * FROM EXA_DBA_SESSIONS WHERE session_id=CURRENT_SESSION"


@pytest.mark.configuration
def test_default_session_parameters(connection, session_info_query, pyexasol_version):
    expected = {f"PyEXASOL {pyexasol_version}"}
    actual = set(connection.execute(session_info_query).fetchone())
    assert actual >= expected


@pytest.mark.configuration
def test_modified_session_parameters(
    dsn, user, password, schema, connection, session_info_query
):
    client_name = "MyCustomClient"
    client_version = "1.2.3"
    client_os_username = "small_cat"
    connection = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        client_name=client_name,
        client_version=client_version,
        client_os_username=client_os_username,
    )

    expected = {f"{client_name} {client_version}", client_os_username}
    actual = set(connection.execute(session_info_query).fetchone())
    assert actual >= expected
