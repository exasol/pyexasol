import pytest

from exasol.driver.websocket.dbapi2 import connect


@pytest.fixture
def connection(dsn, user, password, schema):
    dbapi_connection = connect(
        dsn=dsn,
        username=user,
        password=password,
        schema=schema,
        certificate_validation=False,
    )
    yield dbapi_connection
    dbapi_connection.close()


@pytest.fixture
def cursor(connection):
    dbapi_cursor = connection.cursor()
    yield dbapi_cursor
    dbapi_cursor.close()
