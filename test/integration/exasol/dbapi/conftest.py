import pytest

from exasol.driver.websocket.dbapi2 import connect


@pytest.fixture
def connection(dsn, user, password, schema):
    _connection = connect(
        dsn=dsn,
        username=user,
        password=password,
        schema=schema,
        certificate_validation=False,
    )
    yield _connection
    _connection.close()


@pytest.fixture
def cursor(connection):
    cursor = connection.cursor()
    yield cursor
    cursor.close()
