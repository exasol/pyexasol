import os
import pytest
import pyexasol


@pytest.fixture
def dsn():
    return os.environ.get('EXAHOST', 'localhost:8563')


@pytest.fixture
def user():
    return os.environ.get('EXAUID', 'SYS')


@pytest.fixture
def password():
    return os.environ.get('EXAPWD', 'exasol')


@pytest.fixture
def schema():
    return os.environ.get('EXASCHEMA', 'TEST')


@pytest.fixture
def connection(dsn, user, password, schema):
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
    )
    yield con
    con.close()
