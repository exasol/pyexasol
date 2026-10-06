import os
from test.data_type_cases import (
    DataTypeCase,
    DataTypeCases,
)

import pytest

import pyexasol
from pyexasol import ExaConnection


@pytest.fixture(scope="session")
def default_ipaddr():
    return "localhost"


@pytest.fixture(scope="session")
def default_port():
    return 8563


@pytest.fixture(scope="session")
def user():
    return os.environ.get("EXAUID", "SYS")


@pytest.fixture(scope="session")
def password():
    return os.environ.get("EXAPWD", "exasol")


@pytest.fixture(scope="session")
def schema():
    return os.environ.get("EXASCHEMA", "PYEXASOL_TEST")


@pytest.fixture(scope="session")
def connection_factory(dsn, user, password, schema, websocket_sslopt):
    def _connection_fixture(**kwargs) -> ExaConnection:
        defaults = {
            "dsn": dsn,
            "user": user,
            "password": password,
            "schema": schema,
            "websocket_sslopt": websocket_sslopt,
        }
        config = {**defaults, **kwargs}
        return pyexasol.connect(**config)

    return _connection_fixture


@pytest.fixture
def connection(connection_factory):
    con = connection_factory()
    yield con
    con.close()


ALL_CASES = DataTypeCases().all_cases()


@pytest.fixture(scope="session")
def data_type_cases() -> tuple[DataTypeCase, ...]:
    return ALL_CASES


@pytest.fixture(
    scope="session",
    params=ALL_CASES,
    ids=lambda case: case.websocket_data_type.value,
)
def data_type_case(request) -> DataTypeCase:
    return request.param
