import os
import ssl

import pytest

import pyexasol
from pyexasol import ExaConnection


@pytest.fixture(scope="session")
def ipaddr():
    return "localhost"


@pytest.fixture(scope="session")
def port():
    return 8563


@pytest.fixture(scope="session")
def dsn(certificate_type, ipaddr, port):
    if certificate_type == ssl.CERT_NONE:
        return os.environ.get("EXAHOST", f"{ipaddr}:{port}")
    # The host name is different for this case. As it is required to be the same
    # host name that the certificate is signed. This comes from the ITDE.
    return os.environ.get("EXAHOST", f"exasol-test-database:{port}")


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
