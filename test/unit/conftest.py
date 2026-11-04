from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from pyexasol import ExaConnection


def mock_exaconnection(connection_class, **kwargs):
    """
    :func:`pyexasol.ExaConnection.__init__` already sends requests to external services.
    To explicitly test the code around these calls, these services are mocked.
    """

    defaults = {
        "dsn": "localhost:8563",
        "user": "dummy",
        "password": "dummy",
        "schema": "dummy",
    }
    config = {**defaults, **kwargs}

    default_mock = MagicMock(return_value=None)
    mocks = {
        "_init_ws": default_mock,
        "_login": default_mock,
        "get_attr": default_mock,
    }
    with patch.multiple(connection_class, **mocks):
        return connection_class(**config)


@pytest.fixture(scope="session")
def mock_exaconnection_factory():
    def _exaconnection_fixture(**kwargs) -> ExaConnection:
        defaults = {
            "connection_class": ExaConnection,
        }
        config = {**defaults, **kwargs}

        return mock_exaconnection(**config)

    return _exaconnection_fixture
