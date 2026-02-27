"""
Throughout the development the :class:`pyexasol.ExaConnection`has grown to include a
number of features. In the long-term, these will be broken it up into smaller
encapsulated units. This would allow for more explicit testing, make the code easier to
read & interpret, & help reduce difficulties associated with changes and potentially
overlooked edge cases.

To aid in a potential refactoring, it's useful to add mocked unit tests to cover
the aspects of the :class:`pyexasol.ExaConnection` that are used to define the
connection and its settings, but that these aspects themselves do not require
the connection to be made. The components tested here are prime candidates for being
broken out into separate, more manageable units.
"""

from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from pyexasol import ExaConnection


def mock_exaconnection(connection_class, **kwargs):
    """
    Within the :func:`pyexasol.ExaConnection.__init__` function, external services
    are being sent requests. To explicitly test the code around these calls, these are
    mocked.
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
