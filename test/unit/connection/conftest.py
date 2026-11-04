"""
Throughout the development the :class:`pyexasol.ExaConnection` has grown to include a
number of features. In the long-term, these will be broken up into smaller units.
This would allow for more explicit testing, make the code easier to
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


@pytest.fixture
def mock_http_thread():
    """Patch ExaHttpThread where ExaConnection looks it up."""
    with patch("pyexasol.connection.ExaHttpThread") as mock_cls:
        instance = mock_cls.return_value
        instance.write_pipe = MagicMock()
        instance.write_pipe.__enter__.return_value = MagicMock(spec=["write"])

        def construct_http_thread(*args, **kwargs):
            worker_finished_event = kwargs.get("worker_finished_event")
            if worker_finished_event is not None:
                # The real HTTP thread signals this event when it finishes.
                worker_finished_event.set()
            return instance

        mock_cls.side_effect = construct_http_thread
        yield mock_cls


@pytest.fixture
def mock_sql_import_thread():
    """Mock ExaSQLThread instances used by import callbacks."""
    with patch("pyexasol.connection.ExaSQLThread") as mock_cls:
        yield mock_cls
