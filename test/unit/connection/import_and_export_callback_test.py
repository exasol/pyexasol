from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from pyexasol.connection import ExaConnection


@pytest.fixture
def mock_http_thread():
    """Patch ExaHttpThread where ExaConnection looks it up."""
    with patch("pyexasol.connection.ExaHttpThread") as mock_cls:
        instance = mock_cls.return_value
        instance.write_pipe = MagicMock()
        instance.write_pipe.__enter__.return_value = MagicMock(spec=["write"])
        yield mock_cls


@pytest.fixture
def mock_sql_thread():
    """Mock ExaSQLImportThread instances"""
    with patch("pyexasol.connection.ExaSQLImportThread") as mock_cls:
        yield mock_cls


@pytest.fixture
def exa_conn():
    """
    Create a mock ExaConnection. We use a real instance but mock
    attributes to avoid actual network/socket initialization.
    """
    conn = MagicMock(spec=ExaConnection)
    conn.options = {"compression": True, "encryption": True}
    conn.ws_ipaddr = "127.0.0.1"
    conn.ws_port = 8563

    # Attach the actual methods to the mock instance
    conn.export_to_callback = ExaConnection.export_to_callback.__get__(conn)
    conn.import_from_callback = ExaConnection.import_from_callback.__get__(conn)
    return conn


class TestExportToCallback:
    @staticmethod
    def test_not_a_callable_raises_an_exception(
        exa_conn, mock_http_thread, mock_sql_thread
    ):
        with pytest.raises(TypeError) as ex:
            exa_conn.export_to_callback("not_a_function", None, "dummy_table")

        assert mock_http_thread.call_count == 0
        assert mock_sql_thread.call_count == 0
        assert str(ex.value) == (
            "`callback` must be callable. " "Received: 'not_a_function' (type: str)"
        )


class TestImportFromCallback:
    @staticmethod
    def test_not_a_callable_raises_an_exception(
        exa_conn, mock_http_thread, mock_sql_thread
    ):
        with pytest.raises(TypeError) as ex:
            exa_conn.import_from_callback("not_a_function", None, "dummy_table")

        assert mock_http_thread.call_count == 0
        assert mock_sql_thread.call_count == 0
        assert str(ex.value) == (
            "`callback` must be callable. " "Received: 'not_a_function' (type: str)"
        )
