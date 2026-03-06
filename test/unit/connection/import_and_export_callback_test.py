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
def mock_sql_import_thread():
    """Mock ExaSQLImportThread instances"""
    with patch("pyexasol.connection.ExaSQLImportThread") as mock_cls:
        yield mock_cls


@pytest.fixture
def mock_sql_export_thread():
    """Mock ExaSQLExportThread instances"""
    with patch("pyexasol.connection.ExaSQLExportThread") as mock_cls:
        yield mock_cls


@pytest.fixture
def exa_conn():
    """
    Create a mock ExaConnection. We use a real instance but mock
    attributes to avoid actual network/socket initialization.
    """

    def mock_format_logic(query_or_table):
        return query_or_table

    conn = MagicMock(spec=ExaConnection)
    conn.options = {"compression": True, "encryption": True}
    conn.ws_ipaddr = "127.0.0.1"
    conn.ws_port = 8563
    conn.format = MagicMock()
    conn.format.format.side_effect = mock_format_logic

    # Attach the actual methods to the mock instance
    conn.export_to_callback = ExaConnection.export_to_callback.__get__(conn)
    conn.import_from_callback = ExaConnection.import_from_callback.__get__(conn)
    return conn


@pytest.fixture
def callback_spy():
    """Create a callback function with the additional benefits of a mock"""

    def callback_logic(pipe, src, **kwargs):
        pipe.write(b"data")
        return "success_marker"

    return MagicMock(side_effect=callback_logic)


class TestExportToCallback:
    @staticmethod
    def test_not_a_callable_raises_an_exception(
        exa_conn, mock_http_thread, mock_sql_export_thread
    ):
        with pytest.raises(TypeError) as ex:
            exa_conn.export_to_callback(
                callback="not_a_function", dst=None, query_or_table="dummy_table"
            )

        assert mock_http_thread.call_count == 0
        assert mock_sql_export_thread.call_count == 0
        assert str(ex.value) == (
            "`callback` must be callable. " "Received: 'not_a_function' (type: str)"
        )

    @staticmethod
    def test_set_defaults_as_expected(
        exa_conn,
        mock_http_thread,
        mock_sql_export_thread,
        callback_spy,
    ):

        result = exa_conn.export_to_callback(
            callback=callback_spy, dst=None, query_or_table="dummy_table"
        )

        mock_http_thread.return_value.start.assert_called_once()
        mock_sql_export_thread.return_value.start.assert_called_once()
        assert result == "success_marker"

        # verify compression set as expected when import_params=None, then
        # this is set to self.options["compression"]
        http_args, _ = mock_http_thread.call_args
        assert http_args[2] is exa_conn.options["compression"]

        sql_args, _ = mock_sql_export_thread.call_args
        # verify query_params=None would format query_or_table
        assert sql_args[2] == "dummy_table"
        # verify import_params=None maps to empty dictionary
        assert sql_args[3] == {}

        # verify callback_params=None maps to empty dictionary
        _, callback_kwargs = callback_spy.call_args
        assert callback_kwargs == {}


class TestImportFromCallback:
    @staticmethod
    def test_not_a_callable_raises_an_exception(
        exa_conn, mock_http_thread, mock_sql_import_thread
    ):
        with pytest.raises(TypeError) as ex:
            exa_conn.import_from_callback(
                callback="not_a_function", src="src_data", table="dummy_table"
            )

        assert mock_http_thread.call_count == 0
        assert mock_sql_import_thread.call_count == 0
        assert str(ex.value) == (
            "`callback` must be callable. " "Received: 'not_a_function' (type: str)"
        )

    @staticmethod
    def test_set_defaults_as_expected(
        exa_conn,
        mock_http_thread,
        mock_sql_import_thread,
        callback_spy,
    ):
        result = exa_conn.import_from_callback(
            callback=callback_spy, src="src_data", table="dummy_table"
        )

        mock_http_thread.return_value.start.assert_called_once()
        mock_sql_import_thread.return_value.start.assert_called_once()
        assert result == "success_marker"

        # verify compression set as expected when import_params=None, then
        # this is set to self.options["compression"]
        http_args, _ = mock_http_thread.call_args
        assert http_args[2] is exa_conn.options["compression"]

        # verify import_params=None maps to empty dictionary
        sql_args, _ = mock_sql_import_thread.call_args
        assert sql_args[3] == {}

        # verify callback_params=None maps to empty dictionary
        _, callback_kwargs = callback_spy.call_args
        assert callback_kwargs == {}
