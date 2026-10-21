from test.import_from_file_test_helpers import (
    BINARY_SOURCE_FACTORIES,
    TEXT_SOURCE_FACTORIES,
)
from unittest.mock import (
    MagicMock,
)

import pytest

from pyexasol.connection import ExaConnection


@pytest.fixture
def mock_exa_conn():
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
    conn.import_from_file = ExaConnection.import_from_file.__get__(conn)
    conn.import_from_callback = ExaConnection.import_from_callback.__get__(conn)
    return conn


class TestImportFromFileSource:
    @staticmethod
    @pytest.mark.parametrize(
        "source_factory",
        BINARY_SOURCE_FACTORIES,
    )
    def test_binary_sources_are_accepted(
        source_factory,
        mock_exa_conn,
        mock_http_thread,
        mock_sql_import_thread,
        tmp_path,
    ):

        filepath = tmp_path / "data.csv"
        filepath.write_text("col a, col b")
        with source_factory(filepath) as src:
            mock_exa_conn.import_from_file(src=src, table="dummy_table")

    @staticmethod
    @pytest.mark.parametrize(
        "source_factory",
        TEXT_SOURCE_FACTORIES,
    )
    def test_text_sources_are_rejected(
        source_factory,
        mock_exa_conn,
        mock_http_thread,
        mock_sql_import_thread,
        tmp_path,
    ):

        filepath = tmp_path / "data.csv"
        filepath.write_text("col a, col b")

        with source_factory(filepath) as src:
            with pytest.raises(TypeError) as exception_info:
                mock_exa_conn.import_from_file(src=src, table="dummy_table")
        assert str(exception_info.value) == (
            "Source must be either a file path or a binary stream "
            "(a file opened in binary mode)."
        )
