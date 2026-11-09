from io import (
    BytesIO,
    StringIO,
)
from unittest.mock import (
    MagicMock,
)

import pytest

import pyexasol.callback as cb
from pyexasol.connection import ExaConnection


@pytest.fixture
def mock_exa_conn():
    conn = MagicMock(spec=ExaConnection)
    conn.import_from_file = ExaConnection.import_from_file.__get__(conn)
    conn.import_from_callback = MagicMock()
    return conn


class TestImportFromFileSource:
    @staticmethod
    def test_valid_source_is_delegated_to_import_callback(mock_exa_conn):
        src = BytesIO(b"col_a,col_b\n")
        mock_exa_conn.import_from_callback.return_value = None

        mock_exa_conn.import_from_file(
            src=src,
            table="dummy_table",
        )

        mock_exa_conn.import_from_callback.assert_called_once_with(
            cb.import_from_file,
            src,
            "dummy_table",
            None,
            None,
        )

    @staticmethod
    def test_invalid_source_is_not_delegated_to_import_callback(mock_exa_conn):
        src = StringIO("col_a,col_b\n")
        with pytest.raises(TypeError):
            mock_exa_conn.import_from_file(src=src, table="dummy_table")
        mock_exa_conn.import_from_callback.assert_not_called()
