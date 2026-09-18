import io
from pathlib import Path
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


def test_import_from_file_error_on_text_mode_src(
    mock_exa_conn,
    mock_http_thread,
    mock_sql_import_thread,
    tmp_path,
):
    def write_minimal_csv(directory: Path) -> Path:
        filepath = directory / "data.csv"
        csv_text = "col a, col b"
        filepath.write_text(csv_text)
        return filepath

    filepath = write_minimal_csv(directory=tmp_path)
    srcs = [
        (
            io.StringIO("a,b\n"),
            TypeError(
                "Source must be either a file path, or a binary stream , i.e. a file opened in binary mode"
            ),
        ),  # should err
        (io.BytesIO(b"a,b\n"), None),
        (
            open(filepath),
            TypeError(
                "Source must be either a file path, or a binary stream , i.e. a file opened in binary mode"
            ),
        ),  # should err
        (open(filepath, "rb"), None),
        (filepath, None),
    ]
    for src in srcs:
        if src[1] is None:

            mock_exa_conn.import_from_file(src=src[0], table="dummy_table")
        else:

            with pytest.raises(TypeError) as e:
                mock_exa_conn.import_from_file(src=src[0], table="dummy_table")
                assert e.value == src[1].value
