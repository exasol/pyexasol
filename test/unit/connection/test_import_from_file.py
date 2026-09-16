from pathlib import Path
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest
from pydantic import ValidationError

from pyexasol.connection import ExaConnection
from test.integration.import_and_export.helper import select_result


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


@pytest.fixture
def mock_sql_thread():
    """Mock ExaSQLThread instances used by export callbacks."""
    with patch("pyexasol.connection.ExaSQLThread") as mock_cls:
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
    conn.import_from_file = ExaConnection.import_from_file.__get__(conn)
    conn.import_from_callback = ExaConnection.import_from_callback.__get__(conn)
    return conn

@pytest.fixture
def callback_spy():
    """Create a callback function with the additional benefits of a mock"""

    def callback_logic(pipe, src, **kwargs):
        pipe.write(b"data")
        return "success_marker"

    return MagicMock(side_effect=callback_logic)

import io
#@pytest.mark.parametrize("src", [io.StringIO("a,b\n"), io.BytesIO(b"a,b\n")])#open('/tmp/p.csv'), '/tmp/p.csv'])
def test_import_from_file(
        #src,
        exa_conn,
        mock_http_thread,
        mock_sql_import_thread,
        callback_spy,
        tmp_path,
    ):
    def write_csv(
        directory: Path
    ) -> Path:
        filepath = directory / "data.csv"

        csv_text = ("col a, col b")
        filepath.write_text(csv_text)
        return filepath

    filepath = write_csv(directory=tmp_path)
    srcs= [io.StringIO("a,b\n"), #should err
           io.BytesIO(b"a,b\n"),
           open(filepath), #should err
           open(filepath, "rb"),
           filepath]
    for src in srcs:
        result = exa_conn.import_from_file( src=src, table="dummy_table"
        )
        print(src)
        print(result)
        print(select_result(exa_conn))
        print("__________________")#todo check for error and compare if is thrown

    assert False

