import socket
from unittest.mock import (
    MagicMock,
    patch,
)

import pytest

from pyexasol.transport.parquet.thread import ExaParquetHttpThread

SERVER_CLASS = "pyexasol.transport.parquet.thread.ExaParquetTCPServer"


@pytest.fixture
def server():
    server = MagicMock()
    server.exa_address = "10.0.0.7:4242"
    server.is_terminated = False
    server.request_error = None
    return server


@pytest.fixture
def thread(server, tmp_path):
    with patch(SERVER_CLASS, return_value=server) as server_class:
        thread = ExaParquetHttpThread(
            ipaddr="127.0.0.1",
            port=1234,
            parquet_file=tmp_path / "f.parquet",
            encryption=True,
        )
    thread.server_class = server_class
    return thread


class TestRun:
    @staticmethod
    def test_serves_first_client_and_closes_server(thread, server):
        thread.run()

        server.serve_first_client.assert_called_once_with()
        server.server_close.assert_called_once()
        assert thread.exc is None

    @staticmethod
    def test_keeps_exception_from_server(thread, server):
        error = RuntimeError("boom")
        server.serve_first_client.side_effect = error

        thread.run()

        assert thread.exc is error
        server.server_close.assert_called_once()

    @staticmethod
    def test_reports_error_recorded_by_request_handler(thread, server):
        error = OSError("file changed")
        server.request_error = error

        thread.run()

        assert thread.exc is error

    @staticmethod
    def test_server_exception_wins_over_request_error(thread, server):
        raised = RuntimeError("boom")
        server.serve_first_client.side_effect = raised
        server.request_error = OSError("file changed")

        thread.run()

        assert thread.exc is raised

    @staticmethod
    def test_keeps_non_exception_base_exception(thread, server):
        server.serve_first_client.side_effect = KeyboardInterrupt

        thread.run()

        assert isinstance(thread.exc, KeyboardInterrupt)
        server.server_close.assert_called_once()


class TestJoinWithExc:
    @staticmethod
    def test_raises_stored_exception(thread, server):
        server.serve_first_client.side_effect = RuntimeError("boom")
        thread.start()

        with pytest.raises(RuntimeError, match="boom"):
            thread.join_with_exc()

    @staticmethod
    def test_returns_when_no_error(thread):
        thread.start()

        thread.join_with_exc()


class TestTerminate:
    @staticmethod
    def test_marks_server_terminated_and_shuts_socket_down(thread, server):
        thread.terminate()

        assert server.is_terminated is True
        server.socket.shutdown.assert_called_once_with(socket.SHUT_RDWR)

    @staticmethod
    def test_ignores_already_closed_socket(thread, server):
        server.socket.shutdown.side_effect = OSError

        thread.terminate()

        assert server.is_terminated is True
