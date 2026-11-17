from pathlib import Path

import pytest

from pyexasol.transport.parquet.server import ExaParquetTCPServer


@pytest.fixture
def server():
    # Skip __init__: it would connect to Exasol (see ExaTunnelServer.server_bind).
    server = ExaParquetTCPServer.__new__(ExaParquetTCPServer)
    server.parquet_file = Path("f.parquet")
    server.request_error = None
    return server


class TestHandleError:
    @staticmethod
    def test_records_exception_and_prints_nothing(server, capsys):
        error = FileNotFoundError("gone")

        try:
            raise error
        except FileNotFoundError:
            server.handle_error(request=None, client_address=("127.0.0.1", 1))

        assert server.request_error is error
        assert capsys.readouterr().err == ""

    @staticmethod
    def test_keeps_first_error(server):
        first = BrokenPipeError("first")
        for error in (first, ConnectionResetError("second")):
            try:
                raise error
            except OSError:
                server.handle_error(request=None, client_address=("127.0.0.1", 1))

        assert server.request_error is first
