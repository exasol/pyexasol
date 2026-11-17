from __future__ import annotations

import socket
import threading
from pathlib import Path

from .handler import ExaParquetHttpRequestHandler
from .server import ExaParquetTCPServer


class ExaParquetHttpThread(threading.Thread):
    """Serve one local Parquet file through a dedicated Exasol HTTP tunnel."""

    def __init__(
        self,
        ipaddr: str,
        port: int,
        parquet_file: Path,
        encryption: bool,
    ):
        self.server = ExaParquetTCPServer(
            (ipaddr, port),
            ExaParquetHttpRequestHandler,
            parquet_file=parquet_file,
            encryption=encryption,
        )
        self.exc = None
        super().__init__()

    @property
    def exa_address(self) -> str:
        return self.server.exa_address

    def run(self):
        try:
            # One connection serves all requests of the import. Stop waiting for it
            # if the import fails before Exasol connects.
            self.server.serve_first_client()
        except BaseException as error:  # NOSONAR(S5754) re-raised by join_with_exc()
            self.exc = error
        finally:
            self.exc = self.exc or self.server.request_error
            self.server.server_close()

    def join_with_exc(self):
        self.join()

        if self.exc:
            raise self.exc

    def terminate(self):
        self.server.is_terminated = True
        # Wake up the handler if it is waiting for Exasol's next request.
        try:
            self.server.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
