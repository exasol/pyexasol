from __future__ import annotations

import sys
from pathlib import Path

from pyexasol.transport.tunnel_server import ExaTunnelServer


class ExaParquetTCPServer(ExaTunnelServer):
    """Tunnel server for Parquet transfers, which serves one local Parquet file."""

    def __init__(self, *args, parquet_file: Path, **kwargs):
        self.parquet_file = parquet_file
        self.request_error: BaseException | None = None
        super().__init__(*args, **kwargs)

    def handle_error(self, request, client_address):
        """
        Record the exception that is being handled instead of printing its traceback.

        Exceptions raised while handling a request (e.g. the file is missing, or the
        connection breaks while sending) are caught by ``socketserver`` and passed
        here. The default implementation only prints a traceback to stderr, so the
        import would be reported as successful. The first error is kept, as later
        ones are usually just consequences of it, and is raised by
        ``ExaParquetHttpThread.join_with_exc()``.
        """
        if self.request_error is None:
            self.request_error = sys.exc_info()[1]
