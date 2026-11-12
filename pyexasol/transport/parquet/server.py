from __future__ import annotations

from pathlib import Path

from pyexasol.transport.tunnel_server import ExaTunnelServer


class ExaParquetTCPServer(ExaTunnelServer):
    """Tunnel server for Parquet transfers, which serves one local Parquet file."""

    def __init__(self, *args, parquet_file: Path, **kwargs):
        self.parquet_file = parquet_file
        self.request_error: BaseException | None = None
        super().__init__(*args, **kwargs)
