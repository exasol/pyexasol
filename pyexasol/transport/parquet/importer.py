from __future__ import annotations

import ssl
from pathlib import Path
from typing import TYPE_CHECKING

from pyexasol import callback as cb
from pyexasol.exceptions import ExaImportError
from pyexasol.http_transport import ExaSQLThread
from pyexasol.query_builders.parquet.builders import (
    ImportBuilder as ParquetImportBuilder,
)
from pyexasol.transport.parquet.thread import ExaParquetHttpThread
from pyexasol.transport.tunnel_server import ExaTunnelServer

if TYPE_CHECKING:
    from pyexasol.connection import ExaConnection


class ExaParquetImporter:
    """Native Parquet import for one :class:`pyexasol.ExaConnection`."""

    def __init__(self, connection: ExaConnection):
        self.connection = connection

    def import_from_native_parquet(
        self,
        source: list[Path] | Path | str,
        table: str | tuple[str, ...],
        import_params: dict | None = None,
        max_parallel_files: int = 10,
    ):
        if max_parallel_files < 1:
            raise ValueError("max_parallel_files must be at least 1.")
        if import_params is None:
            import_params = {}

        parquet_files = cb.get_parquet_files(source)
        import_builder = ParquetImportBuilder(table=table, **import_params)
        # Generating the certificate is costly, so do it once and share it between
        # all tunnels of this import.
        ssl_context = (
            ExaTunnelServer.generate_adhoc_ssl_context()
            if self.connection.options["encryption"]
            else None
        )

        # An Exasol tunnel accepts one connection for one statement, so each file
        # gets its own tunnel (one thread each). Up to ``max_parallel_files`` files
        # are imported by one IMPORT statement, and the batches run sequentially.
        # The batches are not imported atomically: if one fails, earlier batches
        # stay imported (subject to ``autocommit``).
        for batch_start in range(0, len(parquet_files), max_parallel_files):
            batch = parquet_files[batch_start : batch_start + max_parallel_files]
            self._import_native_parquet_batch(
                batch, import_builder, ssl_context, batch_start, len(parquet_files)
            )

    def _import_native_parquet_batch(
        self,
        batch: list[Path],
        import_builder: ParquetImportBuilder,
        ssl_context: tuple[ssl.SSLContext, str] | None,
        batch_start: int,
        total_files: int,
    ):
        http_threads: list[ExaParquetHttpThread] = []
        sql_thread = None

        try:
            for parquet_file in batch:
                http_thread = ExaParquetHttpThread(
                    ipaddr=self.connection.ws_ipaddr,
                    port=self.connection.ws_port,
                    parquet_file=parquet_file,
                    encryption=self.connection.options["encryption"],
                    ssl_context=ssl_context,
                )
                http_threads.append(http_thread)
                http_thread.start()

            sql_thread = ExaSQLThread(
                connection=self.connection,
                compression=False,
                query_builder=import_builder,
            )
            sql_thread.set_exa_address_list(
                [http_thread.exa_address for http_thread in http_threads]
            )
            sql_thread.start()
            sql_thread.join_with_exc()

            for http_thread in http_threads:
                http_thread.terminate()
                http_thread.join_with_exc()
        except (Exception, KeyboardInterrupt) as error:
            for http_thread in http_threads:
                http_thread.terminate()
                if http_thread.is_alive():
                    http_thread.join()

            if sql_thread is not None:
                sql_thread.join(1)
                if sql_thread.is_alive():
                    self.connection.abort_query()
                    sql_thread.join()

            self.connection.logger.debug(
                f"Native Parquet import failed for the batch of files "
                f"{batch_start + 1}-{batch_start + len(batch)} of {total_files}; "
                f"{batch_start} file(s) were imported before it"
            )
            raise ExaImportError(
                connection=self.connection,
                exceptions=(
                    error,
                    *(http_thread.exc for http_thread in http_threads),
                    sql_thread.exc if sql_thread else None,
                ),
            ) from error
