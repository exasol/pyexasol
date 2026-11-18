from __future__ import annotations

import os
import socketserver
import threading
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING
from warnings import warn

from .query_builders.base_builder import QueryBuilder
from .query_builders.csv.builders import (
    BooleanRepresentation,
    Delimit,
    ExportBuilder,
    FileFormat,
    ImportBuilder,
    Trim,
)
from .transport.tunnel_server import ExaTunnelServer
from .warnings import PyexasolDeprecationWarning

if TYPE_CHECKING:
    from pyexasol import ExaConnection


def _warn_deprecated(name: str, replacement: str, stacklevel: int = 3) -> None:
    # Repetition is limited by the stdlib per-call-site warnings registry
    warn(
        f"{name} is deprecated and will be removed in the future. "
        f"Use {replacement} instead.",
        PyexasolDeprecationWarning,
        stacklevel=stacklevel,
    )


@dataclass
class SqlQuery:
    """
    Base class for the import and export query dataclasses.

    .. deprecated::
        Use ``ImportBuilder`` or ``ExportBuilder`` from ``pyexasol.query_builders.csv.builders`` instead.
    """

    connection: ExaConnection
    compression: bool
    # set these values in param dictionary to ExaConnection
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Iterable[str] | None = None
    comment: str | None = None
    csv_cols: Iterable[str] | None = None
    encoding: str | None = None
    format: FileFormat | None = None
    null: str | None = None
    row_separator: str | None = None

    def __post_init__(self):
        replacement = {
            "ImportQuery": "pyexasol.query_builders.csv.builders.ImportBuilder",
            "ExportQuery": "pyexasol.query_builders.csv.builders.ExportBuilder",
        }.get(
            type(self).__name__,
            "ImportBuilder or ExportBuilder from pyexasol.query_builders.csv.builders",
        )
        _warn_deprecated(type(self).__name__, replacement, stacklevel=4)


@dataclass
class ImportQuery(SqlQuery):
    """
    Validate import parameters and build an IMPORT query.

    .. deprecated::
        Use ``pyexasol.query_builders.csv.builders.ImportBuilder`` instead.
    """

    # set these values in param dictionary to ExaConnection
    skip: str | int | None = None
    trim: Trim | None = None

    def build_query(
        self, table: str | tuple[str, ...], exa_address_list: list[str]
    ) -> str:
        import_builder = self._get_import_builder(table)
        return import_builder.build_query(
            database_version=self.connection.exasol_db_version,
            encryption=self.connection.options["encryption"],
            exa_address_list=exa_address_list,
            formatter=self.connection.format,
        )

    @staticmethod
    def load_from_dict(
        connection: ExaConnection, compression: bool, params: dict
    ) -> ImportQuery:
        """
        Load the params dictionary into the ImportQuery class

        Keys in `params` that are not present in as attributes of the `ImportQuery`
        class will raise an Exception.
        """
        return ImportQuery(connection=connection, compression=compression, **params)

    def _get_import_builder(self, table: str | tuple[str, ...]) -> ImportBuilder:
        return ImportBuilder(
            compression=self.compression,
            table=table,
            column_delimiter=self.column_delimiter,
            column_separator=self.column_separator,
            columns=self.columns,
            comment=self.comment,
            csv_cols=self.csv_cols,
            encoding=self.encoding,
            format=self.format,
            null=self.null,
            row_separator=self.row_separator,
            skip=self.skip,
            trim=self.trim,
        )


@dataclass
class ExportQuery(SqlQuery):
    """
    Validate export parameters and build an EXPORT query.

    .. deprecated::
        Use ``pyexasol.query_builders.csv.builders.ExportBuilder`` instead.
    """

    # set these values in param dictionary to ExaConnection
    delimit: Delimit | None = None
    with_column_names: bool = False
    boolean: BooleanRepresentation | None = None

    def build_query(
        self,
        table: str | tuple[str, ...],
        exa_address_list: list[str],
    ) -> str:
        export_builder = self._get_export_builder(table)
        return export_builder.build_query(
            database_version=self.connection.exasol_db_version,
            encryption=self.connection.options["encryption"],
            exa_address_list=exa_address_list,
            formatter=self.connection.format,
        )

    @staticmethod
    def load_from_dict(
        connection: ExaConnection, compression: bool, params: dict
    ) -> ExportQuery:
        """
        Load the params dictionary into the ExportQuery class

        Keys in `params` that are not present in as attributes of the `ExportQuery`
        class will raise an Exception.
        """
        return ExportQuery(connection=connection, compression=compression, **params)

    def _get_export_builder(
        self, query_or_table: str | tuple[str, ...]
    ) -> ExportBuilder:
        return ExportBuilder(
            compression=self.compression,
            query_or_table=query_or_table,
            column_delimiter=self.column_delimiter,
            column_separator=self.column_separator,
            columns=list(self.columns) if self.columns is not None else None,
            comment=self.comment,
            csv_cols=list(self.csv_cols) if self.csv_cols is not None else None,
            delimit=self.delimit,
            encoding=self.encoding,
            format=self.format,
            null=self.null,
            row_separator=self.row_separator,
            with_column_names=self.with_column_names,
            boolean=self.boolean,
        )


class ExaSQLThread(threading.Thread):
    """
    Thread class which re-throws any Exception to parent thread
    """

    def __init__(
        self,
        connection: ExaConnection,
        compression: bool,
        worker_finished_event: threading.Event | None = None,
        query_builder: QueryBuilder | None = None,
    ):
        self.connection = connection
        self.compression = compression

        self.params: dict = {}
        self.http_thread = None
        self.exa_address_list: list[str] = []
        self.exc = None
        self.worker_finished_event = worker_finished_event
        self.query_builder = query_builder

        super().__init__()

    def set_http_thread(self, http_thread):
        self.http_thread = http_thread
        self.exa_address_list = [http_thread.exa_address]

    def set_exa_address_list(self, exa_address_list):
        self.exa_address_list = exa_address_list

    def run(self):
        try:
            self.run_sql()
        except BaseException as e:  # NOSONAR(S5754) re-raised by join_with_exc()
            self.exc = e

            # In case of SQL error stop HTTP server, close pipes and interrupt I/O in callback function
            if self.http_thread:
                self.http_thread.terminate()
        finally:
            if self.worker_finished_event:
                self.worker_finished_event.set()

    def run_sql(self):
        if self.query_builder is not None:
            query = self.query_builder.build_query(
                database_version=self.connection.exasol_db_version,
                encryption=self.connection.options["encryption"],
                exa_address_list=self.exa_address_list,
                formatter=self.connection.format,
            )
            self.connection.execute(query)

    def join_with_exc(self, *args):
        super().join(*args)
        if self.exc:
            raise self.exc


class ExaSQLExportThread(ExaSQLThread):
    """
    Build and run EXPORT query into separate thread
    Main thread is busy outputting data in callbacks

    .. deprecated::
        Use ``ExaSQLThread`` with an ``ExportBuilder`` as ``query_builder`` instead.
    """

    def __init__(
        self,
        connection: ExaConnection,
        compression: bool,
        query_or_table,
        export_params: dict,
        worker_finished_event: threading.Event | None = None,
    ):
        _warn_deprecated(
            "ExaSQLExportThread",
            "ExaSQLThread with an ExportBuilder as query_builder",
        )
        super().__init__(
            connection, compression, worker_finished_event=worker_finished_event
        )

        self.query_or_table = query_or_table
        self.params = export_params

    def run_sql(self):
        export_query = ExportQuery.load_from_dict(
            connection=self.connection, compression=self.compression, params=self.params
        ).build_query(
            table=self.query_or_table,
            exa_address_list=self.exa_address_list,
        )
        self.connection.execute(export_query)


class ExaSQLImportThread(ExaSQLThread):
    """
    Build and run IMPORT query into separate thread
    Main thread is busy parsing results in callbacks

    .. deprecated::
        Use ``ExaSQLThread`` with an ``ImportBuilder`` as ``query_builder`` instead.
    """

    def __init__(
        self,
        connection: ExaConnection,
        compression: bool,
        table: str | tuple[str, ...],
        import_params: dict,
        worker_finished_event: threading.Event | None = None,
    ):
        _warn_deprecated(
            "ExaSQLImportThread",
            "ExaSQLThread with an ImportBuilder as query_builder",
        )
        super().__init__(
            connection, compression, worker_finished_event=worker_finished_event
        )

        self.table = table
        self.params = import_params

    def run_sql(self):
        import_query = ImportQuery.load_from_dict(
            connection=self.connection, compression=self.compression, params=self.params
        ).build_query(table=self.table, exa_address_list=self.exa_address_list)
        self.connection.execute(import_query)


class ExaHttpThread(threading.Thread):
    """
    HTTP communication and compression / decompression is offloaded to a separate thread.
    PyExasol uses a thread instead of a subprocess or multiprocessing to avoid
    compatibility issues on Windows operating systems. For further details, see
    - https://github.com/exasol/pyexasol/issues/73
    - https://pythonforthelab.com/blog/differences-between-multiprocessing-windows-and-linux/
    """

    def __init__(
        self,
        ipaddr: str,
        port: int,
        compression: bool,
        encryption: bool,
        worker_finished_event: threading.Event | None = None,
    ):
        self.server = ExaTCPServer(
            (ipaddr, port),
            ExaHttpRequestHandler,
            compression=compression,
            encryption=encryption,
        )

        self.read_pipe = self.server.read_pipe
        self.write_pipe = self.server.write_pipe
        self.worker_finished_event = worker_finished_event

        self.exc = None

        super().__init__()

    @property
    def exa_address(self) -> str:
        return self.server.exa_address

    def run(self):
        try:
            # Handle exactly one HTTP request
            # Exit loop if thread was explicitly terminated prior to receiving HTTP request
            self.server.serve_first_client()
        except BaseException as e:  # NOSONAR(S5754) re-raised by join_with_exc()
            self.exc = e
        finally:
            self.server.server_close()
            if self.worker_finished_event is not None:
                self.worker_finished_event.set()

    def join(self, timeout=None):
        self.server.can_finish_get.set()
        super().join(timeout)

    def join_with_exc(self):
        self.join()

        if self.exc:
            raise self.exc

    def terminate(self):
        self.server.is_terminated = True
        self.server.can_finish_get.set()

        # Must close pipes here to prevent infinite lock in callback function
        # Termination pipe order is important for Windows
        self.write_pipe.close()
        self.read_pipe.close()


class ExaHTTPTransportWrapper:
    """
    Wrapper for :ref:`http_transport_parallel`.

    You may create this wrapper using :func:`pyexasol.http_transport`.

    Note:

        Starts an HTTP server, obtains the address (the ``"ipaddr:port"`` string),
        and sends it to the parent process.

        Block into ``export_*()`` or ``import_*()`` call,
        wait for incoming connection, process data and exit.
    """

    def __init__(
        self,
        ipaddr: str,
        port: int,
        compression: bool = False,
        encryption: bool = True,
    ):
        self.http_thread = ExaHttpThread(ipaddr, port, compression, encryption)
        self.http_thread.start()

    @property
    def exa_address(self) -> str:
        """
        Internal Exasol address as ``ipaddr:port`` string.

        Note:
            This string should be passed from child processes to parent process
            and used as an argument for ``export_parallel()`` and
            ``import_parallel()`` functions.
        """
        return self.http_thread.exa_address

    def get_proxy(self):
        """
        Caution:
            **DEPRECATED**, please use ``.exa_address`` property
        """
        return self.http_thread.exa_address

    def export_to_callback(self, callback, dst, callback_params=None):
        """
        Exports chunk of data using callback function.

        Args:
            callback:
                Callback function.
            dst:
                Export destination for callback function.
            callback_params:
                Dict with additional parameters for callback function.

        Returns:
            Result of the callback function.

        Note:
            You may use exactly the same callbacks utilized by standard
            non-parallel ``export_to_callback()`` function.
        """
        if not callable(callback):
            raise ValueError("Callback argument is not callable")

        if callback_params is None:
            callback_params = {}

        try:
            result = callback(self.http_thread.read_pipe, dst, **callback_params)

            self.http_thread.read_pipe.close()
            self.http_thread.join_with_exc()

            return result

        except (Exception, KeyboardInterrupt) as e:
            self.http_thread.terminate()
            self.http_thread.join()

            raise e

    def import_from_callback(self, callback, src, callback_params=None):
        """
        Import chunk of data using callback function.

        Args:
            callback:
                Callback function.
            src:
                Import source for the callback function.
            callback_params:
                Dict with additional parameters for the callback function.

        Returns:
            Result of callback function

        Note:
            You may use exactly the same callbacks utilized by standard
            non-parallel ``import_from_callback()`` function.
        """
        if not callable(callback):
            raise ValueError("Callback argument is not callable")

        if callback_params is None:
            callback_params = {}

        try:
            result = callback(self.http_thread.write_pipe, src, **callback_params)

            self.http_thread.write_pipe.close()
            self.http_thread.join_with_exc()

            return result

        except (Exception, KeyboardInterrupt) as e:
            self.http_thread.terminate()
            self.http_thread.join()

            raise e

    def __repr__(self):
        return f"<{self.__class__.__name__} exa_address={self.exa_address}>"


class ExaTCPServer(ExaTunnelServer):
    """Tunnel server for CSV transfers, which stream data through a pipe."""

    def __init__(self, *args, **kwargs) -> None:
        self.compression: bool = kwargs.pop("compression", False)

        r_fd, w_fd = os.pipe()

        self.read_pipe = open(r_fd, "rb", 0)
        self.write_pipe = open(w_fd, "wb", 0)

        # GET method calls (IMPORT) require extra protection
        #
        # Callback function may close pipe abruptly and raise an exception
        # It may cause partial valid data to be sent to Exasol server
        #
        # This event waits for callback function to finish before sending final chunk of data
        #
        # If callback function failed with exception, HTTP thread will be terminated
        # Final chunk will not be sent, causing IMPORT query to fail and to discard partial data
        self.can_finish_get = threading.Event()

        super().__init__(*args, **kwargs)


class ExaHttpRequestHandler(socketserver.StreamRequestHandler):
    success_headers = (
        b"HTTP/1.1 200 OK\r\n"
        b"Connection: close\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"\r\n"
    )

    error_headers = (
        b"HTTP/1.1 500 Internal Server Error\r\n" b"Connection: close\r\n" b"\r\n"
    )

    server: ExaTCPServer

    def handle(self):
        self.server.total_clients += 1

        # Extract method from the first header
        method = str(self.rfile.readline(), "iso-8859-1").split()[0]

        # Skip all other headers
        while self.rfile.readline() != b"\r\n":
            pass

        if method == "PUT":
            if self.server.compression:
                self.method_put_compressed()
            else:
                self.method_put_raw()
        elif method == "GET":
            if self.server.compression:
                self.method_get_compressed()
            else:
                self.method_get_raw()
        else:
            raise RuntimeError(f"Unsupported HTTP method [{method}]")

    def method_put_raw(self):
        try:
            while not self.server.is_terminated:
                data = self.read_chunk()

                if data is None:
                    break

                self.server.write_pipe.write(data)

        except Exception as e:
            self.write_error_headers()
            raise e

        else:
            self.write_success_headers()
            self.write_final_chunk()

        finally:
            self.server.write_pipe.close()

    def method_put_compressed(self):
        try:
            d = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)

            while not self.server.is_terminated:
                data = self.read_chunk()

                if data is None:
                    self.server.write_pipe.write(d.flush())
                    break

                self.server.write_pipe.write(d.decompress(data))

        except Exception as e:
            self.write_error_headers()
            raise e

        else:
            self.write_success_headers()
            self.write_final_chunk()

        finally:
            self.server.write_pipe.close()

    def method_get_raw(self):
        try:
            self.write_success_headers()

            while not self.server.is_terminated:
                # Exasol server produces chunks of this size (without chunk header part)
                data = self.server.read_pipe.read(65524)

                if data is None or len(data) == 0:
                    break

                self.write_chunk(data)

        except Exception as e:
            raise e

        finally:
            self.server.read_pipe.close()

        if self.server.can_finish_get.wait() and not self.server.is_terminated:
            self.write_final_chunk()

    def method_get_compressed(self):
        try:
            self.write_success_headers()
            c = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)

            while not self.server.is_terminated:
                #  Linux common pipe buffer, 64Kb
                data = self.server.read_pipe.read(65536)

                if data is None or len(data) == 0:
                    self.write_chunk(c.flush(zlib.Z_FINISH))
                    break

                self.write_chunk(c.compress(data))

        except Exception as e:
            raise e

        finally:
            self.server.read_pipe.close()

        if self.server.can_finish_get.wait() and not self.server.is_terminated:
            self.write_final_chunk()

    def read_chunk(self):
        hex_length = self.rfile.readline().rstrip()

        if len(hex_length) == 0:
            chunk_len = 0
        else:
            chunk_len = int(hex_length, 16)

        if chunk_len == 0:
            return None

        data = self.rfile.read(chunk_len)

        if self.rfile.read(2) != b"\r\n":
            raise RuntimeError("Invalid chunk delimiter in HTTP stream")

        return data

    def write_chunk(self, data):
        chunk_len = len(data)

        if chunk_len == 0:
            return

        self.wfile.write(b"%X\r\n%b\r\n" % (chunk_len, data))

    def write_final_chunk(self):
        self.wfile.write(b"0\r\n\r\n")

    def write_success_headers(self):
        self.wfile.write(self.success_headers)

    def write_error_headers(self):
        self.wfile.write(self.error_headers)
