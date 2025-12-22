import base64
import getpass
import hashlib
import itertools
import platform
import random
import re
import socket
import ssl
import threading
import time
import urllib.parse
import zlib
from collections.abc import (
    Callable,
    Iterable,
)
from inspect import (
    Signature,
    cleandoc,
    signature,
)
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    NamedTuple,
    Union,
)
from warnings import warn

import websocket
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from packaging.version import Version

from . import callback as cb
from .exceptions import *
from .ext import ExaExtension
from .formatter import ExaFormatter
from .http_transport import (
    ExaHttpThread,
    ExaSQLExportThread,
    ExaSQLImportThread,
)
from .logger import ExaLogger
from .meta import ExaMetaData
from .script_output import ExaScriptOutputProcess
from .statement import ExaStatement
from .version import __version__
from .warnings import PyexasolWarning

if TYPE_CHECKING:
    import pandas
    import polars


class Host(NamedTuple):
    """This represents a resolved host name with its IP address and port number."""

    hostname: str
    ip_address: str | None
    port: int
    fingerprint: str | None


def get_exaconnection_signature() -> Signature:
    return signature(ExaConnection.__init__)


class ExaConnection:
    """
    Warning:
        Threads may share the module, but not connections
        One connection may be used by different threads, just not at the same time
        :meth:`pyexasol.ExaConnection.abort_query` is an exception,
        it is meant to be called from another thread

    Note:

        It is advisable to use multiprocessing instead of threading and create
        a new connection in each sub-process

        Public Attributes:
            ``attr``:
                Read-only ``dict`` of attributes of current connection.

            ``login_info``:
                Read-only ``dict`` of login information returned by second
                response of LOGIN command.

            ``options``:
                Read-only ``dict`` of arguments passed to
                :meth:`pyexasol.ExaConnection.connect`.
    """

    cls_statement = ExaStatement
    cls_formatter = ExaFormatter
    cls_logger = ExaLogger
    cls_extension = ExaExtension
    cls_meta = ExaMetaData

    threadsafety = 1

    def __init__(
        self,
        dsn: str | None = None,
        user: str | None = None,
        password: str | None = None,
        schema: str = "",
        autocommit=constant.DEFAULT_AUTOCOMMIT,
        snapshot_transactions=None,
        connection_timeout=constant.DEFAULT_CONNECTION_TIMEOUT,
        socket_timeout=constant.DEFAULT_SOCKET_TIMEOUT,
        query_timeout=constant.DEFAULT_QUERY_TIMEOUT,
        compression: bool = False,
        encryption: bool = True,
        fetch_dict: bool = False,
        fetch_mapper=None,
        fetch_size_bytes=constant.DEFAULT_FETCH_SIZE_BYTES,
        lower_ident: bool = False,
        quote_ident: bool = False,
        json_lib: str = "json",
        verbose_error: bool = True,
        debug: bool = False,
        debug_logdir=None,
        udf_output_bind_address=None,
        udf_output_connect_address=None,
        udf_output_dir=None,
        http_proxy=None,
        resolve_hostnames: bool = True,
        client_name=None,
        client_version=None,
        client_os_username=None,
        protocol_version=constant.PROTOCOL_V3,
        websocket_sslopt: dict | None = None,
        access_token: str | None = None,
        refresh_token: str | None = None,
    ):
        """
        Exasol connection object

        Args:
            dsn:
                Connection string, same format as standard JDBC / ODBC drivers
                (e.g. 10.10.127.1..11:8564)
            user:
                Username
            password:
                Password
            schema:
                Open schema after connection
                (Default: '', no schema)
            autocommit:
                Enable autocommit on connection
                (Default: True)
            snapshot_transactions:
                Explicitly enable or disable snapshot transactions on connection
                (Default: None, database default)
            connection_timeout:
                Socket timeout in seconds used to establish connection
                (Default: 10)
            socket_timeout:
                Socket timeout in seconds used for requests after connection was established
                (Default: 30)
            query_timeout:
                Maximum execution time of queries before automatic abort, in seconds
                (Default: 0, no timeout)
            compression:
                Use zlib compression both for WebSocket and HTTP transport
                (Default: False)
            encryption:
                Use SSL to encrypt client-server communications for WebSocket and HTTP transport
                (Default: True)
            fetch_dict:
                Fetch result rows as dicts instead of tuples (Default: False)
            fetch_mapper:
                Use custom mapper function to convert Exasol values into
                Python objects during fetching
                (Default: None)
            fetch_size_bytes:
                Maximum size of data message for single fetch request in bytes
                (Default: 5Mb)
            lower_ident:
                Automatically lowercase identifiers (table names, column names, etc.)
                returned from relevant functions
                (Default: False)
            quote_ident:
                Add double quotes and escape identifiers passed to relevant functions
                (export_*, import_*, ext.*, etc.)
                (Default: False)
            json_lib:
                Supported values: rapidjson, ujson, orjson, json
                (Default: json)
            verbose_error:
                Display additional information when error occurs
                (Default: True)
            debug:
                Output debug information for client-server communication and
                connection attempts to STDERR
            debug_logdir:
                Store debug information into files in debug_logdir instead of
                outputting it to STDERR
            udf_output_bind_address:
                Specific server_address to bind TCP server for UDF script output
                (default: ('', 0))
            udf_output_connect_address:
                Specific SCRIPT_OUTPUT_ADDRESS value to connect from Exasol to
                UDF script output server
                (default: inherited from TCP server)
            udf_output_dir:
                Directory to store captured UDF script output logs, split by
                <session_id>_<statement_id>/<vm_num>
            http_proxy:
                HTTP proxy string in Linux http_proxy format
                (default: None)
            resolve_hostnames:
                Explicitly resolve host names to IP addresses before connecting.
                Deactivating this will let the operating system resolve the host name
                (default: True)
            client_name:
                Custom name of client application displayed in Exasol sessions tables
                (Default: PyExasol)
            client_version:
                Custom version of client application
                (Default: pyexasol.__version__)
            client_os_username:
                Custom OS username displayed in Exasol sessions table
                (Default: getpass.getuser())
            protocol_version:
                Major WebSocket protocol version requested for connection
                (Default: pyexasol.PROTOCOL_V3)
            websocket_sslopt:
                Set custom SSL options for WebSocket client
                (Default: None)
            access_token:
                OpenID access token to use for the login process
            refresh_token:
                OpenID refresh token to use for the login process
        """

        # convert all arguments to a dict[argument_name, argument_value]
        sig = get_exaconnection_signature()
        all_locals = locals()
        self.options = {
            param.name: all_locals[param.name]
            for param in sig.parameters.values()
            if param.name != "self"
        }

        self.login_info: dict = {}
        self.login_time = 0
        self.attr: dict = {}
        self.is_closed: bool = False

        self.ws_req_count = 0
        self.ws_req_time = 0

        self.last_stmt = None
        self.stmt_count = 0

        self.json_encode = None
        self.json_decode = None

        self._udf_output_count = 0
        self._req_lock = threading.Lock()

        self._init_format()
        self._init_json()
        self._init_ext()
        self._init_meta()

        self._init_logger()
        self._init_ws()

        self._login()
        self.get_attr()

    def execute(self, query: str, query_params: dict | None = None) -> ExaStatement:
        """
        Execute SQL query with optional query formatting parameters

        Args:
            query:
                SQL query text, possibly with placeholders
            query_params:
                 Values for placeholders

        Returns:
            ExaStatement object

        Examples:

            >>> con = ExaConnection(...)
            >>> con.execute(
            ...        query="SELECT * FROM {table!i} WHERE col1={col1}",
            ...        query_params={'table': 'users', 'col1':'bar'}
            ...)
        """
        return self.cls_statement(self, query, query_params)

    def execute_udf_output(self, query: str, query_params: dict | None = None):
        """
        Execute SQL query with UDF script, capture output

        Note:
            Exasol should be able to open connection to the machine where current script is running.
            It is usually OK in the same data centre, but it is normally not working
            if you try to run this function on local laptop.

        Args:
            query:
                SQL query text, possibly with placeholders
            query_params:
                Values for placeholders

        Returns:
            Return tuple with two elements: (1) instance of :class:`pyexasol.ExaStatement`
            and (2) list of :class:`Path` objects for script output log files.

        Attention:
            Exasol should be able to open connection to the machine where current script is running

        Examples:
            >>> con = ExaConnection(...)
            >>> stmt, output_files = con.execute_udf_output(
            ...        query="SELECT * FROM {table!i} WHERE col1={col1}",
            ...        query_params={'table': 'users', 'col1':'bar'}
            ...)
        """
        stmt_output_dir = self._get_stmt_output_dir()

        script_output = ExaScriptOutputProcess(
            (
                self.options["udf_output_bind_address"][0]
                if self.options["udf_output_bind_address"]
                else None
            ),
            (
                self.options["udf_output_bind_address"][1]
                if self.options["udf_output_bind_address"]
                else None
            ),
            stmt_output_dir,
        )

        try:
            script_output.start()

            # This option is useful to get around complex network setups, like Exasol running in Docker containers
            if self.options["udf_output_connect_address"]:
                address = f"{self.options['udf_output_connect_address'][0]}:{self.options['udf_output_connect_address'][1]}"
            else:
                address = script_output.get_output_address()

            self.execute(
                "ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = {address}",
                {"address": address},
            )

            stmt = self.execute(query, query_params)
            log_files = sorted(list(stmt_output_dir.glob("*.log")))

            if len(log_files) > 0:
                script_output.join_with_exc()
            else:
                # In some cases Exasol does not run any VM's even when UDF scripts are being called
                # In this case we must terminate TCP server, since it won't stop automatically
                script_output.terminate()
                script_output.join()
        except ExaQueryError:
            script_output.terminate()
            script_output.join()

            raise

        return stmt, log_files

    def commit(self):
        """Wrapper for query 'COMMIT'"""
        return self.execute("COMMIT")

    def rollback(self):
        """Wrapper for query 'ROLLBACK'"""
        return self.execute("ROLLBACK")

    def set_autocommit(self, val: str) -> None:
        """
        Set autocommit mode.

        Args:
            val:
                Set ``False`` to execute following statements in transaction.
                Set ``True`` to get back to automatic COMMIT after each statement.

        Note:
            Autocommit is ``True`` by default because Exasol has to commit indexes and statistics
            objects even for pure SELECT statements. Lack of default COMMIT may lead to serious
            performance degradation.
        """
        if not isinstance(val, bool):
            raise ValueError("Autocommit value must be boolean")

        self.set_attr({"autocommit": val})

    def set_query_timeout(self, val):
        """
        Set the maximum time in seconds for which a query can run before Exasol kills it automatically.

        Args:
            val:
                Timeout value in seconds.
                Set value ``0`` to disable timeout.

        Note:
            It is highly recommended to set timeout for UDF scripts to
            avoid potential infinite loops and very long transactions.
        """
        self.set_attr({"queryTimeout": int(val)})

    def open_schema(self, schema):
        """
        Wrapper for `OPEN SCHEMA`

        Args:
            schema: Schema name
        """
        self.set_attr({"currentSchema": self.format.default_format_ident_value(schema)})

    def current_schema(self):
        """
        Get the name of the current schema.

        Returns:
            Name of currently opened schema. Returns an empty string if no schema was opened.
        """
        return self.attr.get("currentSchema", "")

    def export_to_file(
        self,
        dst,
        query_or_table: str,
        query_params: dict | None = None,
        export_params: dict | None = None,
    ):
        """
        Export large amount of data from Exasol to file or file-like object using fast HTTP transport.

        Note:
            File must be opened in binary mode.

        Args:
            dst:
                Path to file or file-like object where data will be exported to.
            query_or_table:
                SQL query or table from which to export data.
            query_params:
                Values for SQL query placeholders.
            export_params:
                Custom parameters for EXPORT query.

        Examples:
            >>> con = ExaConnection(...)
            >>> with open('/tmp/file.csv', 'wb') as f:
            ...     con.export_to_file(
            ...         dst=f,
            ...         query_or_table="SELECT * FROM table"
            ...     )
        """
        return self.export_to_callback(
            cb.export_to_file, dst, query_or_table, query_params, None, export_params
        )

    def export_to_list(
        self,
        query_or_table: str,
        query_params: dict | None = None,
        export_params: dict | None = None,
    ) -> list:
        """
        Export large amount of data from Exasol to basic Python `list` using fast HTTP transport.

        Args:
            query_or_table:
                SQL query or table from which to export data.
            query_params:
                Values for SQL query placeholders.
            export_params:
                Custom parameters for EXPORT query.

        Returns:
            `list` of `tuples`

        Warnings:
            - This function may run out of memory

        Examples:
            >>> con = ExaConnection(...)
            >>> myresult = con.export_to_list(
            ...    query_or_table="SELECT * FROM table"
            ... )
        """
        return self.export_to_callback(
            cb.export_to_list, None, query_or_table, query_params, None, export_params
        )

    def export_to_pandas(
        self,
        query_or_table: str,
        query_params: dict | None = None,
        callback_params: dict | None = None,
        export_params: dict | None = None,
    ) -> "pandas.DataFrame":
        """
        Export large amount of data from Exasol to :class:`pandas.DataFrame`.

        Args:
            query_or_table:
                SQL query or table from which to export data.
            query_params:
                Values for SQL query placeholders.
            callback_params:
                Dictionary with additional parameters for callback function
                `pandas.read_csv <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html>`__.
            export_params:
                 Custom parameters for EXPORT query.

        Returns:
            instance of :class:`pandas.DataFrame`

        Warnings:
            - This function may run out of memory

        Examples:
            >>> con = ExaConnection(...)
            >>> myresult = con.export_to_pandas(
            ...    query_or_table="SELECT * FROM table"
            ... )
        """
        if not export_params:
            export_params = {}

        export_params["with_column_names"] = True

        return self.export_to_callback(
            cb.export_to_pandas,
            None,
            query_or_table,
            query_params,
            callback_params,
            export_params,
        )

    def export_to_parquet(
        self,
        dst: Path | str,
        query_or_table: str,
        query_params: dict | None = None,
        callback_params: dict | None = None,
        export_params: dict | None = None,
    ):
        """
        Export large amounts of data from Exasol to local parquet file(s).

        Args:
            dst:
                Local path to directory for exporting files. Can be one either a Path or
                str. **The default behavior, which can be changed via** ``callback_params``,
                **is that the specified directory should be empty.** If that is not
                the case, one of these exceptions may be thrown:

                    ValueError
                        '<dst>' exists and is not a directory
                    ValueError
                        '<dst>' contains existing files and `callback_params['existing_data_behavior']` is not one of these values: ("overwrite_or_ignore", "delete_matching").
                    pyarrow.lib.ArrowInvalid:
                        Could not write to <dst> Parquet Export from Exasol via Python Container/parquet as the directory is not empty and existing_data_behavior is to error
                    ValueError:
                        I/O operation on closed file.
                    DB error message:
                        ETL-5106: Following error occured while writing data to external connection [https://172.0.0.1:8653/000.csv failed after 200009 bytes. [OpenSSL SSL_read: SSL_ERROR_SYSCALL, errno 0],[56],[Failure when receiving data from the peer]] (Session: XXXXX)

                The ValueError exceptions would come from a check we provide via :func:`pyexasol.callback.check_export_to_parquet_directory_setting`.
                The purpose of calling this check is to detect issues before executing code within the callback pattern, which uses three threads.
                If a user has a different issue than we anticipated, it's possible the one of the other three exceptions is tossed for this, as
                discussed on `Importing and Exporting Data <https://exasol.github.io/pyexasol/master/user_guide/exploring_features/import_and_export/index.html>`__.

            query_or_table:
                SQL query or table from which to export data.
            query_params:
                Values for SQL query placeholders.
            callback_params:
                Dictionary with additional parameters for callback function
                `pyarrow.dataset.write_dataset <https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html>`__.
                Some important defaults to note are:

                existing_data_behavior
                   Set to ``error``, which requires that the specified ``dst`` not
                   contain any files or an exception will be thrown.
                max_rows_per_file
                   Set to ``0``, which means that all rows will be written to 1 file.
                   If ``max_rows_per_file`` is altered, ensure that ``max_rows_per_group``
                   is set to a value less than or equal to the value of ``max_rows_per_file``.
                use_threads
                   Set to ``True`` and ``preserve_order`` is set to ``False``. This means
                   that the writing of multiple files will be done in parallel and that
                   the order is not guaranteed to be preserved.
            export_params:
                Custom parameters for EXPORT query.
        """
        if not export_params:
            export_params = {}

        cb.check_export_to_parquet_directory_setting(
            dst=dst, callback_params=callback_params
        )

        export_params["with_column_names"] = True

        return self.export_to_callback(
            cb.export_to_parquet,
            dst,
            query_or_table,
            query_params,
            callback_params,
            export_params,
        )

    def export_to_polars(
        self,
        query_or_table: str,
        query_params: dict | None = None,
        callback_params: dict | None = None,
        export_params: dict | None = None,
    ) -> "polars.DataFrame":
        """
        Export large amount of data from Exasol to :class:`polars.DataFrame`.

        Args:
            query_or_table:
                SQL query or table from which to export data.
            query_params:
                Values for SQL query placeholders.
            callback_params:
                Dictionary with additional parameters for callback function
                `polars.read_csv <https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html>`__.
            export_params:
                Custom parameters for EXPORT query.

        Returns:
            instance of :class:`polars.DataFrame`

        Warnings:
            - This function may run out of memory

        Examples:
            >>> con = ExaConnection(...)
            >>> df = con.export_to_polars(
            ...    query_or_table="SELECT * FROM table"
            ... )
        """
        if not export_params:
            export_params = {}

        export_params["with_column_names"] = True

        return self.export_to_callback(
            cb.export_to_polars,
            None,
            query_or_table,
            query_params,
            callback_params,
            export_params,
        )

    def import_from_file(self, src, table: str, import_params: dict | None = None):
        """
        Import a large amount of data from a file or file-like object.

        Args:
            src:
                Source file or file-like object.
            table:
                Destination table for IMPORT.
            import_params:
                Custom parameters for IMPORT query.

        Note:
            File must be opened in binary mode.
        """
        return self.import_from_callback(
            cb.import_from_file, src, table, None, import_params
        )

    def import_from_iterable(
        self, src: Iterable, table: str, import_params: dict | None = None
    ):
        """
        Import a large amount of data from an ``iterable`` Python object.

        Args:
            src:
                Source object implementing ``__iter__``.
                Iterator must return tuples of values.
            table:
                Destination table for IMPORT.
            import_params:
                Custom parameters for IMPORT query.
        """
        return self.import_from_callback(
            cb.import_from_iterable, src, table, None, import_params
        )

    def import_from_pandas(
        self,
        src: "pandas.DataFrame",
        table: str,
        callback_params: dict | None = None,
        import_params: dict | None = None,
    ):
        """
        Import a large amount of data from :class:`pandas.DataFrame`.

        Args:
            src:
                Source :class:`pandas.DataFrame` instance.
            table:
                Destination table for IMPORT.
            callback_params:
                Dictionary with additional parameters for callback function
                `pandas.DataFrame.to_csv <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html>`__.
            import_params:
                Custom parameters for IMPORT query.
        """
        return self.import_from_callback(
            cb.import_from_pandas, src, table, callback_params, import_params
        )

    def import_from_polars(
        self,
        src: Union["polars.LazyFrame", "polars.DataFrame"],
        table: str,
        callback_params: dict | None = None,
        import_params: dict | None = None,
    ):
        """
        Import a large amount of data from :class:`polars.DataFrame` or :class:`polars.LazyFrame`.

        Args:
            src:
                Source :class:`polars.DataFrame` or :class:`polars.LazyFrame` instance.
            table:
                Destination table for IMPORT.
            callback_params:
                Dictionary with additional parameters for callback function
                `polars.DataFrame.write_csv <https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_csv.html>`__.
            import_params:
                Custom parameters for IMPORT query.
        """
        return self.import_from_callback(
            cb.import_from_polars, src, table, callback_params, import_params
        )

    def import_from_parquet(
        self,
        source: list[Path] | Path | str,
        table: str,
        callback_params: dict | None = None,
        import_params: dict | None = None,
    ):
        """
        Import a large amount of data from :class:`pyarrow.parquet.Table`.

        Args:
            source: Local filepath specification(s) to process. Can be one of:
                - list[pathlib.Path]: list of specific files
                - pathlib.Path: can be either a file or directory. If it's a directory,
                all files matching this pattern *.parquet will be processed.
                - str: representing a filepath which already contains a glob pattern
                (e.g., "/local_dir/*.parquet")
            table:
                Destination table for IMPORT.
            callback_params:
                Dict with additional parameters for callback function
                `parquet.ParquetFile.iter_batches <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html#pyarrow.parquet.ParquetFile.iter_batches>`__.
            import_params:
                Custom parameters for IMPORT query.
        """
        return self.import_from_callback(
            cb.import_from_parquet, source, table, callback_params, import_params
        )

    def export_to_callback(
        self,
        callback: Callable,
        dst,
        query_or_table: str,
        query_params: dict | None = None,
        callback_params: dict | None = None,
        export_params: dict | None = None,
    ):
        """
        Export a large amount of data to a user-defined callback function

        Args:
            callback:
                Callback function.
            dst:
                (optional) Path to file or file-like object where data will be exported to.
            query_or_table:
                SQL query or table from which to export data.
            query_params:
                Values for SQL query placeholders.
            callback_params:
                Dictionary with additional parameters for callback function.
            export_params:
                Custom parameters for EXPORT query.

        Returns:
            result of callback function

        Warnings:
            - This function may run out of memory

        Examples:
            >>> cb = lambda args: print(args)
            >>> con = ExaConnection(...)
            >>> con.export_to_callback(
            ...    callback=cb,
            ...    query_or_table="SELECT * FROM table"
            ... )
        """
        if not callable(callback):
            raise ValueError("Callback argument is not callable")

        if callback_params is None:
            callback_params = {}

        if export_params is None:
            export_params = {}

        if query_params is not None:
            query_or_table = self.format.format(query_or_table, **query_params)

        compression = (
            False if ("format" in export_params) else self.options["compression"]
        )

        http_thread = ExaHttpThread(
            self.ws_ipaddr,  # type: ignore
            self.ws_port,  # type: ignore
            compression,
            self.options["encryption"],
        )
        sql_thread = ExaSQLExportThread(
            self, compression, query_or_table, export_params
        )

        try:
            http_thread.start()

            sql_thread.set_http_thread(http_thread)
            sql_thread.start()

            with http_thread.read_pipe as pipe:
                result = callback(pipe, dst, **callback_params)

            http_thread.join_with_exc()
            sql_thread.join_with_exc()

            return result

        except (Exception, KeyboardInterrupt) as e:
            http_thread.terminate()
            http_thread.join()

            sql_thread.join(1)

            # Prevent infinite lock if SQL query is still running
            if sql_thread.is_alive():
                self.abort_query()
                sql_thread.join()

            # Give SQL exception higher priority
            if sql_thread.exc:
                raise sql_thread.exc

            raise e

    def import_from_callback(
        self,
        callback: Callable,
        src,
        table: str,
        callback_params: dict | None = None,
        import_params: dict | None = None,
    ):
        """
        Import a large amount of data from a user-defined callback function.

        Args:
            callback:
                Callback function.
            src:
                Source for the callback function.
            table:
                Destination table for IMPORT.
            callback_params:
                Dictionary with additional parameters for callback function.
            import_params:
                Custom parameters for IMPORT query.

        Raises:
            ValueError: callback argument isn't callable.
        """
        if callback_params is None:
            callback_params = {}

        if import_params is None:
            import_params = {}

        compression = (
            False if ("format" in import_params) else self.options["compression"]
        )

        if not callable(callback):
            raise ValueError("Callback argument is not callable")

        http_thread = ExaHttpThread(
            self.ws_ipaddr,  # type: ignore
            self.ws_port,  # type: ignore
            compression,
            self.options["encryption"],
        )
        sql_thread = ExaSQLImportThread(self, compression, table, import_params)

        try:
            http_thread.start()

            sql_thread.set_http_thread(http_thread)
            sql_thread.start()

            with http_thread.write_pipe as pipe:
                result = callback(pipe, src, **callback_params)

            http_thread.join_with_exc()
            sql_thread.join_with_exc()

            return result

        except (Exception, KeyboardInterrupt) as e:
            http_thread.terminate()
            http_thread.join()

            sql_thread.join(1)

            # Prevent infinite lock if SQL query is still running
            if sql_thread.is_alive():
                self.abort_query()
                sql_thread.join()

            # Give SQL exception higher priority
            if sql_thread.exc:
                raise sql_thread.exc

            raise e

    def export_parallel(
        self, exa_address_list, query_or_table, query_params=None, export_params=None
    ):
        """
        This function is part of :ref:`http_transport_parallel` API.

        Args:
            exa_address_list:
                List of ``ipaddr:port`` strings obtained from HTTP transport ``.address``.
            query_or_table:
                SQL query or table for the export.
            query_params:
                Values for SQL query placeholders.
            export_params:
                Custom parameters for Export query.
        Note:
            - Init HTTP transport in child processes first using pyexasol.http_transport()
            - Get internal Exasol address from each child process using .address
            - Pass address strings to parent process, combine into single list and use it for export_parallel() call
        """
        if export_params is None:
            export_params = {}

        compression = (
            False if ("format" in export_params) else self.options["compression"]
        )

        if query_params is not None:
            query_or_table = self.format.format(query_or_table, **query_params)

        # There is no need to actually run a separate thread here, all work is performed in separate processes
        # We simply reuse thread class to keep logic in one place
        sql_thread = ExaSQLExportThread(
            self, compression, query_or_table, export_params
        )
        sql_thread.set_exa_address_list(exa_address_list)
        sql_thread.run_sql()

    def import_parallel(self, exa_address_list, table, import_params=None):
        """
        This function is part of :ref:`http_transport_parallel` API.

        Args:
            exa_address_list:
                List of ``ipaddr:port`` strings obtained from HTTP transport ``.address``.
            table:
                Table to import to.
            import_params:
                Custom parameters for import.

        Note:
            - Init HTTP transport in child processes first using pyexasol.http_transport()
            - Get internal Exasol address from each child process using .address
            - Pass address strings to parent process, combine into single list and use it for import_parallel() call

        """
        if import_params is None:
            import_params = {}

        compression = (
            False if ("format" in import_params) else self.options["compression"]
        )

        # There is no need to actually run a separate thread here, all work is performed in separate processes
        # We simply reuse thread class to keep logic in one place
        sql_thread = ExaSQLImportThread(self, compression, table, import_params)
        sql_thread.set_exa_address_list(exa_address_list)
        sql_thread.run_sql()

    def session_id(self) -> str:
        """
        Session id of current session.

        Returns:
            Unique `SESSION_ID` of the current session as string.
        """
        return str(self.login_info.get("sessionId", ""))

    def protocol_version(self) -> int:
        """
        Actual protocol version used by the established connection.

        Returns:
            ``0`` if connection was not established yet (e.g. due to exception handling), otherwise protocol version as int.

        Warnings:
            Actual Protocol version might be downgraded from requested protocol version if Exasol server does not support it

        Note:
            The actual protocol version may be lower than the requested protocol version
            defined by the ``protocol_version`` connection option. For further details,
            refer to :ref:`protocol_version`.

        """
        return int(self.login_info.get("protocolVersion", 0))

    @property
    def exasol_db_version(self) -> Version | None:
        """
        Version of the Exasol database of the current session.

        The login information is returned by the second response of LOGIN command
        and calls this "releaseVersion".
        """
        if release_version := self.login_info.get("releaseVersion"):
            return Version(release_version)
        return None

    def last_statement(self) -> ExaStatement:
        """
        Last created statement object

        Returns:
            ExaStatement: last created statement.

        Note:
            It is mainly used for HTTP transport to access internal IMPORT / EXPORT query,
            measure execution time and number of rows

        Tip:
            It is useful while working with `export_*` and `import_*` functions normally
            returning result of callback function instead of statement object.
        """
        if self.last_stmt is None:
            raise ExaRuntimeError(self, "Last statement not found")

        return self.last_stmt

    def close(self, disconnect=True):
        """
        Closes connection to database.

        Args:
            disconnect:
                If ``true`` send optional "disconnect" command to free resources and close session on Exasol server side properly.

        Note:
            Please note that "disconnect" should always be False when .close() is being called from .req()-like functions
            to prevent an infinite loop if websocket exception happens during handling of "disconnect" command
        """
        if self._ws.connected:
            if disconnect:
                self.req({"command": "disconnect"})

            self.logger.debug("[WebSocket connection close]")
            self._ws.close()

        self.is_closed = True
        self.last_stmt = None

    def get_attr(self):
        ret = self.req(
            {
                "command": "getAttributes",
            }
        )

        self.attr = ret["attributes"]

    def set_attr(self, new_attr):
        self.req(
            {
                "command": "setAttributes",
                "attributes": new_attr,
            }
        )

        # At this moment setAttributes response is inconsistent, so attributes must be refreshed after every call
        self.get_attr()

    def get_nodes(self, pool_size=None):
        """
        List of currently active Exasol nodes which is normally used for :ref:`http_transport_parallel`.

        Args:
            pool_size:
                Return list of specific size.

        Returns:
            list of dictionaries describing active Exasol nodes

        Note:

            Format: ``{'ipaddr': <ip_address>, 'port': <port>, 'idx': <incremental index of returned node>}``

            - If pool_size is bigger than number of nodes, list will wrap around and nodes will repeat with different 'idx'
            - If pool_size is omitted, return every active node once
            - It is useful to balance workload for parallel IMPORT and EXPORT Exasol shuffles list for every connection
            - Exasol shuffles list for every connection.
        """
        ret = self.req(
            {
                "command": "getHosts",
                "hostIp": self.ws_ipaddr,
            }
        )

        if pool_size is None:
            pool_size = ret["responseData"]["numNodes"]

        # Key 'host' is deprecated and remains only for backwards compatibility, please use `ipaddr` instead
        return [
            {"host": ipaddr, "ipaddr": ipaddr, "port": self.ws_port, "idx": idx}
            for idx, ipaddr in enumerate(
                itertools.islice(
                    itertools.cycle(ret["responseData"]["nodes"]), pool_size
                ),
                start=1,
            )
        ]

    def req(self, req):
        """Send WebSocket request and wait for response"""
        self.ws_req_count += 1
        local_req_count = self.ws_req_count

        # Build request
        send_data = self.json_encode(req)
        self.logger.debug_json(f"WebSocket request #{local_req_count}", req)

        # Prevent and discourage attempts to use connection object from another thread simultaneously
        if not self._req_lock.acquire(blocking=False):
            self.logger.debug(f"[WebSocket request #{local_req_count} WAS NOT SENT]")
            raise ExaConcurrencyError(
                self,
                "Connection cannot be shared between multiple threads "
                "sending requests simultaneously",
            )

        # Send request, wait for response
        try:
            start_ts = time.time()

            self._ws_send(send_data)
            recv_data = self._ws_recv()

            self.ws_req_time = time.time() - start_ts
        except (websocket.WebSocketException, ConnectionError) as e:
            self.close(disconnect=False)
            raise ExaCommunicationError(self, str(e))
        finally:
            self._req_lock.release()

        if not recv_data:
            raise ExaCommunicationError(
                self, "Empty WebSocket response, connection was likely closed"
            )

        # Parse response
        ret = self.json_decode(recv_data)
        self.logger.debug_json(f"WebSocket response #{local_req_count}", ret)

        # Updated attributes may be returned from any request
        if "attributes" in ret:
            self.attr = {**self.attr, **ret["attributes"]}

        if ret["status"] == "ok":
            return ret

        if ret["status"] == "error":
            # Special treatment for "execute" command to prevent very long tracebacks in most common cases
            if req.get("command") in ["execute", "createPreparedStatement"]:
                if ret["exception"]["sqlCode"] == "R0001":
                    cls_err = ExaQueryTimeoutError
                elif ret["exception"]["sqlCode"] == "R0003":
                    cls_err = ExaQueryAbortError
                else:
                    cls_err = ExaQueryError

                raise cls_err(
                    self,
                    req["sqlText"],
                    ret["exception"]["sqlCode"],
                    ret["exception"]["text"],
                )
            elif req.get("username") is not None:
                raise ExaAuthError(
                    self, ret["exception"]["sqlCode"], ret["exception"]["text"]
                )
            else:
                raise ExaRequestError(
                    self, ret["exception"]["sqlCode"], ret["exception"]["text"]
                )

    def abort_query(self):
        """
        Abort running query

        Warnings:

            This function should be called from a separate thread and has no response
            Response should be checked in the main thread which started execution of query

            There are three possible outcomes of calling this function:

            #. Query is aborted normally, connection remains active
            #. Query was stuck in a state which cannot be aborted, so Exasol has to terminate connection
            #. Query might be finished successfully before abort call had a chance to take effect
        """
        req = {"command": "abortQuery"}

        send_data = self.json_encode(req)
        self.logger.debug_json("WebSocket abort request", req)

        try:
            self._ws_send(send_data)
        except (websocket.WebSocketException, ConnectionError) as e:
            self.close(disconnect=False)
            raise ExaCommunicationError(self, str(e))

    def _login(self):
        start_ts = time.time()

        if self.options["access_token"] or self.options["refresh_token"]:
            auth_params = self._login_token()
        else:
            auth_params = self._login_password()

        self.login_info = self.req(
            {
                **auth_params,
                "driverName": f"{constant.DRIVER_NAME} {__version__}",
                "clientName": (
                    self.options["client_name"]
                    if self.options["client_name"]
                    else constant.DRIVER_NAME
                ),
                "clientVersion": (
                    self.options["client_version"]
                    if self.options["client_version"]
                    else __version__
                ),
                "clientOs": platform.platform(),
                "clientOsUsername": (
                    self.options["client_os_username"]
                    if self.options["client_os_username"]
                    else getpass.getuser()
                ),
                "clientRuntime": f"Python {platform.python_version()}",
                "useCompression": self.options["compression"],
                "attributes": self._get_login_attributes(),
            }
        )["responseData"]

        self.login_time = time.time() - start_ts

        if self.options["compression"]:
            self._ws_send = lambda x: self._ws.send_binary(
                zlib.compress(x.encode() if isinstance(x, str) else x, 1)
            )
            self._ws_recv = lambda: zlib.decompress(self._ws.recv())

    def _login_password(self):
        ret = self.req(
            {
                "command": "login",
                "protocolVersion": self.options["protocol_version"],
            }
        )

        auth_params = {
            "username": self.options["user"],
            "password": self._encrypt_password(ret["responseData"]["publicKeyPem"]),
        }

        return auth_params

    def _login_token(self):
        self.req(
            {
                "command": "loginToken",
                "protocolVersion": self.options["protocol_version"],
            }
        )

        auth_params = {}

        if self.options["refresh_token"]:
            auth_params["refreshToken"] = self.options["refresh_token"]

        if self.options["access_token"]:
            auth_params["accessToken"] = self.options["access_token"]

        return auth_params

    def _encrypt_password(self, public_key_pem):
        public_key = serialization.load_pem_public_key(public_key_pem.encode())
        encrypted_data = public_key.encrypt(
            self.options["password"].encode(), padding.PKCS1v15()
        )
        return base64.b64encode(encrypted_data).decode()

    def _init_ws(self):
        """
        Init websocket connection

        Note:
            - Connection redundancy is supported
            - Specific Exasol node is randomly selected for every connection attempt
        """
        dsn_items = self._process_dsn(self.options["dsn"])
        failed_attempts = 0
        for hostname, ipaddr, port, fingerprint in dsn_items:
            try:
                self._ws = self._create_websocket_connection(
                    hostname, ipaddr, port, fingerprint
                )
            except Exception as e:
                failed_attempts += 1
                if failed_attempts == len(dsn_items):
                    raise ExaConnectionFailedError(
                        self, "Could not connect to Exasol: " + str(e)
                    ) from e
            else:
                self._ws.settimeout(self.options["socket_timeout"])

                self.ws_ipaddr = ipaddr or hostname
                self.ws_port = port

                self._ws_send = self._ws.send
                self._ws_recv = self._ws.recv

                if fingerprint:
                    self._validate_fingerprint(fingerprint)

                return

    def _create_websocket_connection(
        self, hostname: str, ipaddr: str, port: int, fingerprint: str | None
    ) -> websocket.WebSocket:
        ws_options = self._get_ws_options(fingerprint=fingerprint)
        # Use correct hostname matching IP address for each connection attempt
        if self.options["encryption"] and self.options["resolve_hostnames"]:
            ws_options["sslopt"]["server_hostname"] = hostname

        connection_string = self._get_websocket_connection_string(
            hostname, ipaddr, port
        )
        self.logger.debug(f"Connection attempt {connection_string}")
        try:
            return websocket.create_connection(connection_string, **ws_options)
        except Exception as e:
            self.logger.debug(f"Failed to connect [{connection_string}]: {e}")
            raise e

    def _get_websocket_connection_string(
        self, hostname: str, ipaddr: str | None, port: int
    ) -> str:
        host = hostname
        if self.options["resolve_hostnames"]:
            if ipaddr is None:
                raise ValueError("IP address was not resolved")
            host = ipaddr
        if self.options["encryption"]:
            return f"wss://{host}:{port}"
        else:
            return f"ws://{host}:{port}"

    def _get_ws_options(self, fingerprint: str | None) -> dict:
        options = {
            "timeout": self.options["connection_timeout"],
            "skip_utf8_validation": True,
            "enable_multithread": True,
            # Extra lock is necessary to protect abort_query() calls
        }

        if self.options["encryption"]:
            # refer to the `Security <https://exasol.github.io/pyexasol/master/user_guide/configuration/security.html>`__ page.
            if self.options["websocket_sslopt"] is None:
                # If a fingerprint is provided, then we do not use the default
                # to require a certificate verification.
                if fingerprint is not None:
                    options["sslopt"] = {"cert_reqs": ssl.CERT_NONE}
                else:
                    # When not provided by the user, the default behavior is to require
                    # strict certificate verification.
                    warn(
                        cleandoc(
                            """
                            From PyExasol version ``1.0.0``, the default behavior of
                            ExaConnection for encrypted connections without a fingerprint
                            is to require strict certificate validation with
                            ``websocket_sslopt=None`` being mapped to
                            ``{"cert_reqs": ssl.CERT_REQUIRED}``. The prior default behavior
                            was to map such cases to ``{"cert_reqs": ssl.CERT_NONE}``. For
                            more information about encryption & best practices, please refer to
                            `Security <https://exasol.github.io/pyexasol/master/user_guide/configuration/security.html>`__ page.
                            """
                        ),
                        PyexasolWarning,
                    )
                    options["sslopt"] = {"cert_reqs": ssl.CERT_REQUIRED}
            else:
                options["sslopt"] = self.options["websocket_sslopt"].copy()

        if self.options["http_proxy"]:
            proxy_components = urllib.parse.urlparse(self.options["http_proxy"])

            if proxy_components.hostname is None:
                raise ValueError("Could not parse http_proxy")

            options["http_proxy_host"] = proxy_components.hostname
            options["http_proxy_port"] = proxy_components.port
            options["http_proxy_auth"] = (
                proxy_components.username,
                proxy_components.password,
            )

        return options

    def _get_login_attributes(self):
        attributes = {
            "currentSchema": str(self.options["schema"]),
            "autocommit": self.options["autocommit"],
            "queryTimeout": self.options["query_timeout"],
        }

        if self.options["snapshot_transactions"] is not None:
            attributes["snapshotTransactionsEnabled"] = self.options[
                "snapshot_transactions"
            ]

        return attributes

    def _process_dsn(self, dsn: str) -> list[Host]:
        """
        Parse DSN, expand ranges and resolve IP addresses for all hostnames.

        Note:
            Randomness is required to guarantee proper distribution of workload across all nodes

        Returns:
            List of (hostname, ip_address, port) tuples in random order
        """
        if dsn is None or len(dsn.strip()) == 0:
            raise ExaConnectionDsnError(self, "Connection string is empty")

        current_port = constant.DEFAULT_PORT
        current_fingerprint = None

        result = []

        dsn_re = re.compile(
            r"^(?P<hostname_prefix>.+?)"
            # Optional range (e.g. myxasol1..4.com)
            r"(?:(?P<range_start>\d+)\.\.(?P<range_end>\d+)(?P<hostname_suffix>.*?))?"
            # Optional fingerprint (e.g. myexasol1..4.com/135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181)
            r"(?:/(?P<fingerprint>[0-9A-Fa-f]+|nocertcheck))?"
            # Optional port (e.g. myexasol1..4.com:8564)
            r"(?::(?P<port>\d+)?)?$",
            re.IGNORECASE,
        )

        # Port is applied backwards, so we iterate the whole list backwards to avoid second loop
        for part in reversed(dsn.split(",")):
            if len(part) == 0:
                continue

            m = dsn_re.search(part)

            if not m:
                raise ExaConnectionDsnError(
                    self, f"Could not parse connection string part [{part}]"
                )

            # Optional port
            if m.group("port"):
                current_port = int(m.group("port"))

            # Optional fingerprint
            if m.group("fingerprint"):
                current_fingerprint = m.group("fingerprint").upper()

                if not self.options["encryption"]:
                    raise ExaConnectionDsnError(
                        self,
                        "Fingerprint was specified in connection string, but encryption is not enabled",
                    )

            # Hostname or IP range was specified, expand it
            if m.group("range_start"):
                if int(m.group("range_start")) > int(m.group("range_end")):
                    raise ExaConnectionDsnError(
                        self,
                        f"Connection string part [{part}] contains an invalid range, "
                        f"lower bound is higher than upper bound",
                    )

                zfill_width = len(m.group("range_start"))

                for i in range(
                    int(m.group("range_start")), int(m.group("range_end")) + 1
                ):
                    hostname = f"{m.group('hostname_prefix')}{str(i).zfill(zfill_width)}{m.group('hostname_suffix')}"
                    result.extend(
                        self._resolve_hostname(
                            hostname, current_port, current_fingerprint
                        )
                    )
            # Just a single hostname or single IP address
            else:
                hostname = m.group("hostname_prefix")
                if self.options["resolve_hostnames"]:
                    result.extend(
                        self._resolve_hostname(
                            hostname, current_port, current_fingerprint
                        )
                    )
                else:
                    result.append(
                        Host(hostname, None, current_port, current_fingerprint)
                    )

        random.shuffle(result)

        return result

    def _resolve_hostname(
        self, hostname: str, port: int, fingerprint: str | None
    ) -> list[Host]:
        """
        Resolve all IP addresses for hostname and add port.

        Warnings:
            - It also implicitly checks that all hostnames mentioned in DSN can be resolved
        """
        try:
            hostname, _, ipaddr_list = socket.gethostbyname_ex(hostname)
        except OSError as e:
            raise ExaConnectionDsnError(
                self,
                f"Could not resolve IP address of hostname [{hostname}] "
                f"derived from connection string",
            ) from e

        return [Host(hostname, ipaddr, port, fingerprint) for ipaddr in ipaddr_list]

    def _validate_fingerprint(self, provided_fingerprint):
        if provided_fingerprint.upper() != "NOCERTCHECK":
            server_fingerprint = (
                hashlib.sha256(self._ws.sock.getpeercert(True)).hexdigest().upper()
            )

            if provided_fingerprint != server_fingerprint:
                raise ExaConnectionFailedError(
                    self,
                    f"Provided fingerprint [{provided_fingerprint}] did not match "
                    f"server fingerprint [{server_fingerprint}]",
                )

    def _init_logger(self):
        self.logger = self.cls_logger(self, constant.DRIVER_NAME)
        self.logger.setLevel("DEBUG" if self.options["debug"] else "WARNING")
        self.logger.add_default_handler()

    def _init_format(self):
        self.format = self.cls_formatter(self)

    def _init_json(self):
        if self.options["json_lib"] == "rapidjson":
            import rapidjson

            self.json_encode = lambda x, indent=False: rapidjson.dumps(
                x,
                number_mode=rapidjson.NM_NATIVE,
                indent=2 if indent else None,
                ensure_ascii=False,
            )
            self.json_decode = lambda x: rapidjson.loads(
                x, number_mode=rapidjson.NM_NATIVE
            )

        elif self.options["json_lib"] == "ujson":
            import ujson

            self.json_encode = lambda x, indent=False: ujson.dumps(
                x, indent=2 if indent else 0, ensure_ascii=False
            )
            self.json_decode = lambda x: ujson.loads(x)

        elif self.options["json_lib"] == "orjson":
            import orjson

            self.json_encode = lambda x, indent=False: orjson.dumps(
                x,
                option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE if indent else 0,
            )
            self.json_decode = lambda x: orjson.loads(x)

        elif self.options["json_lib"] == "json":
            import json

            self.json_encode = lambda x, indent=False: json.dumps(
                x, indent=2 if indent else None, ensure_ascii=False
            )
            self.json_decode = lambda x: json.loads(x)

        else:
            raise ValueError(f"Unsupported json library [{self.options['json_lib']}]")

    def _init_ext(self):
        self.ext = self.cls_extension(self)

    def _init_meta(self):
        self.meta = self.cls_meta(self)

    def _get_stmt_output_dir(self):
        import pathlib
        import tempfile

        if self.options["udf_output_dir"]:
            base_output_dir = self.options["udf_output_dir"]
        else:
            base_output_dir = tempfile.gettempdir()

        # Create unique sub-directory for every statement of every session
        self._udf_output_count += 1

        stmt_output_dir = (
            pathlib.Path(base_output_dir)
            / f"{self.session_id()}_{self._udf_output_count}"
        )
        stmt_output_dir.mkdir(parents=True, exist_ok=True)

        return stmt_output_dir

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} session_id={self.session_id()}"
            f" dsn={self.options['dsn']} user={self.options['user']}>"
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        """
        Will close the connection.

        Note:
            close() is being called automatically in order to:

            #.  send OP_CLOSE frame to Exasol server rather than silently terminating the socket on client side
            #.  make sure connection is closed immediately even if garbage collection was disabled for any reasons
            #.  write debug logs
        """
        # Based on our investigations, two scenarios have emerged, one of which does not function correctly:
        #
        # 1. `__del__` is invoked during a regular garbage collector run while the process remains active.
        # 2. `__del__` is invoked during interpreter shutdown (throws an exception).
        #     * This leads to a minor residual on the database, which will automatically be cleared up within a couple of hours
        #
        # In situations where interpreter shutdown occurs while a connection remains unclosed
        # due to not being addressed during a regular garbage collector run or manual close,
        # the error scenario (2.) will be triggered.
        #
        # The issue arises because during interpreter shutdown, orderly deletion is not guaranteed
        # (see also [Python documentation](https://docs.python.org/3/reference/datamodel.html#object.__del__) and
        # [C-API documentation](https://docs.python.org/3/c-api/init.html#c.Py_FinalizeEx)).
        # Consequently, scenarios may occur where the underlying socket is already closed or cleaned up while
        # the connection still tries using it (see [#108](https://github.com/exasol/pyexasol/issues/108)).
        #
        # Despite this problem, having a `__del__` method that performs cleanup remains valuable,
        # particularly in the context of long-running processes, to prevent resource leaks.
        #
        # Possible alternative solutions to address this would be:
        # * Within the catch, report a clear error message that the user did not close all the connections which were opened.
        # * Remove the `__del__` function to "force" the user to always address open/close in a clean manner.
        #
        # Still, the feedback we received from users so far indicates that neither alternative is desirable for them.
        try:
            self.close()
        except Exception:
            pass
