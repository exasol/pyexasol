from __future__ import annotations

import hashlib
import os
import re
import socket
import socketserver
import struct
import sys
import threading
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from ssl import SSLContext
from typing import TYPE_CHECKING

from packaging.version import Version

if TYPE_CHECKING:
    from pyexasol import ExaConnection


@dataclass
class SqlQuery:
    connection: ExaConnection
    compression: bool
    # set these values in param dictionary to ExaConnection
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Iterable[str] | None = None
    comment: str | None = None
    csv_cols: Iterable[str] | None = None
    encoding: str | None = None
    format: str | None = None
    null: str | None = None
    row_separator: str | None = None

    def _build_csv_cols(self) -> str:
        if self.csv_cols is not None:
            safe_csv_cols_regexp = re.compile(
                r"^(\d+|\d+\.\.\d+)(\sFORMAT='[^'\n]+')?$", re.IGNORECASE
            )
            for c in self.csv_cols:
                if not safe_csv_cols_regexp.match(c):
                    raise ValueError(f"Value [{c}] is not a safe csv_cols part")

            csv_cols = ",".join(self.csv_cols)
            if csv_cols != "":
                return f"({csv_cols})"

        return ""

    @staticmethod
    def _split_exa_address_into_components(exa_address: str) -> tuple[str, str | None]:
        """
        Split ip_address:port and public key from exa address, where the expected
        patterns are:
            ip_address:port
            ip_address:port/public_key
        The value for public key is expected to be a SHA-256 hash of the public key,
        which is then base64-encoded.
        """
        pattern = r"^([\d\.]+:\d+)(?:\/([a-zA-Z0-9_\-+\/]+=))?$"
        match = re.match(pattern, exa_address)
        if match is None:
            raise ValueError(
                f"Could not split exa_address {exa_address} into known components"
            )
        ip_address, public_key = match.groups()
        if not public_key:
            return ip_address, None
        return ip_address, public_key

    def _get_file_list(self, exa_address_list: list[str]) -> list[str]:
        file_ext = self._file_ext
        prefix = self._url_prefix

        csv_cols = self._build_csv_cols()
        files = []
        for i, exa_address in enumerate(exa_address_list):
            ip_address_port, public_key = self._split_exa_address_into_components(
                exa_address
            )
            statement = f"AT '{prefix}{ip_address_port}'"
            if self._requires_tls_public_key():
                if not public_key:
                    raise ValueError(
                        "Public key is required to be in the 'exa_address' for encrypted connections with Exasol DB >= 8.32.0"
                    )
                statement += f" PUBLIC KEY 'sha256//{public_key}'"
            statement += f" FILE '{str(i).rjust(3, '0')}.{file_ext}'{csv_cols}"
            files.append(statement)
        return files

    @staticmethod
    def _get_query_str(query_lines: list[str | None]) -> str:
        filtered_query_lines = [q for q in query_lines if q is not None]
        return "\n".join(filtered_query_lines)

    def _requires_tls_public_key(self) -> bool:
        version = self.connection.exasol_db_version
        return (
            version is not None
            and version >= Version("8.32.0")
            and self.connection.options["encryption"]
        )

    @property
    def _column_spec(self) -> str:
        """
        Return either empty string or comma-separated list of columns in parentheses,
        e.g. '("A", "B")'
        """
        if self.columns is not None:
            formatted = [
                self.connection.format.default_format_ident(c) for c in self.columns
            ]
            comma_sep = ",".join(formatted)
            if comma_sep != "":
                return f"({comma_sep})"
        return ""

    @property
    def _column_delimiter(self) -> str | None:
        if self.column_delimiter is None:
            return None
        return (
            f"COLUMN DELIMITER = {self.connection.format.quote(self.column_delimiter)}"
        )

    @property
    def _column_separator(self) -> str | None:
        if self.column_separator is None:
            return None
        return (
            f"COLUMN SEPARATOR = {self.connection.format.quote(self.column_separator)}"
        )

    @property
    def _comment(self) -> str | None:
        if self.comment is None:
            return None

        if "*/" in self.comment:
            raise ValueError(
                f'Invalid comment "{self.comment}". Comment must not contain "*/".'
            )
        return f"/*{self.comment}*/"

    @property
    def _encoding(self) -> str | None:
        if self.encoding is None:
            return None
        return f"ENCODING = {self.connection.format.quote(self.encoding)}"

    @property
    def _file_ext(self) -> str:
        if self.format is None:
            if self.compression:
                return "gz"
            return "csv"
        if self.format not in ("gz", "bz2", "zip"):
            raise ValueError(f"Unsupported compression format: {self.format}")
        return self.format

    @property
    def _null(self) -> str | None:
        if self.null is None:
            return None
        return f"NULL = {self.connection.format.quote(self.null)}"

    @property
    def _url_prefix(self) -> str:
        if self.connection.options["encryption"]:
            return "https://"
        return "http://"

    @property
    def _row_separator(self) -> str | None:
        if self.row_separator is None:
            return None
        return f"ROW SEPARATOR = {self.connection.format.quote(self.row_separator)}"


@dataclass
class ImportQuery(SqlQuery):
    # set these values in param dictionary to ExaConnection
    skip: str | int | None = None
    trim: str | None = None

    def build_query(self, table: str, exa_address_list: list[str]) -> str:
        query_lines = [
            self._comment,
            self._get_import(table=table),
            *self._get_file_list(exa_address_list=exa_address_list),
            self._encoding,
            self._null,
            self._skip,
            self._trim,
            self._row_separator,
            self._column_separator,
            self._column_delimiter,
        ]
        return self._get_query_str(query_lines)

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

    def _get_import(self, table: str) -> str:
        return f"IMPORT INTO {table}{self._column_spec} FROM CSV"

    @property
    def _skip(self) -> str | None:
        if self.skip is None:
            return None
        return f"SKIP = {self.connection.format.safe_decimal(self.skip)}"

    @property
    def _trim(self) -> str | None:
        if self.trim is None:
            return None

        trim = str(self.trim).upper()
        if trim not in ("TRIM", "LTRIM", "RTRIM"):
            raise ValueError(f"Invalid value for import parameter TRIM: {trim}")
        return trim


@dataclass
class ExportQuery(SqlQuery):
    # set these values in param dictionary to ExaConnection
    delimit: str | None = None
    with_column_names: bool = False

    def build_query(self, table: str, exa_address_list: list[str]) -> str:
        query_lines = [
            self._comment,
            self._get_export(table=table),
            *self._get_file_list(exa_address_list=exa_address_list),
            self._delimit,
            self._encoding,
            self._null,
            self._row_separator,
            self._column_separator,
            self._column_delimiter,
            self._with_column_names,
        ]
        return self._get_query_str(query_lines)

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

    def _get_export(self, table: str) -> str:
        return f"EXPORT {table}{self._column_spec} INTO CSV"

    @property
    def _delimit(self) -> str | None:
        if self.delimit is None:
            return None

        delimit = str(self.delimit).upper()
        if delimit not in ("AUTO", "ALWAYS", "NEVER"):
            raise ValueError(f"Invalid value for export parameter DELIMIT: {delimit}")
        return f"DELIMIT={delimit}"

    @property
    def _with_column_names(self) -> str | None:
        if not isinstance(self.with_column_names, bool):
            raise ValueError(
                "Invalid value for export parameter WITH_COLUMNS: "
                f"{self.with_column_names}. Only a boolean is allowed."
            )
        if self.with_column_names is False:
            return None
        return "WITH COLUMN NAMES"


class ExaSQLThread(threading.Thread):
    """
    Thread class which re-throws any Exception to parent thread
    """

    def __init__(self, connection: ExaConnection, compression: bool):
        self.connection = connection
        self.compression = compression

        self.params: dict = {}
        self.http_thread = None
        self.exa_address_list: list[str] = []
        self.exc = None

        super().__init__()

    def set_http_thread(self, http_thread):
        self.http_thread = http_thread
        self.exa_address_list = [http_thread.exa_address]

    def set_exa_address_list(self, exa_address_list):
        self.exa_address_list = exa_address_list

    def run(self):
        try:
            self.run_sql()
        except BaseException as e:
            self.exc = e

            # In case of SQL error stop HTTP server, close pipes and interrupt I/O in callback function
            if self.http_thread:
                self.http_thread.terminate()

    def run_sql(self):
        pass

    def join_with_exc(self, *args):
        super().join(*args)

        if self.exc:
            raise self.exc


class ExaSQLExportThread(ExaSQLThread):
    """
    Build and run EXPORT query into separate thread
    Main thread is busy outputting data in callbacks
    """

    def __init__(
        self,
        connection: ExaConnection,
        compression: bool,
        query_or_table,
        export_params: dict,
    ):
        super().__init__(connection, compression)

        self.query_or_table = query_or_table
        self.params = export_params

    def run_sql(self):
        if (
            isinstance(self.query_or_table, tuple)
            or str(self.query_or_table).strip().find(" ") == -1
        ):
            export_table = self.connection.format.default_format_ident(
                self.query_or_table
            )
        else:
            # New lines are mandatory to handle queries with single-line comments '--'
            export_query = self.query_or_table.lstrip(" \n").rstrip(" \n;")
            export_table = f"(\n{export_query}\n)"

            if self.params.get("columns"):
                raise ValueError(
                    "Export option 'columns' is not compatible with SQL query export source"
                )

        export_query = ExportQuery.load_from_dict(
            connection=self.connection, compression=self.compression, params=self.params
        ).build_query(table=export_table, exa_address_list=self.exa_address_list)
        self.connection.execute(export_query)


class ExaSQLImportThread(ExaSQLThread):
    """
    Build and run EXPORT query into separate thread
    Main thread is busy parsing results in callbacks
    """

    def __init__(
        self,
        connection: ExaConnection,
        compression: bool,
        table: str,
        import_params: dict,
    ):
        super().__init__(connection, compression)

        self.table = table
        self.params = import_params

    def run_sql(self):
        table = self.connection.format.default_format_ident(self.table)

        import_query = ImportQuery.load_from_dict(
            connection=self.connection, compression=self.compression, params=self.params
        ).build_query(table=table, exa_address_list=self.exa_address_list)
        self.connection.execute(import_query)


class ExaHttpThread(threading.Thread):
    """
    HTTP communication and compression / decompression is offloaded to a separate thread.
    PyExasol uses a thread instead of a subprocess or multiprocessing to avoid
    compatibility issues on Windows operating systems. For further details, see
    - https://github.com/exasol/pyexasol/issues/73
    - https://pythonforthelab.com/blog/differences-between-multiprocessing-windows-and-linux/
    """

    def __init__(self, ipaddr: str, port: int, compression: bool, encryption: bool):
        self.server = ExaTCPServer(
            (ipaddr, port),
            ExaHttpRequestHandler,
            compression=compression,
            encryption=encryption,
        )

        self.read_pipe = self.server.read_pipe
        self.write_pipe = self.server.write_pipe

        self.exc = None

        super().__init__()

    @property
    def exa_address(self) -> str:
        address = f"{self.server.exa_address_ipaddr}:{self.server.exa_address_port}"
        if public_key := self.server.exa_address_public_key:
            address = f"{address}/{public_key}"
        return address

    def run(self):
        try:
            # Handle exactly one HTTP request
            # Exit loop if thread was explicitly terminated prior to receiving HTTP request
            while self.server.total_clients == 0 and not self.server.is_terminated:
                self.server.handle_request()
        except BaseException as e:
            self.exc = e
        finally:
            self.server.server_close()

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


class ExaTCPServer(socketserver.TCPServer):
    exa_address_ipaddr: str
    exa_address_port: int
    exa_address_public_key: str | None = None

    total_clients: int = 0
    is_terminated: bool = False

    timeout: int | None = 1

    def __init__(self, *args, **kwargs):
        self.compression: bool = kwargs.pop("compression", False)
        self.encryption: bool = kwargs.pop("encryption", True)

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

    def server_bind(self):
        self.set_sock_opts()

        """ Special Exasol packet to establish tunneling and return an internal Exasol address, which can be used in a query """
        self.socket.connect(self.server_address)
        self.socket.sendall(struct.pack("iii", 0x02212102, 1, 1))
        _, port, ipaddr = struct.unpack("ii16s", self.socket.recv(24))

        self.exa_address_ipaddr = ipaddr.replace(b"\x00", b"").decode()
        self.exa_address_port = port

        if self.encryption:
            context, public_key_sha = self.generate_adhoc_ssl_context()
            self.socket = context.wrap_socket(
                self.socket, server_side=True, do_handshake_on_connect=False
            )
            self.exa_address_public_key = public_key_sha

    def server_activate(self):
        pass

    def get_request(self):
        return self.socket, self.server_address

    def shutdown_request(self, request):
        pass

    def close_request(self, request):
        pass

    def set_sock_opts(self):
        # only large packets are expected for HTTP transport
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # default keep-alive once a minute, 5 probes
        keepidle = 60
        keepintvl = 60
        keepcnt = 5

        if sys.platform.startswith("linux"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, keepidle)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, keepintvl)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, keepcnt)

        elif sys.platform.startswith("darwin"):
            # TCP_KEEPALIVE = 0x10
            # https://bugs.python.org/issue34932
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.socket.setsockopt(socket.IPPROTO_TCP, 0x10, keepidle)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, keepintvl)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, keepcnt)

        elif sys.platform.startswith("win"):
            self.socket.ioctl(
                socket.SIO_KEEPALIVE_VALS, (1, keepidle * 1000, keepintvl * 1000)
            )

    @staticmethod
    def generate_adhoc_ssl_context() -> tuple[SSLContext, str]:
        """
        Create temporary self-signed certificate for encrypted HTTP transport
        Exasol does not check validity of certificates
        """
        from base64 import b64encode
        from datetime import (
            datetime,
            timedelta,
            timezone,
        )
        from pathlib import Path
        from ssl import (
            CERT_NONE,
            PROTOCOL_TLS_SERVER,
        )
        from tempfile import TemporaryDirectory

        from cryptography import x509
        from cryptography.hazmat.primitives import (
            hashes,
            serialization,
        )
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        key_pair = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

        # For a self-signed certificate, subject and issuer are identical.
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "DE"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Franconia"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "Nuremberg"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Exasol AG"),
                x509.NameAttribute(NameOID.COMMON_NAME, "exasol.com"),
            ]
        )
        today = datetime.now(timezone.utc)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key_pair.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(today)
            .not_valid_after(today + timedelta(days=365))
            .sign(key_pair, hashes.SHA256())
        )

        der_data = key_pair.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        sha256_hash = hashlib.sha256(der_data).digest()
        base64_encoded = b64encode(sha256_hash)
        public_key_sha256 = base64_encoded.decode("utf-8")

        # TemporaryDirectory is used instead of NamedTemporaryFile for compatibility with Windows
        with TemporaryDirectory(prefix="pyexasol_ssl_") as tempdir:
            directory = Path(tempdir)

            cert_file = open(directory / "cert", "wb")
            cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
            cert_file.close()

            key_file = open(directory / "key", "wb")
            key_file.write(
                key_pair.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
            key_file.close()

            context = SSLContext(PROTOCOL_TLS_SERVER)
            context.verify_mode = CERT_NONE
            context.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)

            return context, public_key_sha256


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
