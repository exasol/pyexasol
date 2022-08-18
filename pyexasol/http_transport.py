import os
import re
import socket
import socketserver
import struct
import sys
import threading
import zlib


class ExaSQLThread(threading.Thread):
    """
    Thread class which re-throws any Exception to parent thread
    """
    def __init__(self, connection, compression):
        self.connection = connection
        self.compression = compression

        self.params = {}
        self.http_thread = None
        self.exa_address_list = []
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

    def build_file_list(self):
        files = list()

        if 'format' in self.params:
            if self.params['format'] not in ['gz', 'bz2', 'zip']:
                raise ValueError(f"Unsupported compression format: {self.params['format']}")

            ext = self.params['format']
        else:
            ext = 'gz' if self.compression else 'csv'

        if self.connection.options['encryption']:
            prefix = 'https://'
        else:
            prefix = 'http://'

        csv_cols = self.build_csv_cols()

        for i, exa_address in enumerate(self.exa_address_list):
            files.append(f"AT '{prefix}{exa_address}' FILE '{str(i).rjust(3, '0')}.{ext}'{csv_cols}")

        return files

    def build_columns_list(self):
        if 'columns' not in self.params:
            return ''

        return f"({','.join([self.connection.format.default_format_ident(c) for c in self.params['columns']])})"

    def build_csv_cols(self):
        if 'csv_cols' not in self.params:
            return ''

        safe_csv_cols_regexp = re.compile(r"^(\d+|\d+\.\.\d+)(\sFORMAT='[^'\n]+')?$", re.IGNORECASE)

        for c in self.params['csv_cols']:
            if not safe_csv_cols_regexp.match(c):
                raise ValueError(f"Value [{c}] is not a safe csv_cols part")

        return f"({','.join(self.params['csv_cols'])})"


class ExaSQLExportThread(ExaSQLThread):
    """
    Build and run EXPORT query into separate thread
    Main thread is busy outputting data in callbacks
    """
    def __init__(self, connection, compression, query_or_table, export_params):
        super().__init__(connection, compression)

        self.query_or_table = query_or_table
        self.params = export_params

    def run_sql(self):
        if isinstance(self.query_or_table, tuple) or str(self.query_or_table).strip().find(' ') == -1:
            export_source = self.connection.format.default_format_ident(self.query_or_table)
        else:
            # New lines are mandatory to handle queries with single-line comments '--'
            export_query = self.query_or_table.lstrip(" \n").rstrip(" \n;")
            export_source = f'(\n{export_query}\n)'

            if self.params.get('columns'):
                raise ValueError("Export option 'columns' is not compatible with SQL query export source")

        parts = list()

        if self.params.get('comment'):
            comment = self.params.get('comment')
            if '*/' in comment:
                raise ValueError('Invalid comment, cannot contain */')
            parts.append(f"/*{comment}*/")

        parts.append(f"EXPORT {export_source}{self.build_columns_list()} INTO CSV")
        parts.extend(self.build_file_list())

        if self.params.get('delimit'):
            delimit = str(self.params['delimit']).upper()

            if delimit != 'AUTO' and delimit != 'ALWAYS' and delimit != 'NEVER':
                raise ValueError('Invalid value for export parameter DELIMIT: ' + delimit)

            parts.append(f"DELIMIT = {delimit}")

        if self.params.get('encoding'):
            parts.append(f"ENCODING = {self.connection.format.quote(self.params['encoding'])}")

        if self.params.get('null'):
            parts.append(f"NULL = {self.connection.format.quote(self.params['null'])}")

        if self.params.get('row_separator'):
            parts.append(f"ROW SEPARATOR = {self.connection.format.quote(self.params['row_separator'])}")

        if self.params.get('column_separator'):
            parts.append(f"COLUMN SEPARATOR = {self.connection.format.quote(self.params['column_separator'])}")

        if self.params.get('column_delimiter'):
            parts.append(f"COLUMN DELIMITER = {self.connection.format.quote(self.params['column_delimiter'])}")

        if self.params.get('with_column_names'):
            parts.append("WITH COLUMN NAMES")

        self.connection.execute("\n".join(parts))


class ExaSQLImportThread(ExaSQLThread):
    """
    Build and run EXPORT query into separate thread
    Main thread is busy parsing results in callbacks
    """
    def __init__(self, connection, compression, table, import_params):
        super().__init__(connection, compression)

        self.table = table
        self.params = import_params

    def run_sql(self):
        table_ident = self.connection.format.default_format_ident(self.table)

        parts = list()

        if self.params.get('comment'):
            comment = self.params.get('comment')
            if '*/' in comment:
                raise ValueError('Invalid comment, cannot contain */')
            parts.append(f"/*{comment}*/")

        parts.append(f"IMPORT INTO {table_ident}{self.build_columns_list()} FROM CSV")
        parts.extend(self.build_file_list())

        if self.params.get('encoding'):
            parts.append(f"ENCODING = {self.connection.format.quote(self.params['encoding'])}")

        if self.params.get('null'):
            parts.append(f"NULL = {self.connection.format.quote(self.params['null'])}")

        if self.params.get('skip'):
            parts.append(f"SKIP = {self.connection.format.safe_decimal(self.params['skip'])}")

        if self.params.get('trim'):
            trim = str(self.params['trim']).upper()

            if trim != 'TRIM' and trim != 'LTRIM' and trim != 'RTRIM':
                raise ValueError('Invalid value for import parameter TRIM: ' + trim)

            parts.append(trim)

        if self.params.get('row_separator'):
            parts.append(f"ROW SEPARATOR = {self.connection.format.quote(self.params['row_separator'])}")

        if self.params.get('column_separator'):
            parts.append(f"COLUMN SEPARATOR = {self.connection.format.quote(self.params['column_separator'])}")

        if self.params.get('column_delimiter'):
            parts.append(f"COLUMN DELIMITER = {self.connection.format.quote(self.params['column_delimiter'])}")

        self.connection.execute("\n".join(parts))


class ExaHttpThread(threading.Thread):
    """
    HTTP communication and compression / decompression is offloaded to separate thread
    Thread can be used instead of subprocess when compatibility is an issue
    """
    def __init__(self, ipaddr, port, compression, encryption):
        self.server = ExaTCPServer((ipaddr, port), ExaHttpRequestHandler, compression=compression, encryption=encryption)

        self.read_pipe = self.server.read_pipe
        self.write_pipe = self.server.write_pipe

        self.exc = None

        super().__init__()

    @property
    def exa_address(self):
        return f'{self.server.exa_address_ipaddr}:{self.server.exa_address_port}'

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


class ExaHTTPTransportWrapper(object):
    """
    Start HTTP server, obtain address ("ipaddr:port" string)
    Send it to parent process

    Block into "export_*()" or "import_*()" call,
    wait for incoming connection, process data and exit.
    """
    def __init__(self, ipaddr, port, compression=False, encryption=True):
        self.http_thread = ExaHttpThread(ipaddr, port, compression, encryption)
        self.http_thread.start()

    @property
    def exa_address(self):
        return self.http_thread.exa_address

    def get_proxy(self):
        """ DEPRECATED, please use .exa_address property """
        return self.http_thread.exa_address

    def export_to_callback(self, callback, dst, callback_params=None):
        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        if callback_params is None:
            callback_params = {}

        try:
            result = callback(self.http_thread.read_pipe, dst, **callback_params)

            self.http_thread.read_pipe.close()
            self.http_thread.join_with_exc()

            return result

        except (Exception, KeyboardInterrupt) as e:
            self.http_thread.terminate_export()
            self.http_thread.join()

            raise e

    def import_from_callback(self, callback, src, callback_params=None):
        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        if callback_params is None:
            callback_params = {}

        try:
            result = callback(self.http_thread.write_pipe, src, **callback_params)

            self.http_thread.write_pipe.close()
            self.http_thread.join_with_exc()

            return result

        except (Exception, KeyboardInterrupt) as e:
            self.http_thread.terminate_import()
            self.http_thread.join()

            raise e

    def __repr__(self):
        return f'<{self.__class__.__name__} exa_address={self.exa_address}>'


class ExaTCPServer(socketserver.TCPServer):
    exa_address_ipaddr: str
    exa_address_port: int

    total_clients = 0
    is_terminated = False

    timeout = 1

    def __init__(self, *args, **kwargs):
        self.compression = kwargs.pop('compression', False)
        self.encryption = kwargs.pop('encryption', True)

        r_fd, w_fd = os.pipe()

        self.read_pipe = open(r_fd, 'rb', 0)
        self.write_pipe = open(w_fd, 'wb', 0)

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

        """ Special Exasol packet to establish tunneling and return internal exasol address, which can be used in query """
        self.socket.connect(self.server_address)
        self.socket.sendall(struct.pack("iii", 0x02212102, 1, 1))
        _, port, ipaddr = struct.unpack("ii16s", self.socket.recv(24))

        self.exa_address_ipaddr = ipaddr.replace(b'\x00', b'').decode()
        self.exa_address_port = port

        if self.encryption:
            context = self.generate_adhoc_ssl_context()
            self.socket = context.wrap_socket(self.socket, server_side=True, do_handshake_on_connect=False)

    def server_activate(self): pass

    def get_request(self):
        return self.socket, self.server_address

    def shutdown_request(self, request): pass

    def close_request(self, request): pass

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
            self.socket.ioctl(socket.SIO_KEEPALIVE_VALS, (1, keepidle * 1000, keepintvl * 1000))

    @staticmethod
    def generate_adhoc_ssl_context():
        """
        Create temporary self-signed certificate for encrypted HTTP transport
        Exasol does not check validity of certificates
        """
        from OpenSSL import crypto

        import pathlib
        import ssl
        import tempfile

        k = crypto.PKey()
        k.generate_key(crypto.TYPE_RSA, 2048)

        cert = crypto.X509()
        cert.set_serial_number(1)
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(60 * 60 * 24 * 365)

        cert.set_pubkey(k)
        cert.sign(k, 'sha256')

        # TemporaryDirectory is used instead of NamedTemporaryFile for compatibility with Windows
        with tempfile.TemporaryDirectory(prefix='pyexasol_ssl_') as tempdir:
            tempdir = pathlib.Path(tempdir)

            cert_file = open(tempdir / 'cert', 'wb')
            cert_file.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
            cert_file.close()

            key_file = open(tempdir / 'key', 'wb')
            key_file.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k))
            key_file.close()

            context = ssl.SSLContext()
            context.verify_mode = ssl.CERT_NONE
            context.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)

            return context


class ExaHttpRequestHandler(socketserver.StreamRequestHandler):
    success_headers = b'HTTP/1.1 200 OK\r\n' \
                      b'Connection: close\r\n' \
                      b'Transfer-Encoding: chunked\r\n' \
                      b'\r\n'

    error_headers = b'HTTP/1.1 500 Internal Server Error\r\n' \
                    b'Connection: close\r\n' \
                    b'\r\n'

    server: ExaTCPServer

    def handle(self):
        self.server.total_clients += 1

        # Extract method from the first header
        method = str(self.rfile.readline(), 'iso-8859-1').split()[0]

        # Skip all other headers
        while self.rfile.readline() != b'\r\n':
            pass

        if method == 'PUT':
            if self.server.compression:
                self.method_put_compressed()
            else:
                self.method_put_raw()
        elif method == 'GET':
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

        if self.rfile.read(2) != b'\r\n':
            raise RuntimeError('Invalid chunk delimiter in HTTP stream')

        return data

    def write_chunk(self, data):
        chunk_len = len(data)

        if chunk_len == 0:
            return

        self.wfile.write(b'%X\r\n%b\r\n' % (chunk_len, data))

    def write_final_chunk(self):
        self.wfile.write(b'0\r\n\r\n')

    def write_success_headers(self):
        self.wfile.write(self.success_headers)

    def write_error_headers(self):
        self.wfile.write(self.error_headers)
