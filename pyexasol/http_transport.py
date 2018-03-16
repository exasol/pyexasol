import struct
import zlib
import os

import threading
import multiprocessing

from socketserver import TCPServer
from http.server import BaseHTTPRequestHandler

from . import utils

HTTP_EXPORT = 'export'
HTTP_IMPORT = 'import'


class ExaSQLThread(threading.Thread):
    """
    Thread class which re-throws any Exception to parent thread
    """
    def __init__(self, connection, http_proxy):
        self.connection = connection
        self.http_proxy = http_proxy if isinstance(http_proxy, list) else [http_proxy]
        self.exc = None

        super().__init__()

    def run(self):
        try:
            self.run_sql()
        except BaseException as e:
            self.exc = e

    def run_sql(self):
        pass

    def join(self, *args):
        super().join(*args)

        if self.exc:
            raise self.exc

    def build_file_list(self):
        files = list()
        ext = '.csv.gz' if self.connection.compression else '.csv'

        for i, proxy in enumerate(self.http_proxy):
            files.append(f"FILE '{proxy}/{str(i).ljust(3, '0')}{ext}'")

        return '\n'.join(files)


class ExaSQLExportThread(ExaSQLThread):
    """
    Build and run IMPORT query into separate thread
    Main thread is busy outputting data in callbacks
    """
    def __init__(self, connection, http_proxy, query_or_table, export_params):
        self.query_or_table = query_or_table
        self.params = export_params

        super().__init__(connection, http_proxy)

    def run_sql(self):
        if isinstance(self.query_or_table, tuple) or str(self.query_or_table).strip().find(' ') == -1:
            export_source = self.connection.format.safe_ident(self.query_or_table)
        else:
            # New lines are mandatory to handle queries with single-line comments '--'
            export_source = f'(\n{self.query_or_table}\n)'

        query = f"EXPORT {export_source} INTO CSV AT 'http://'\n"
        query += self.build_file_list()

        if self.params.get('delimit'):
            delimit = str(self.params['delimit']).upper()

            if delimit != 'AUTO' and delimit != 'ALWAYS' and delimit != 'NONE':
                raise ValueError('Invalid value for export parameter DELIMIT: ' + delimit)

            query += '\n DELIMIT = ' + delimit

        if self.params.get('null'):
            query += '\nNULL = ' + self.connection.format.quote(self.params['null'])

        if self.params.get('row_separator'):
            query += '\nROW SEPARATOR = ' + self.connection.format.quote(self.params['row_separator'])

        if self.params.get('column_separator'):
            query += '\nCOLUMN SEPARATOR = ' + self.connection.format.quote(self.params['column_separator'])

        if self.params.get('column_delimiter'):
            query += '\nCOLUMN DELIMITER = ' + self.connection.format.quote(self.params['column_delimiter'])

        if self.params.get('with_column_names'):
            query += '\nWITH COLUMN NAMES'

        self.connection.execute(query)


class ExaSQLImportThread(ExaSQLThread):
    """
    Build and run EXPORT query into separate thread
    Main thread is busy parsing results in callbacks
    """
    def __init__(self, connection, http_proc, table, import_params):
        self.table = table
        self.params = import_params

        super().__init__(connection, http_proc)

    def run_sql(self):
        table_ident = self.connection.format.safe_ident(self.table)

        query = f"IMPORT INTO {table_ident} FROM CSV AT 'http://'\n"
        query += self.build_file_list()

        if self.params.get('null'):
            query += '\nNULL ' + self.connection.format.quote(self.params['null'])

        if self.params.get('skip'):
            query += '\nSKIP = ' + self.connection.format.safe_decimal(self.params['skip'])

        if self.params.get('trim'):
            trim = str(self.params['trim']).upper()

            if trim != 'TRIM' and trim != 'LTRIM' and trim != 'RTRIM':
                raise ValueError('Invalid value for import parameter TRIM: ' + trim)

            query += '\n' + trim

        if self.params.get('row_separator'):
            query += '\nROW SEPARATOR = ' + self.connection.format.quote(self.params['row_separator'])

        if self.params.get('column_separator'):
            query += '\nCOLUMN SEPARATOR = ' + self.connection.format.quote(self.params['column_separator'])

        if self.params.get('column_delimiter'):
            query += '\nCOLUMN DELIMITER = ' + self.connection.format.quote(self.params['column_delimiter'])

        self.connection.execute(query)


class ExaHTTPProcess(multiprocessing.Process):
    """
    HTTP communication and compression / decompression is offloaded to separate process
    It communicates with main process using pipes
    """
    def __init__(self, host, port, compression, mode):
        self.host = host
        self.port = port
        self.compression = compression
        self.mode = mode
        self.server = None

        # Init common named pipes for data transfer
        # Strictly binary mode, no wrappers and no buffering
        read_fd, write_fd = os.pipe()

        self.read_pipe = os.fdopen(read_fd, 'rb', 0)
        self.write_pipe = os.fdopen(write_fd, 'wb', 0)

        # Init multiprocessing private "magic" pipes for communication
        # Used only to send (proxy_host, proxy_port) to main process without sharing TCPServer handle
        self._proxy_read_pipe, self._proxy_write_pipe = multiprocessing.Pipe(False)

        super().__init__()

    def run(self):
        self.server = ExaTCPServer((self.host, self.port), ExaHTTPRequestHandler)
        self._proxy_read_pipe.close()

        self._proxy_write_pipe.send(f'{self.server.proxy_host}:{self.server.proxy_port}')
        self._proxy_write_pipe.close()

        if self.mode == HTTP_IMPORT:
            self.server.set_pipe(self.read_pipe)
            self.write_pipe.close()
        else:
            self.server.set_pipe(self.write_pipe)
            self.read_pipe.close()

        self.server.set_compression(self.compression)
        self.server.handle_request()

    def start(self):
        super().start()

        if self.mode == HTTP_IMPORT:
            self.read_pipe.close()
        else:
            self.write_pipe.close()

        self._proxy_write_pipe.close()

    def get_proxy(self):
        host_port = self._proxy_read_pipe.recv()
        self._proxy_read_pipe.close()

        return host_port


class ExaTCPServer(TCPServer):
    """
    This TCPServer is fake
    Instead of listening for incoming connections it connects to Exasol
    """
    def __init__(self, *args, **kwargs):
        self.proxy_host = None
        self.proxy_port = None
        self.pipe = None
        self.compression = False

        super().__init__(*args, **kwargs)

    def set_pipe(self, pipe):
        self.pipe = pipe

    def set_compression(self, compression):
        self.compression = compression

    def server_bind(self):
        """ Special Exasol packet to establish tunneling and return proxy host and port which can be used in query """
        self.socket.connect(self.server_address)
        self.socket.sendall(struct.pack("iii", 0x02212102, 1, 1))
        _, port, host = struct.unpack("ii16s", self.socket.recv(24))

        self.proxy_host = host.replace(b'\x00', b'').decode()
        self.proxy_port = port

    def server_activate(self): pass

    def get_request(self):
        return self.socket, self.server_address

    def shutdown_request(self, request): pass

    def close_request(self, request): pass


class ExaHTTPRequestHandler(BaseHTTPRequestHandler):
    """
    GzipFile cannot be used for this request handler
    Data stream is not monolith gzip, but chunked gzip with \r\n between chunks
    """
    def log_message(self, format, *args): pass

    def do_PUT(self):
        if self.server.compression:
            d = zlib.decompressobj(wbits=16+zlib.MAX_WBITS)

        while True:
            line = self.rfile.readline().strip()

            if len(line) == 0:
                chunk_len = 0
            else:
                chunk_len = int(line, 16)

            if chunk_len == 0:
                if self.server.compression:
                    self.server.pipe.write(d.flush())

                self.server.pipe.close()
                break

            data = self.rfile.read(chunk_len)

            if self.server.compression:
                data = d.decompress(data)

            self.server.pipe.write(data)

            if self.rfile.read(2) != b'\r\n':
                self.server.pipe.close()
                raise RuntimeError('Got wrong chunk delimiter in HTTP')

        self.send_response(200, 'OK')
        self.end_headers()

    def do_GET(self):
        if self.server.compression:
            c = zlib.compressobj(level=1, wbits=16+zlib.MAX_WBITS)

        self.protocol_version = 'HTTP/1.1'
        self.send_response(200, 'OK')
        self.send_header('Content-type', 'application/octet-stream')
        self.send_header('Connection', 'close')
        self.end_headers()

        while True:
            data = self.server.pipe.read(65535)

            if data is None or len(data) == 0:
                if self.server.compression:
                    self.wfile.write(c.flush(zlib.Z_FINISH))
                    self.wfile.flush()

                break

            if self.server.compression:
                data = c.compress(data)

            self.wfile.write(data)
            self.wfile.flush()


class ExaHTTPTransportWrapper(object):
    def __init__(self, dsn, mode, compression=False):
        host, port = utils.get_random_host_port_from_dsn(dsn)[0]

        self.http_proc = ExaHTTPProcess(host, port, compression, mode)
        self.http_proc.start()

        self.proxy = self.http_proc.get_proxy()

    def get_proxy(self):
        return self.proxy

    def export_to_callback(self, callback, dst, callback_params=None):
        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        if callback_params is None:
            callback_params = {}

        try:
            result = callback(self.http_proc.read_pipe, dst, **callback_params)

            self.http_proc.read_pipe.close()
            self.http_proc.join()

            return result
        except Exception as e:
            # Close HTTP Server if it is still running
            if self.http_proc.is_alive():
                self.http_proc.terminate()

            raise e
