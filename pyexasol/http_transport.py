import struct
import zlib
import os

import threading
import multiprocessing

from socketserver import TCPServer
from http.server import BaseHTTPRequestHandler

from . import utils


class ExaSQLThread(threading.Thread):
    """
    Thread class which re-throws any Exception to parent thread
    """
    def __init__(self, connection, http_proc):
        self.connection = connection
        self.http_proc = http_proc
        self.exc = None

        super().__init__()

    def run(self):
        try:
            self.run_sql()
        except BaseException as e:
            self.exc = e
            self.http_proc.terminate()

    def run_sql(self):
        pass

    def join(self, *args):
        super().join(*args)

        if self.exc:
            raise self.exc


class ExaSQLExportThread(ExaSQLThread):
    """
    Build and run IMPORT query into separate thread
    Main thread is busy outputting data in callbacks
    """
    def __init__(self, connection, http_proc, query_or_table, export_params):
        self.query_or_table = query_or_table
        self.params = export_params

        super().__init__(connection, http_proc)

    def run_sql(self):
        if isinstance(self.query_or_table, tuple) or str(self.query_or_table).strip().find(' ') == -1:
            export_source = self.connection.format.safe_ident(self.query_or_table)
        else:
            # New lines are mandatory to handle queries with single-line comments '--'
            export_source = f'(\n{self.query_or_table}\n)'

        filename = utils.get_random_filename_for_http(self.connection.compression)

        query = f"""
            EXPORT {export_source}
            INTO CSV AT 'http://{self.http_proc.server.proxy_host}:{self.http_proc.server.proxy_port}' 
            FILE '{filename}'
        """

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
        filename = utils.get_random_filename_for_http(self.connection.compression)
        table_ident = self.connection.format.safe_ident(self.table)

        query = f"""
            IMPORT INTO {table_ident}
            FROM CSV AT 'http://{self.http_proc.server.proxy_host}:{self.http_proc.server.proxy_port}' 
            FILE '{filename}'
        """

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

        """
        try:
            self.connection.execute(query)
        except ExaError as e:
            self.http_proc.terminate()
            raise e
        """


class ExaHTTPProcess(multiprocessing.Process):
    """
    HTTP communication and compression / decompression is offloaded to separate process
    It communicates with main process using pipes
    """
    def __init__(self, connection, mode):
        self.connection = connection
        self.mode = mode

        self.server = ExaTCPServer((self.connection.ws_host, self.connection.ws_port), ExaHTTPRequestHandler)

        # Init common named pipes, not multiprocessing pipe magic
        read_fd, write_fd = os.pipe()

        self.read_pipe = os.fdopen(read_fd, 'rb')
        self.write_pipe = os.fdopen(write_fd, 'wb')

        if self.mode == 'import':
            self.server.set_pipe(self.read_pipe)
        else:
            self.server.set_pipe(self.write_pipe)

        self.server.set_compression(self.connection.compression)

        super().__init__()

    def run(self):
        if self.mode == 'import':
            self.write_pipe.close()
        else:
            self.read_pipe.close()

        self.server.handle_request()


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
