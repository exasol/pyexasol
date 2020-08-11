import os
import re
import subprocess
import sys
import threading

HTTP_EXPORT = 'export'
HTTP_IMPORT = 'import'


class ExaSQLThread(threading.Thread):
    """
    Thread class which re-throws any Exception to parent thread
    """
    def __init__(self, connection, compression):
        self.connection = connection
        self.compression = compression

        self.params = {}
        self.http_proc = None
        self.exa_proxy_list = []
        self.exc = None

        super().__init__()

    def set_http_proc(self, http_proc):
        self.http_proc = http_proc

    def set_exa_proxy_list(self, exa_proxy_list):
        self.exa_proxy_list = exa_proxy_list if isinstance(exa_proxy_list, list) else [exa_proxy_list]

    def run(self):
        try:
            self.run_sql()
        except BaseException as e:
            self.exc = e

            # In case of SQL error terminate HTTP server
            # It closes other end of pipe and interrupts I/O in callback function in main thread
            if self.http_proc is not None:
                self.http_proc.terminate()

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

        for i, proxy in enumerate(self.exa_proxy_list):
            files.append(f"AT '{prefix}{proxy}' FILE '{str(i).rjust(3, '0')}.{ext}'{csv_cols}")

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
    Build and run IMPORT query into separate thread
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


class ExaHTTPProcess(object):
    """
    HTTP communication and compression / decompression is offloaded to sub-process
    It communicates with main process using pipes
    """
    def __init__(self, host, port, compression, encryption, mode):
        self.host = host
        self.port = port
        self.compression = compression
        self.encryption = encryption
        self.mode = mode

        self.server = None
        self.proxy = None
        self.proc = None

        self.read_pipe = None
        self.write_pipe = None

    def start(self):
        args = [sys.executable,
                '-m', 'pyexasol_utils.http_transport',
                '--host', self.host,
                '--port', str(self.port),
                '--mode', self.mode,
                '--ppid', str(os.getpid())
                ]

        if self.compression:
            args.append('--compression')

        if self.encryption:
            args.append('--encryption')

        self.proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=0)
        self.proxy = self.proc.stdout.readline().decode().rstrip('\n')

        self.read_pipe = self.proc.stdout
        self.write_pipe = self.proc.stdin

    def join(self):
        if self.proc:
            self.read_pipe.close()
            self.write_pipe.close()

            return self.proc.wait()

        return None

    def join_with_exc(self):
        code = self.join()

        if code != 0:
            raise RuntimeError(f"HTTP transport process finished with exitcode: {code}")

        return code

    def terminate(self):
        if self.proc:
            self.proc.terminate()

    def get_proxy(self):
        if self.proxy is None:
            raise RuntimeError("Proxy 'host:port' string is not available")

        return self.proxy


class ExaHTTPTransportWrapper(object):
    """
    Start fake HTTP server, obtain proxy "host:port" string
    Send it to parent process

    Block into "export_to_callback()" or "import_from_callback()" call,
    wait for incoming connection, process data, exit.
    """
    def __init__(self, host, port, mode, compression=False, encryption=False):
        self.http_proc = ExaHTTPProcess(host, port, compression, encryption, mode)
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
            self.http_proc.join_with_exc()

            return result
        except Exception as e:
            # Close HTTP Server if it is still running
            self.http_proc.terminate()
            self.http_proc.join()

            raise e

    def import_from_callback(self, callback, src, callback_params=None):
        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        if callback_params is None:
            callback_params = {}

        try:
            result = callback(self.http_proc.write_pipe, src, **callback_params)

            self.http_proc.write_pipe.close()
            self.http_proc.join_with_exc()

            return result
        except Exception as e:
            # Close HTTP Server if it is still running
            self.http_proc.terminate()
            self.http_proc.join()

            raise e

    def __repr__(self):
        return f'<{self.__class__.__name__} proxy={self.proxy}>'
