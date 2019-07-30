import getpass
import itertools
import platform
import random
import re
import socket
import ssl
import time
import threading
import urllib.parse
import websocket
import zlib

from . import callback as cb
from . import constant
from . import utils

from .exceptions import *
from .statement import ExaStatement
from .logger import ExaLogger
from .formatter import ExaFormatter
from .ext import ExaExtension
from .script_output import ExaScriptOutputProcess
from .version import __version__


class ExaConnection(object):
    cls_statement = ExaStatement
    cls_formatter = ExaFormatter
    cls_logger = ExaLogger
    cls_extension = ExaExtension

    """
    Threads may share the module, but not connections
    One connection may be used by different threads, just not at the same time

    .abort_query() is an exception, it is meant to be called from another thread

    It is advisable to use multiprocessing instead of threading and create new connection in each sub-process
    """
    threadsafety = 1

    def __init__(self
            , dsn=None
            , user=None
            , password=None
            , schema=''
            , autocommit=constant.DEFAULT_AUTOCOMMIT
            , snapshot_transactions=False
            , socket_timeout=constant.DEFAULT_SOCKET_TIMEOUT
            , query_timeout=constant.DEFAULT_QUERY_TIMEOUT
            , compression=False
            , encryption=False
            , fetch_dict=False
            , fetch_mapper=None
            , fetch_size_bytes=constant.DEFAULT_FETCH_SIZE_BYTES
            , lower_ident=False
            , quote_ident=False
            , json_lib='json'
            , verbose_error=True
            , debug=False
            , debug_logdir=None
            , udf_output_bind_address=None
            , udf_output_connect_address=None
            , udf_output_dir=None
            , http_proxy=None
            , client_name=None
            , client_version=None):
        """
        Exasol connection object

        :param dsn: Connection string, same format as standard JDBC / ODBC drivers (e.g. 10.10.127.1..11:8564)
        :param user: Username
        :param password: Password
        :param schema: Open schema after connection (Default: '', no schema)
        :param autocommit: Enable autocommit on connection (Default: True)
        :param snapshot_transactions: Enable snapshot transactions on connection (Default: False)
        :param socket_timeout: Socket timeout in seconds passed directly to websocket (Default: 10)
        :param query_timeout: Maximum execution time of queries before automatic abort (Default: 0, no timeout)
        :param compression: Use zlib compression both for WebSocket and HTTP transport (Default: False)
        :param encryption: Use SSL to encrypt client-server communications for WebSocket and HTTP transport (Default: False)
        :param fetch_dict: Fetch result rows as dicts instead of tuples (Default: False)
        :param fetch_mapper: Use custom mapper function to convert Exasol values into Python objects during fetching (Default: None)
        :param fetch_size_bytes: Maximum size of data message for single fetch request in bytes (Default: 5Mb)
        :param lower_ident: Automatically lowercase identifiers (table names, column names, etc.) returned from relevant functions (Default: False)
        :param quote_ident: Add double quotes and escape identifiers passed to relevant functions (export_*, import_*, ext.*, etc.) (Default: False)
        :param json_lib: Supported values: rapidjson, ujson, json (Default: json)
        :param verbose_error: Display additional information when error occurs (Default: True)
        :param debug: Output debug information for client-server communication and connection attempts to STDERR
        :param debug_logdir: Store debug information into files in debug_logdir instead of outputting it to STDERR
        :param udf_output_bind_address: Specific server_address to bind TCP server for UDF script output (default: ('', 0))
        :param udf_output_connect_address: Specific SCRIPT_OUTPUT_ADDRESS value to connect from Exasol to UDF script output server (default: inherited from TCP server)
        :param udf_output_dir: Directory to store captured UDF script output logs, split by <session_id>_<statement_id>/<vm_num>
        :param http_proxy: HTTP proxy string in Linux http_proxy format (default: None)
        :param client_name: Custom name of client application displayed in Exasol sessions tables (Default: PyEXASOL)
        :param client_version: Custom version of client application (Default: pyexasol.__version__)
        """

        self.dsn = dsn
        self.user = user
        self.password = password
        self.schema = schema
        self.autocommit = autocommit
        self.snapshot_transactions = snapshot_transactions

        self.socket_timeout = socket_timeout
        self.query_timeout = query_timeout
        self.compression = compression
        self.encryption = encryption

        self.fetch_dict = fetch_dict
        self.fetch_mapper = fetch_mapper
        self.fetch_size_bytes = fetch_size_bytes
        self.lower_ident = lower_ident
        self.quote_ident = quote_ident

        self.json_lib = json_lib

        self.verbose_error = verbose_error
        self.debug = debug
        self.debug_logdir = debug_logdir

        self.udf_output_bind_address = udf_output_bind_address
        self.udf_output_connect_address = udf_output_connect_address
        self.udf_output_dir = udf_output_dir
        self.udf_output_count = 0

        self.http_proxy = http_proxy

        self.client_name = client_name
        self.client_version = client_version

        self.meta = {}
        self.attr = {}
        self.last_stmt = None
        self.stmt_count = 0
        self.is_closed = False

        self.ws_host = None
        self.ws_port = None
        self.ws_req_count = 0
        self.ws_req_time = 0

        self._req_lock = threading.Lock()

        self._init_logger()
        self._init_format()
        self._init_ws()
        self._init_json()
        self._init_ext()

        self._login()
        self.get_attr()

    def execute(self, query, query_params=None) -> ExaStatement:
        """
        Execute SQL query with optional query formatting parameters
        Return ExaStatement object
        """
        self.last_stmt = self.cls_statement(self, query, query_params)

        return self.last_stmt

    def execute_udf_output(self, query, query_params=None):
        """
        Execute SQL query with UDF script, capture output
        Return ExaStatement object and list of Path-objects for script output log files

        Exasol should be able to open connection to the host where current script is running
        """

        self.udf_output_count += 1
        output_dir = utils.get_output_dir_for_statement(self.udf_output_dir, self.session_id(), self.udf_output_count)

        script_output = ExaScriptOutputProcess(self.udf_output_bind_address[0], self.udf_output_bind_address[1], output_dir)
        script_output.start()

        # This option is useful to get around complex network setups, like Exasol running in Docker containers
        if self.udf_output_connect_address:
            address = f"{self.udf_output_connect_address[0]}:{self.udf_output_connect_address[1]}"
        else:
            address = script_output.get_output_address()

        self.execute("ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = {address}", {'address': address})

        try:
            stmt = self.execute(query, query_params)
            script_output.join()
        except ExaQueryError:
            script_output.terminate()
            raise

        return stmt, sorted(list(output_dir.glob('*.log')))

    def commit(self):
        return self.execute('COMMIT')

    def rollback(self):
        return self.execute('ROLLBACK')

    def set_autocommit(self, val):
        if not isinstance(val, bool):
            raise ValueError("Autocommit value must be boolean")

        self.set_attr({
            'autocommit': val
        })

    def set_query_timeout(self, val):
        self.set_attr({
            'queryTimeout': int(val)
        })

    def open_schema(self, schema):
        self.set_attr({
            'currentSchema': self.format.default_format_ident(schema)
        })

    def current_schema(self):
        return self.attr.get('currentSchema', '')

    def export_to_file(self, dst, query_or_table, query_params=None, export_params=None):
        return self.export_to_callback(cb.export_to_file, dst, query_or_table, query_params, None, export_params)

    def export_to_list(self, query_or_table, query_params=None, export_params=None):
        return self.export_to_callback(cb.export_to_list, None, query_or_table, query_params, None, export_params)

    def export_to_pandas(self, query_or_table, query_params=None, callback_params=None, export_params=None):
        if not export_params:
            export_params = {}

        export_params['with_column_names'] = True

        return self.export_to_callback(cb.export_to_pandas, None, query_or_table, query_params, callback_params, export_params)

    def import_from_file(self, src, table, import_params=None):
        return self.import_from_callback(cb.import_from_file, src, table, None, import_params)

    def import_from_iterable(self, src, table, import_params=None):
        return self.import_from_callback(cb.import_from_iterable, src, table, None, import_params)

    def import_from_pandas(self, src, table, callback_params=None, import_params=None):
        return self.import_from_callback(cb.import_from_pandas, src, table, callback_params, import_params)

    def export_to_callback(self, callback, dst, query_or_table, query_params=None, callback_params=None, export_params=None):
        from .http_transport import ExaSQLExportThread, ExaHTTPProcess, HTTP_EXPORT

        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        if callback_params is None:
            callback_params = {}

        if export_params is None:
            export_params = {}

        if 'format' in export_params:
            compression = False
        else:
            compression = self.compression

        if query_params is not None:
            query_or_table = self.format.format(query_or_table, **query_params)

        try:
            http_proc = ExaHTTPProcess(self.ws_host, self.ws_port, compression, self.encryption, HTTP_EXPORT)
            http_proc.start()

            sql_thread = ExaSQLExportThread(self, http_proc.get_proxy(), compression, query_or_table, export_params)
            sql_thread.set_http_proc(http_proc)
            sql_thread.start()

            result = callback(http_proc.read_pipe, dst, **callback_params)
            http_proc.read_pipe.close()

            http_proc.join()
            sql_thread.join()

            return result
        except Exception as e:
            # Close HTTP Server if it is still running
            if 'http_proc' in locals():
                http_proc.terminate()

            # Give higher priority to SQL thread exception
            if 'sql_thread' in locals() and sql_thread.exc:
                raise sql_thread.exc

            raise e

    def import_from_callback(self, callback, src, table, callback_params=None, import_params=None):
        from .http_transport import ExaSQLImportThread, ExaHTTPProcess, HTTP_IMPORT

        if callback_params is None:
            callback_params = {}

        if import_params is None:
            import_params = {}

        if 'format' in import_params:
            compression = False
        else:
            compression = self.compression

        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        try:
            http_proc = ExaHTTPProcess(self.ws_host, self.ws_port, compression, self.encryption, HTTP_IMPORT)
            http_proc.start()

            sql_thread = ExaSQLImportThread(self, http_proc.get_proxy(), compression, table, import_params)
            sql_thread.set_http_proc(http_proc)
            sql_thread.start()

            result = callback(http_proc.write_pipe, src, **callback_params)
            http_proc.write_pipe.close()

            http_proc.join()
            sql_thread.join()

            return result
        except Exception as e:
            # Close HTTP Server if it is still running
            if 'http_proc' in locals():
                http_proc.terminate()

            # Give higher priority to SQL thread exception
            if 'sql_thread' in locals() and sql_thread.exc:
                raise sql_thread.exc

            raise e

    def export_parallel(self, exa_proxy_list, query_or_table, query_params=None, export_params=None):
        """
        Init HTTP transport in child processes first using pyexasol.http_transport()
        Get proxy strings from each child process
        Pass proxy strings to parent process and use it for export_parallel() call
        """
        from .http_transport import ExaSQLExportThread

        if export_params is None:
            export_params = {}

        if 'format' in export_params:
            compression = False
        else:
            compression = self.compression

        if query_params is not None:
            query_or_table = self.format.format(query_or_table, **query_params)

        # There is no need to run separate thread here, all work is performed in child processes
        # We simply reuse thread class to keep logic in one place
        sql_thread = ExaSQLExportThread(self, exa_proxy_list, compression, query_or_table, export_params)
        sql_thread.run_sql()

    def import_parallel(self, exa_proxy_list, table, import_params=None):
        """
        Init HTTP transport in child processes first using pyexasol.http_transport()
        Get proxy strings from each child process
        Pass proxy strings to parent process and use it for import_parallel() call
        """
        from .http_transport import ExaSQLImportThread

        if import_params is None:
            import_params = {}

        if 'format' in import_params:
            compression = False
        else:
            compression = self.compression

        # There is no need to run separate thread here, all work is performed in child processes
        # We simply reuse thread class to keep logic in one place
        sql_thread = ExaSQLImportThread(self, exa_proxy_list, compression, table, import_params)
        sql_thread.run_sql()

    def session_id(self):
        return str(self.meta.get('sessionId', ''))

    def last_statement(self) -> ExaStatement:
        if self.last_stmt is None:
            raise ExaRuntimeError(self, 'Last statement not found')

        return self.last_stmt

    def close(self):
        if not self.is_closed:
            self.req({
                'command': 'disconnect',
            })

            self.is_closed = True

    def get_attr(self):
        ret = self.req({
            'command': 'getAttributes',
        })

        self.attr = ret['attributes']

    def set_attr(self, new_attr):
        self.req({
            'command': 'setAttributes',
            'attributes': new_attr,
        })

        # At this moment setAttributes response is inconsistent, so we have to fully refresh after every call
        self.get_attr()

    def get_nodes(self, pool_size=None):
        """
        Returns list of dictionaries describing active Exasol nodes
        Format: {'host': <ip_address>, 'port': <port>, 'idx': <incremental index of returned node>}

        If pool_size is bigger than number of nodes, list will wrap around and nodes will repeat with different 'idx'
        If pool_size is omitted, returns every active node once

        It is useful to balance workload for parallel IMPORT and EXPORT
        Exasol shuffles list for every connection
        """
        ret = self.req({
            'command': 'getHosts',
            'hostIp': self.ws_host,
        })

        if pool_size is None:
            pool_size = ret['responseData']['numNodes']

        return [{'host': ip_address, 'port': self.ws_port, 'idx': idx} for idx, ip_address
                in enumerate(itertools.islice(itertools.cycle(ret['responseData']['nodes']), pool_size), start=1)]

    def req(self, req):
        """ Send WebSocket request and wait for response """
        self.ws_req_count += 1
        local_req_count = self.ws_req_count

        # Build request
        send_data = self._json_encode(req)
        self.logger.debug_json(f'WebSocket request #{local_req_count}', req)

        # Prevent and discourage attempts to use connection object from another thread simultaneously
        if not self._req_lock.acquire(blocking=False):
            self.logger.debug(f'[WebSocket request #{local_req_count} WAS NOT SENT]')
            raise ExaConcurrencyError(self, 'Connection cannot be shared between multiple threads '
                                            'sending requests simultaneously')

        # Send request, wait for response
        try:
            start_ts = time.time()

            self._ws_send(send_data)
            recv_data = self._ws_recv()

            self.ws_req_time = time.time() - start_ts
        except websocket.WebSocketException as e:
            raise ExaCommunicationError(self, str(e))
        finally:
            self._req_lock.release()

        # Parse response
        ret = self._json_decode(recv_data)
        self.logger.debug_json(f'WebSocket response #{local_req_count}', ret)

        # Updated attributes may be returned from any request
        if 'attributes' in ret:
            self.attr = {**self.attr, **ret['attributes']}

        if ret['status'] == 'ok':
            return ret

        if ret['status'] == 'error':
            # Special treatment for "execute" command to prevent very long tracebacks in most common cases
            if req.get('command') == 'execute':
                if ret['exception']['sqlCode'] == 'R0001':
                    cls_err = ExaQueryTimeoutError
                elif ret['exception']['sqlCode'] == 'R0003':
                    cls_err = ExaQueryAbortError
                else:
                    cls_err = ExaQueryError

                raise cls_err(self, req['sqlText'], ret['exception']['sqlCode'], ret['exception']['text'])
            elif req.get('username') is not None:
                raise ExaAuthError(self, ret['exception']['sqlCode'], ret['exception']['text'])
            else:
                raise ExaRequestError(self, ret['exception']['sqlCode'], ret['exception']['text'])

    def abort_query(self):
        """
        Abort running query
        This function should be called from a separate thread and has no response
        Response should be checked in the main thread which started execution of query

        There are three possible outcomes of calling this function:
        1) Query is aborted normally, connection remains active
        2) Query was stuck in a state which cannot be aborted, so Exasol has to terminate connection
        3) Query might be finished successfully before abort call had a chance to take effect
        """
        req = {
            'command': 'abortQuery'
        }

        send_data = self._json_encode(req)
        self.logger.debug_json('WebSocket abort request', req)

        try:
            self._ws_send(send_data)
        except websocket.WebSocketException as e:
            raise ExaCommunicationError(self, str(e))

    def _login(self):
        ret = self.req({
            'command': 'login',
            'protocolVersion': 1,
        })

        self.meta = self.req({
            'username': self.user,
            'password': utils.encrypt_password(ret['responseData']['publicKeyPem'], self.password),
            'driverName': f'{constant.DRIVER_NAME} {__version__}',
            'clientName': self.client_name if self.client_name else constant.DRIVER_NAME,
            'clientVersion': self.client_version if self.client_version else __version__,
            'clientOs': platform.platform(),
            'clientOsUsername': getpass.getuser(),
            'clientRuntime': f'Python {platform.python_version()}',
            'useCompression': self.compression,
            'attributes': {
                'currentSchema': str(self.schema),
                'autocommit': self.autocommit,
                'queryTimeout': self.query_timeout,
                'snapshotTransactionsEnabled': self.snapshot_transactions,
            }
        })['responseData']

        if self.compression:
            self._ws_send = lambda x: self._ws.send_binary(zlib.compress(x.encode(), 1))
            self._ws_recv = lambda: zlib.decompress(self._ws.recv())

    def _init_ws(self):
        """
        Init websocket connection
        Connection redundancy is supported
        Specific Exasol host is randomly selected for every connection attempt
        """
        host_port_list = self._process_dsn(self.dsn)
        failed_attempts = 0

        ws_prefix = 'wss://' if self.encryption else 'ws://'
        ws_options = self._get_ws_options()

        for host, port in host_port_list:
            self.logger.debug(f"Connection attempt [{host}:{port}]")

            try:
                self._ws = websocket.create_connection(f'{ws_prefix}{host}:{port}', **ws_options)

                self.ws_host = host
                self.ws_port = port

                self._ws_send = self._ws.send
                self._ws_recv = self._ws.recv

                return
            except Exception as e:
                self.logger.debug(f'Failed to connect [{host}:{port}]')

                failed_attempts += 1

                if failed_attempts == len(host_port_list):
                    raise ExaConnectionFailedError(self, 'Could not connect to Exasol: ' + str(e))

    def _get_ws_options(self):
        options = {
            'timeout': self.socket_timeout,
            'skip_utf8_validation': True,
            'enable_multithread': True,     # Extra lock is necessary to protect abort_query() calls
        }

        if self.encryption:
            # Exasol does not check validity of certificates, so PyEXASOL follows this behaviour
            options['sslopt'] = {
                'cert_reqs': ssl.CERT_NONE
            }

        if self.http_proxy:
            proxy_components = urllib.parse.urlparse(self.http_proxy)

            if proxy_components.hostname is None:
                raise ValueError("Could not parse http_proxy")

            options['http_proxy_host'] = proxy_components.hostname
            options['http_proxy_port'] = proxy_components.port
            options['http_proxy_auth'] = (proxy_components.username, proxy_components.password)

        return options

    def _process_dsn(self, dsn):
        """
        Parse DSN, expand ranges and resolve IP addresses for all hosts
        Return list of host:port tuples in random order
        Randomness is useful to guarantee good distribution of workload across all nodes
        """
        if len(dsn.strip()) == 0:
            raise ExaConnectionDsnError(self, 'Connection string is empty')

        current_port = None
        result = []

        dsn_re = re.compile(r'^(?P<host_prefix>.+?)'
                            # Optional range (e.g. myxasol1..4.com)
                            r'(?:(?P<range_start>\d+)\.\.(?P<range_end>\d+)(?P<host_suffix>.*?))?'
                            # Optional port (e.g. myexasol1..4.com:8564)
                            r'(?::(?P<port>\d+)?)?$'
                            )

        # Port is applied backwards, so we iterate the whole list backwards to avoid second loop
        for part in reversed(dsn.split(',')):
            if len(part) == 0:
                continue

            m = dsn_re.search(part)

            if not m:
                raise ExaConnectionDsnError(self, f'Could not parse connection string part [{part}]')

            # Optional port was specified
            if m.group('port'):
                current_port = int(m.group('port'))

            # If current port is still empty, use default port
            if current_port is None:
                current_port = constant.DEFAULT_PORT

            # Hostname or IP range was specified, expand it
            if m.group('range_start'):
                if int(m.group('range_start')) > int(m.group('range_end')):
                    raise ExaConnectionDsnError(self,
                                                f'Connection string part [{part}] contains an invalid range, '
                                                f'lower bound is higher than upper bound')

                for i in range(int(m.group('range_start')), int(m.group('range_end')) + 1):
                    host = f"{m.group('host_prefix')}{i}{m.group('host_suffix')}"
                    result.extend(self._resolve_host(host, current_port))
            # Just a single hostname or single IP address
            else:
                result.extend(self._resolve_host(m.group('host_prefix'), current_port))

        random.shuffle(result)

        return result

    def _resolve_host(self, host, port):
        """
        Resolve all IP addresses for host and add port
        It also checks that all hosts mentioned in DSN can be resolved
        """
        try:
            hostname, aliaslist, ipaddrlist = socket.gethostbyname_ex(host)
        except OSError:
            raise ExaConnectionDsnError(self, f'Could not resolve IP address of host [{host}] '
                                              f'derived from connection string')

        return [(ipaddr, port) for ipaddr in ipaddrlist]

    def _init_logger(self):
        self.logger = self.cls_logger(self, constant.DRIVER_NAME)
        self.logger.setLevel('DEBUG' if self.debug else 'WARNING')
        self.logger.add_default_handler()

    def _init_format(self):
        self.format = self.cls_formatter(self)

    def _init_json(self):
        # rapidjson is well maintained library with acceptable performance, good choice
        if self.json_lib == 'rapidjson':
            import rapidjson

            self._json_encode = lambda x: rapidjson.dumps(x, number_mode=rapidjson.NM_NATIVE)
            self._json_decode = lambda x: rapidjson.loads(x, number_mode=rapidjson.NM_NATIVE)

        # ujson provides best performance in our tests, but it is abandoned by maintainers
        elif self.json_lib == 'ujson':
            import ujson

            self._json_encode = ujson.dumps
            self._json_decode = ujson.loads

        # json from Python stdlib, very safe choice, but slow
        elif self.json_lib == 'json':
            import json

            self._json_encode = json.dumps
            self._json_decode = json.loads

        else:
            raise ValueError(f'Unsupported json library [{self.json_lib}]')

    def _init_ext(self):
        self.ext = self.cls_extension(self)

    def __repr__(self):
        return f'<{self.__class__.__name__} session_id={self.session_id()} dsn={self.dsn} user={self.user}>'
