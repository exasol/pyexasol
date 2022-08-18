import base64
import getpass
import hashlib
import itertools
import platform
import random
import re
import rsa
import socket
import ssl
import time
import threading
import urllib.parse
import websocket
import zlib

from . import callback as cb

from .exceptions import *
from .statement import ExaStatement
from .logger import ExaLogger
from .formatter import ExaFormatter
from .ext import ExaExtension
from .http_transport import ExaSQLExportThread, ExaSQLImportThread, ExaHttpThread
from .meta import ExaMetaData
from .script_output import ExaScriptOutputProcess
from .version import __version__


class ExaConnection(object):
    cls_statement = ExaStatement
    cls_formatter = ExaFormatter
    cls_logger = ExaLogger
    cls_extension = ExaExtension
    cls_meta = ExaMetaData

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
            , snapshot_transactions=None
            , connection_timeout=constant.DEFAULT_CONNECTION_TIMEOUT
            , socket_timeout=constant.DEFAULT_SOCKET_TIMEOUT
            , query_timeout=constant.DEFAULT_QUERY_TIMEOUT
            , compression=False
            , encryption=True
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
            , client_version=None
            , client_os_username=None
            , protocol_version=constant.PROTOCOL_V3
            , websocket_sslopt=None
            , access_token=None
            , refresh_token=None
            ):
        """
        Exasol connection object

        :param dsn: Connection string, same format as standard JDBC / ODBC drivers (e.g. 10.10.127.1..11:8564)
        :param user: Username
        :param password: Password
        :param schema: Open schema after connection (Default: '', no schema)
        :param autocommit: Enable autocommit on connection (Default: True)
        :param snapshot_transactions: Explicitly enable or disable snapshot transactions on connection (Default: None, database default)
        :param connection_timeout: Socket timeout in seconds used to establish connection (Default: 10)
        :param socket_timeout: Socket timeout in seconds used for requests after connection was established (Default: 30)
        :param query_timeout: Maximum execution time of queries before automatic abort, in seconds (Default: 0, no timeout)
        :param compression: Use zlib compression both for WebSocket and HTTP transport (Default: False)
        :param encryption: Use SSL to encrypt client-server communications for WebSocket and HTTP transport (Default: True)
        :param fetch_dict: Fetch result rows as dicts instead of tuples (Default: False)
        :param fetch_mapper: Use custom mapper function to convert Exasol values into Python objects during fetching (Default: None)
        :param fetch_size_bytes: Maximum size of data message for single fetch request in bytes (Default: 5Mb)
        :param lower_ident: Automatically lowercase identifiers (table names, column names, etc.) returned from relevant functions (Default: False)
        :param quote_ident: Add double quotes and escape identifiers passed to relevant functions (export_*, import_*, ext.*, etc.) (Default: False)
        :param json_lib: Supported values: rapidjson, ujson, orjson, json (Default: json)
        :param verbose_error: Display additional information when error occurs (Default: True)
        :param debug: Output debug information for client-server communication and connection attempts to STDERR
        :param debug_logdir: Store debug information into files in debug_logdir instead of outputting it to STDERR
        :param udf_output_bind_address: Specific server_address to bind TCP server for UDF script output (default: ('', 0))
        :param udf_output_connect_address: Specific SCRIPT_OUTPUT_ADDRESS value to connect from Exasol to UDF script output server (default: inherited from TCP server)
        :param udf_output_dir: Directory to store captured UDF script output logs, split by <session_id>_<statement_id>/<vm_num>
        :param http_proxy: HTTP proxy string in Linux http_proxy format (default: None)
        :param client_name: Custom name of client application displayed in Exasol sessions tables (Default: PyEXASOL)
        :param client_version: Custom version of client application (Default: pyexasol.__version__)
        :param client_os_username: Custom OS username displayed in Exasol sessions table (Default: getpass.getuser())
        :param protocol_version: Major WebSocket protocol version requested for connection (Default: pyexasol.PROTOCOL_V3)
        :param websocket_sslopt: Set custom SSL options for WebSocket client (Default: None)
        :param access_token: OpenID access token to use for the login process
        :param refresh_token: OpenID refresh token to use for the login process
        """

        self.options = {
            'dsn': dsn,
            'user': user,
            'password': password,
            'schema': schema,
            'autocommit': autocommit,
            'snapshot_transactions': snapshot_transactions,

            'connection_timeout': connection_timeout,
            'socket_timeout': socket_timeout,
            'query_timeout': query_timeout,
            'compression': compression,
            'encryption': encryption,

            'fetch_dict': fetch_dict,
            'fetch_mapper': fetch_mapper,
            'fetch_size_bytes': fetch_size_bytes,
            'lower_ident': lower_ident,
            'quote_ident': quote_ident,

            'json_lib': json_lib,

            'verbose_error': verbose_error,
            'debug': debug,
            'debug_logdir': debug_logdir,

            'udf_output_bind_address': udf_output_bind_address,
            'udf_output_connect_address': udf_output_connect_address,
            'udf_output_dir': udf_output_dir,

            'http_proxy': http_proxy,

            'client_name': client_name,
            'client_version': client_version,
            'client_os_username': client_os_username,

            'protocol_version': protocol_version,
            'websocket_sslopt': websocket_sslopt,

            'access_token': access_token,
            'refresh_token': refresh_token,
        }

        self.login_info = {}
        self.login_time = 0
        self.attr = {}
        self.is_closed = False

        self.ws_ipaddr = None
        self.ws_port = None
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

    def execute(self, query, query_params=None) -> ExaStatement:
        """
        Execute SQL query with optional query formatting parameters
        Return ExaStatement object
        """
        return self.cls_statement(self, query, query_params)

    def execute_udf_output(self, query, query_params=None):
        """
        Execute SQL query with UDF script, capture output
        Return ExaStatement object and list of Path-objects for script output log files

        Exasol should be able to open connection to the machine where current script is running
        """
        stmt_output_dir = self._get_stmt_output_dir()

        script_output = ExaScriptOutputProcess(
            self.options['udf_output_bind_address'][0] if self.options['udf_output_bind_address'] else None,
            self.options['udf_output_bind_address'][1] if self.options['udf_output_bind_address'] else None,
            stmt_output_dir
        )

        try:
            script_output.start()

            # This option is useful to get around complex network setups, like Exasol running in Docker containers
            if self.options['udf_output_connect_address']:
                address = f"{self.options['udf_output_connect_address'][0]}:{self.options['udf_output_connect_address'][1]}"
            else:
                address = script_output.get_output_address()

            self.execute("ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = {address}", {'address': address})

            stmt = self.execute(query, query_params)
            log_files = sorted(list(stmt_output_dir.glob('*.log')))

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
            'currentSchema': self.format.default_format_ident_value(schema)
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
        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        if callback_params is None:
            callback_params = {}

        if export_params is None:
            export_params = {}

        if query_params is not None:
            query_or_table = self.format.format(query_or_table, **query_params)

        compression = False if ('format' in export_params) else self.options['compression']

        http_thread = ExaHttpThread(self.ws_ipaddr, self.ws_port, compression, self.options['encryption'])
        sql_thread = ExaSQLExportThread(self, compression, query_or_table, export_params)

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

    def import_from_callback(self, callback, src, table, callback_params=None, import_params=None):
        if callback_params is None:
            callback_params = {}

        if import_params is None:
            import_params = {}

        compression = False if ('format' in import_params) else self.options['compression']

        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        http_thread = ExaHttpThread(self.ws_ipaddr, self.ws_port, compression, self.options['encryption'])
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

    def export_parallel(self, exa_address_list, query_or_table, query_params=None, export_params=None):
        """
        Init HTTP transport in child processes first using pyexasol.http_transport()
        Get internal Exasol address from each child process using .address
        Pass address strings to parent process, combine into single list and use it for export_parallel() call
        """
        if export_params is None:
            export_params = {}

        compression = False if ('format' in export_params) else self.options['compression']

        if query_params is not None:
            query_or_table = self.format.format(query_or_table, **query_params)

        # There is no need to actually run a separate thread here, all work is performed in separate processes
        # We simply reuse thread class to keep logic in one place
        sql_thread = ExaSQLExportThread(self, compression, query_or_table, export_params)
        sql_thread.set_exa_address_list(exa_address_list)
        sql_thread.run_sql()

    def import_parallel(self, exa_address_list, table, import_params=None):
        """
        Init HTTP transport in child processes first using pyexasol.http_transport()
        Get internal Exasol address from each child process using .address
        Pass address strings to parent process, combine into single list and use it for import_parallel() call
        """
        if import_params is None:
            import_params = {}

        compression = False if ('format' in import_params) else self.options['compression']

        # There is no need to actually run a separate thread here, all work is performed in separate processes
        # We simply reuse thread class to keep logic in one place
        sql_thread = ExaSQLImportThread(self, compression, table, import_params)
        sql_thread.set_exa_address_list(exa_address_list)
        sql_thread.run_sql()

    def session_id(self):
        return str(self.login_info.get('sessionId', ''))

    def protocol_version(self):
        """
        Return WebSocket protocol version of opened connection
        Return 0 if connection was not established yet (e.g. due to exception handling)

        Actual Protocol version might be downgraded from requested protocol version if Exasol server does not support it
        """
        return int(self.login_info.get('protocolVersion', 0))

    def last_statement(self) -> ExaStatement:
        """
        Return last created ExaStatement object

        It is mainly used for HTTP transport to access internal IMPORT / EXPORT query,
        measure execution time and number of rows
        """
        if self.last_stmt is None:
            raise ExaRuntimeError(self, 'Last statement not found')

        return self.last_stmt

    def close(self, disconnect=True):
        """
        Close connection to Exasol by sending CLOSE websocket frame
        Send optional "disconnect" command to free resources and close session on Exasol server side properly

        Please note that "disconnect" should always be False when .close() is being called from .req()-like functions
        to prevent an infinite loop if websocket exception happens during handling of "disconnect" command
        """
        if self._ws.connected:
            if disconnect:
                self.req({
                    'command': 'disconnect'
                })

            self.logger.debug('[WebSocket connection close]')
            self._ws.close()

        self.is_closed = True
        self.last_stmt = None

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

        # At this moment setAttributes response is inconsistent, so attributes must be refreshed after every call
        self.get_attr()

    def get_nodes(self, pool_size=None):
        """
        Return list of dictionaries describing active Exasol nodes
        Format: {'ipaddr': <ip_address>, 'port': <port>, 'idx': <incremental index of returned node>}

        If pool_size is bigger than number of nodes, list will wrap around and nodes will repeat with different 'idx'
        If pool_size is omitted, return every active node once

        It is useful to balance workload for parallel IMPORT and EXPORT
        Exasol shuffles list for every connection
        """
        ret = self.req({
            'command': 'getHosts',
            'hostIp': self.ws_ipaddr,
        })

        if pool_size is None:
            pool_size = ret['responseData']['numNodes']

        # Key 'host' is deprecated and remains only for backwards compatibility, please use `ipaddr` instead
        return [{'host': ipaddr, 'ipaddr': ipaddr, 'port': self.ws_port, 'idx': idx} for idx, ipaddr
                in enumerate(itertools.islice(itertools.cycle(ret['responseData']['nodes']), pool_size), start=1)]

    def req(self, req):
        """ Send WebSocket request and wait for response """
        self.ws_req_count += 1
        local_req_count = self.ws_req_count

        # Build request
        send_data = self.json_encode(req)
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
        except (websocket.WebSocketException, ConnectionError) as e:
            self.close(disconnect=False)
            raise ExaCommunicationError(self, str(e))
        finally:
            self._req_lock.release()

        if not recv_data:
            raise ExaCommunicationError(self, "Empty WebSocket response, connection was likely closed")

        # Parse response
        ret = self.json_decode(recv_data)
        self.logger.debug_json(f'WebSocket response #{local_req_count}', ret)

        # Updated attributes may be returned from any request
        if 'attributes' in ret:
            self.attr = {**self.attr, **ret['attributes']}

        if ret['status'] == 'ok':
            return ret

        if ret['status'] == 'error':
            # Special treatment for "execute" command to prevent very long tracebacks in most common cases
            if req.get('command') in ['execute', 'createPreparedStatement']:
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

        send_data = self.json_encode(req)
        self.logger.debug_json('WebSocket abort request', req)

        try:
            self._ws_send(send_data)
        except (websocket.WebSocketException, ConnectionError) as e:
            self.close(disconnect=False)
            raise ExaCommunicationError(self, str(e))

    def _login(self):
        start_ts = time.time()

        if self.options['access_token'] or self.options['refresh_token']:
            auth_params = self._login_token()
        else:
            auth_params = self._login_password()

        self.login_info = self.req({
            **auth_params,
            'driverName': f'{constant.DRIVER_NAME} {__version__}',
            'clientName': self.options['client_name'] if self.options['client_name'] else constant.DRIVER_NAME,
            'clientVersion': self.options['client_version'] if self.options['client_version'] else __version__,
            'clientOs': platform.platform(),
            'clientOsUsername': self.options['client_os_username'] if self.options['client_os_username'] else getpass.getuser(),
            'clientRuntime': f'Python {platform.python_version()}',
            'useCompression': self.options['compression'],
            'attributes': self._get_login_attributes()
        })['responseData']

        self.login_time = time.time() - start_ts

        if self.options['compression']:
            self._ws_send = lambda x: self._ws.send_binary(zlib.compress(x.encode() if isinstance(x, str) else x, 1))
            self._ws_recv = lambda: zlib.decompress(self._ws.recv())

    def _login_password(self):
        ret = self.req({
            'command': 'login',
            'protocolVersion': self.options['protocol_version'],
        })

        auth_params = {
            'username': self.options['user'],
            'password': self._encrypt_password(ret['responseData']['publicKeyPem']),
        }

        return auth_params

    def _login_token(self):
        self.req({
            'command': 'loginToken',
            'protocolVersion': self.options['protocol_version'],
        })

        auth_params = {}

        if self.options['refresh_token']:
            auth_params['refreshToken'] = self.options['refresh_token']

        if self.options['access_token']:
            auth_params['accessToken'] = self.options['access_token']

        return auth_params

    def _encrypt_password(self, public_key_pem):
        pk = rsa.PublicKey.load_pkcs1(public_key_pem.encode())
        return base64.b64encode(rsa.encrypt(self.options['password'].encode(), pk)).decode()

    def _init_ws(self):
        """
        Init websocket connection
        Connection redundancy is supported
        Specific Exasol node is randomly selected for every connection attempt
        """
        dsn_items = self._process_dsn(self.options['dsn'])
        failed_attempts = 0

        ws_prefix = 'wss://' if self.options['encryption'] else 'ws://'
        ws_options = self._get_ws_options()

        for hostname, ipaddr, port, fingerprint in dsn_items:
            self.logger.debug(f"Connection attempt [{ipaddr}:{port}]")

            # Use correct hostname matching IP address for each connection attempt
            if self.options['encryption']:
                ws_options['sslopt']['server_hostname'] = hostname

            try:
                self._ws = websocket.create_connection(f'{ws_prefix}{ipaddr}:{port}', **ws_options)
            except Exception as e:
                self.logger.debug(f'Failed to connect [{ipaddr}:{port}]: {e}')

                failed_attempts += 1

                if failed_attempts == len(dsn_items):
                    raise ExaConnectionFailedError(self, 'Could not connect to Exasol: ' + str(e))
            else:
                self._ws.settimeout(self.options['socket_timeout'])

                self.ws_ipaddr = ipaddr
                self.ws_port = port

                self._ws_send = self._ws.send
                self._ws_recv = self._ws.recv

                if fingerprint:
                    self._validate_fingerprint(fingerprint)

                return

    def _get_ws_options(self):
        options = {
            'timeout': self.options['connection_timeout'],
            'skip_utf8_validation': True,
            'enable_multithread': True,     # Extra lock is necessary to protect abort_query() calls
        }

        if self.options['encryption']:
            # Use "websocket_sslopt" argument to set custom SSL options, which are passed directly to WebSocket client constructor
            if self.options['websocket_sslopt']:
                options['sslopt'] = self.options['websocket_sslopt']
            # OpenID tokens are normally used for Exasol SAAS
            # Strict certificate verification is enabled by default
            elif self.options['access_token'] or self.options['refresh_token']:
                options['sslopt'] = {'cert_reqs': ssl.CERT_REQUIRED}
            # Certification verification is disabled for other use cases (e.g. Docker container, "on-premises" Exasol setup)
            else:
                options['sslopt'] = {'cert_reqs': ssl.CERT_NONE}

        if self.options['http_proxy']:
            proxy_components = urllib.parse.urlparse(self.options['http_proxy'])

            if proxy_components.hostname is None:
                raise ValueError("Could not parse http_proxy")

            options['http_proxy_host'] = proxy_components.hostname
            options['http_proxy_port'] = proxy_components.port
            options['http_proxy_auth'] = (proxy_components.username, proxy_components.password)

        return options

    def _get_login_attributes(self):
        attributes = {
            'currentSchema': str(self.options['schema']),
            'autocommit': self.options['autocommit'],
            'queryTimeout': self.options['query_timeout'],
        }

        if self.options['snapshot_transactions'] is not None:
            attributes['snapshotTransactionsEnabled'] = self.options['snapshot_transactions']

        return attributes

    def _process_dsn(self, dsn):
        """
        Parse DSN, expand ranges and resolve IP addresses for all hostnames
        Return list of (hostname, ip_address, port) tuples in random order
        Randomness is required to guarantee proper distribution of workload across all nodes
        """
        if len(dsn.strip()) == 0:
            raise ExaConnectionDsnError(self, 'Connection string is empty')

        current_port = constant.DEFAULT_PORT
        current_fingerprint = None

        result = []

        dsn_re = re.compile(r'^(?P<hostname_prefix>.+?)'
                            # Optional range (e.g. myxasol1..4.com)
                            r'(?:(?P<range_start>\d+)\.\.(?P<range_end>\d+)(?P<hostname_suffix>.*?))?'
                            # Optional fingerprint (e.g. myexasol1..4.com/135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181)
                            r'(?:/(?P<fingerprint>[0-9A-Fa-f]+))?'
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

            # Optional port
            if m.group('port'):
                current_port = int(m.group('port'))

            # Optional fingerprint
            if m.group('fingerprint'):
                current_fingerprint = m.group('fingerprint').upper()

                if not self.options['encryption']:
                    raise ExaConnectionDsnError(self, 'Fingerprint was specified in connection string, but encryption is not enabled')

            # Hostname or IP range was specified, expand it
            if m.group('range_start'):
                if int(m.group('range_start')) > int(m.group('range_end')):
                    raise ExaConnectionDsnError(self,
                                                f'Connection string part [{part}] contains an invalid range, '
                                                f'lower bound is higher than upper bound')

                zfill_width = len(m.group('range_start'))

                for i in range(int(m.group('range_start')), int(m.group('range_end')) + 1):
                    hostname = f"{m.group('hostname_prefix')}{str(i).zfill(zfill_width)}{m.group('hostname_suffix')}"
                    result.extend(self._resolve_hostname(hostname, current_port, current_fingerprint))
            # Just a single hostname or single IP address
            else:
                result.extend(self._resolve_hostname(m.group('hostname_prefix'), current_port, current_fingerprint))

        random.shuffle(result)

        return result

    def _resolve_hostname(self, hostname, port, fingerprint):
        """
        Resolve all IP addresses for hostname and add port
        It also implicitly checks that all hostnames mentioned in DSN can be resolved
        """
        try:
            hostname, alias_list, ipaddr_list = socket.gethostbyname_ex(hostname)
        except OSError:
            raise ExaConnectionDsnError(self, f'Could not resolve IP address of hostname [{hostname}] '
                                              f'derived from connection string')

        return [(hostname, ipaddr, port, fingerprint) for ipaddr in ipaddr_list]

    def _validate_fingerprint(self, provided_fingerprint):
        server_fingerprint = hashlib.sha256(self._ws.sock.getpeercert(True)).hexdigest().upper()

        if provided_fingerprint != server_fingerprint:
            raise ExaConnectionFailedError(self, f"Provided fingerprint [{provided_fingerprint}] did not match "
                                                 f"server fingerprint [{server_fingerprint}]")

    def _init_logger(self):
        self.logger = self.cls_logger(self, constant.DRIVER_NAME)
        self.logger.setLevel('DEBUG' if self.options['debug'] else 'WARNING')
        self.logger.add_default_handler()

    def _init_format(self):
        self.format = self.cls_formatter(self)

    def _init_json(self):
        if self.options['json_lib'] == 'rapidjson':
            import rapidjson

            self.json_encode = lambda x, indent=False: rapidjson.dumps(x, number_mode=rapidjson.NM_NATIVE, indent=2 if indent else None, ensure_ascii=False)
            self.json_decode = lambda x: rapidjson.loads(x, number_mode=rapidjson.NM_NATIVE)

        elif self.options['json_lib'] == 'ujson':
            import ujson

            self.json_encode = lambda x, indent=False: ujson.dumps(x, indent=2 if indent else 0, ensure_ascii=False)
            self.json_decode = lambda x: ujson.loads(x)

        elif self.options['json_lib'] == 'orjson':
            import orjson

            self.json_encode = lambda x, indent=False: orjson.dumps(x, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE if indent else 0)
            self.json_decode = lambda x: orjson.loads(x)

        elif self.options['json_lib'] == 'json':
            import json

            self.json_encode = lambda x, indent=False: json.dumps(x, indent=2 if indent else None, ensure_ascii=False)
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

        if self.options['udf_output_dir']:
            base_output_dir = self.options['udf_output_dir']
        else:
            base_output_dir = tempfile.gettempdir()

        # Create unique sub-directory for every statement of every session
        self._udf_output_count += 1

        stmt_output_dir = pathlib.Path(base_output_dir) / f'{self.session_id()}_{self._udf_output_count}'
        stmt_output_dir.mkdir(parents=True, exist_ok=True)

        return stmt_output_dir

    def __repr__(self):
        return f"<{self.__class__.__name__} session_id={self.session_id()}" \
               f" dsn={self.options['dsn']} user={self.options['user']}>"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        """
        close() is being called automatically in order to:

        1) send OP_CLOSE frame to Exasol server rather than silently terminating the socket on client side
        2) make sure connection is closed immediately even if garbage collection was disabled for any reasons
        3) write debug logs
        """
        try:
            self.close()
        except Exception:
            pass
