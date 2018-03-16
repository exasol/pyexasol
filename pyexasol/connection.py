import websocket
import platform
import getpass
import time
import pathlib
import zlib

from . import callback as cb
from . import constant
from . import utils

from .exceptions import ExaQueryError, ExaQueryTimeoutError, ExaRequestError, ExaCommunicationError, ExaRuntimeError
from .statement import ExaStatement
from .logger import ExaLogger
from .formatter import ExaFormatter
from .ext import ExaExtension
from .version import __version__


class ExaConnection(object):
    def __init__(self
            , dsn=None
            , user=None
            , password=None
            , schema=''
            , autocommit=constant.DEFAULT_AUTOCOMMIT
            , socket_timeout=constant.DEFAULT_SOCKET_TIMEOUT
            , query_timeout=constant.DEFAULT_QUERY_TIMEOUT
            , compression=False
            , fetch_dict=False
            , fetch_mapper=None
            , fetch_size_bytes=constant.DEFAULT_FETCH_SIZE_BYTES
            , lower_ident=False
            , cls_statement=ExaStatement
            , cls_formatter=ExaFormatter
            , cls_logger=ExaLogger
            , cls_extension=ExaExtension
            , json_lib='json'
            , verbose_error=True
            , debug=False
            , debug_logdir=None
            , subc_id=None
            , subc_token=None):
        """
        Exasol connection object

        :param dsn: Connection string, same format as standard JDBC / ODBC drivers (e.g. 10.10.127.1..11:8564)
        :param user: Username
        :param password: Password
        :param schema: Open schema after connection (Default: '', no schema)
        :param autocommit: Autocommit mode after connection (Default: True)
        :param socket_timeout: Socket timeout in seconds passed directly to websocket (Default: 10)
        :param query_timeout: Maximum execution time of queries before automatic abort (Default: 0, no timeout)
        :param compression: Use zlib compression both for WebSocket and HTTP transport (Default: False)
        :param fetch_dict: Fetch result rows as dicts instead of tuples (Default: False)
        :param fetch_mapper: Use custom mapper function to convert Exasol values into Python objects during fetching (Default: None)
        :param fetch_size_bytes: Maximum size of data message for single fetch request in bytes (Default: 5Mb)
        :param lower_ident: Automatically lowercase all identifiers (table names, column names, etc.) returned from relevant functions (Default: False)
        :param cls_statement: Overloaded ExaStatement class
        :param cls_formatter: Overloaded ExaFormatter class
        :param cls_logger: Overloaded ExaLogger class
        :param cls_extension: Overloaded ExaExtension class
        :param json_lib: Supported values: rapidjson, ujson, json (Default: json)
        :param verbose_error: Display additional information when error occurs (Default: True)
        :param debug: Output debug information for client-server communication and connection attempts to STDERR
        :param debug_logdir: Store debug information into files in debug_logdir instead of outputting it to STDERR
        """

        self.dsn = dsn
        self.user = user
        self.password = password
        self.schema = schema
        self.autocommit = autocommit

        self.socket_timeout = socket_timeout
        self.query_timeout = query_timeout
        self.compression = compression

        self.fetch_dict = fetch_dict
        self.fetch_mapper = fetch_mapper
        self.fetch_size_bytes = fetch_size_bytes
        self.lower_ident = lower_ident

        self.cls_statement = cls_statement
        self.cls_formatter = cls_formatter
        self.cls_logger = cls_logger
        self.cls_extension = cls_extension

        self.json_lib = json_lib

        self.verbose_error = verbose_error
        self.debug = debug
        self.debug_logdir = debug_logdir

        self.subc_id = subc_id
        self.subc_token = subc_token

        self.meta = {}
        self.attr = {}
        self.last_stmt = None

        self.ws_host = None
        self.ws_port = None
        self.ws_req_count = 0
        self.ws_req_time = 0

        self._init_logger()
        self._init_format()
        self._init_ws()
        self._init_json()
        self._init_ext()

        self.is_closed = False
        self.session_id = None

        if self.subc_token:
            self._sub_connect()
        else:
            self._connect()
            self._get_attr()

    def execute(self, query, query_params=None):
        self.last_stmt = self._statement(query, query_params)
        self.last_stmt._execute()

        return self.last_stmt

    def commit(self):
        return self.execute('COMMIT')

    def rollback(self):
        return self.execute('ROLLBACK')

    def set_autocommit(self, autocommit):
        if not isinstance(autocommit, bool):
            raise ValueError("Autocommit value must be boolean")

        self._set_attr({
            'autocommit': autocommit
        })

        self.attr['autocommit'] = autocommit

    def open_schema(self, schema):
        self._set_attr({
            'currentSchema': self.format.safe_ident(schema)
        })

    def current_schema(self):
        return self.attr.get('currentSchema', '')

    def export_to_file(self, dst, query_or_table, query_params=None, export_params=None):
        return self.export_to_callback(cb.export_to_file, dst, query_or_table, query_params, None, export_params)

    def export_to_list(self, query_or_table, query_params=None):
        return self.export_to_callback(cb.export_to_list, None, query_or_table, query_params)

    def export_to_pandas(self, query_or_table, query_params=None, callback_params=None):
        return self.export_to_callback(cb.export_to_pandas, None, query_or_table, query_params, callback_params, {'with_column_names': True})

    def import_from_file(self, src, table, import_params=None):
        return self.import_from_callback(cb.import_from_file, src, table, None, import_params)

    def import_from_iterable(self, src, table):
        return self.import_from_callback(cb.import_from_iterable, src, table)

    def import_from_pandas(self, src, table, callback_params=None):
        return self.import_from_callback(cb.import_from_pandas, src, table, callback_params)

    def export_to_callback(self, callback, dst, query_or_table, query_params=None, callback_params=None, export_params=None):
        from .http_transport import ExaSQLExportThread, ExaHTTPProcess, HTTP_EXPORT

        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        if callback_params is None:
            callback_params = {}

        if export_params is None:
            export_params = {}

        if query_params is not None:
            query_or_table = self.format.format(**query_params)

        try:
            http_proc = ExaHTTPProcess(self.ws_host, self.ws_port, self.compression, HTTP_EXPORT)
            http_proc.start()

            sql_thread = ExaSQLExportThread(self, http_proc.get_proxy(), query_or_table, export_params)
            sql_thread.start()

            result = callback(http_proc.read_pipe, dst, **callback_params)
            http_proc.read_pipe.close()

            http_proc.join()
            sql_thread.join()

            return result
        except Exception as e:
            # Close HTTP Server if it is still running
            if 'http_proc' in locals() and http_proc.is_alive():
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

        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        try:
            http_proc = ExaHTTPProcess(self.ws_host, self.ws_port, self.compression, HTTP_IMPORT)
            http_proc.start()

            sql_thread = ExaSQLImportThread(self, http_proc.get_proxy(), table, import_params)
            sql_thread.start()

            result = callback(http_proc.write_pipe, src, **callback_params)
            http_proc.write_pipe.close()

            http_proc.join()
            sql_thread.join()

            return result
        except Exception as e:
            # Close HTTP Server if it is still running
            if 'http_proc' in locals() and http_proc.is_alive():
                http_proc.terminate()

            # Give higher priority to SQL thread exception
            if 'sql_thread' in locals() and sql_thread.exc:
                raise sql_thread.exc

            raise e

    def export_parallel(self, http_proxy_list, query_or_table, query_params=None, export_params=None):
        from .http_transport import ExaSQLExportThread

        if export_params is None:
            export_params = {}

        if query_params is not None:
            query_or_table = self.format.format(**query_params)

        # There is no need to run separate thread here, all work is performed in child processes
        # We simply reuse thread class to keep logic in one place
        sql_thread = ExaSQLExportThread(self, http_proxy_list, query_or_table, export_params)
        sql_thread.run_sql()

    def session_id(self):
        return self.session_id

    def last_statement(self):
        if self.last_stmt is None:
            raise ExaRuntimeError('Last statement not found')

        return self.last_stmt

    def enter_parallel(self, num_parallel):
        ret = self._req({
            'command': 'enterParallel',
            'numRequestedConnections': num_parallel
        })

        return ret['responseData']['token'], ret['responseData']['nodes']

    def subc_open_handle(self, handle_id):
        st = self._statement()
        st._subc_handle(handle_id)

        return st

    def close(self):
        if not self.is_closed:
            self._req({
                'command': 'disconnect',
            })

            self.is_closed = True

    def _connect(self):
        ret = self._req({
            'command': 'login',
            'protocolVersion': 1,
        })

        self.meta = self._req({
            'username': self.user,
            'password': utils.encrypt_password(ret['responseData']['publicKeyPem'], self.password),
            'driverName': constant.DRIVER_NAME,
            'clientName': constant.CLIENT_NAME,
            'clientVersion': __version__,
            'clientOs': f'{platform.system()} {platform.release()} {platform.version()}',
            'clientOsUsername': getpass.getuser(),
            'clientRuntime': f'Python {platform.python_version()}',
            'useCompression': self.compression,
            'attributes': {
                'currentSchema': str(self.schema),
                'autocommit': self.autocommit,
                'queryTimeout': self.query_timeout,
            }
        })['responseData']

        self.session_id = str(self.meta['sessionId'])

        if self.fetch_size_bytes is None:
            self.fetch_size_bytes = self.meta['maxDataMessageSize']

        self._init_ws_compression()

    def _sub_connect(self):
        ret = self._req({
            'command': 'subLogin',
            'protocolVersion': 1,
        })

        self.meta = self._req({
            'username': self.user,
            'password': utils.encrypt_password(ret['responseData']['publicKeyPem'], self.password),
            'token': self.subc_token,
        })['responseData']

        self.session_id = str(self.meta['sessionId'])

        if self.fetch_size_bytes is None:
            self.fetch_size_bytes = self.meta['maxDataMessageSize']

        self._init_ws_compression()

    def _get_attr(self):
        ret = self._req({
            'command': 'getAttributes',
        })

        self.attr = ret['attributes']

    def _set_attr(self, new_attr):
        self._req({
            'command': 'setAttributes',
            'attributes': new_attr,
        })

    def _statement(self, query='', query_params=None):
        if not isinstance(query, str):
            raise ValueError("Query must be instance of str")

        if self.is_closed:
            raise ExaRuntimeError(self, "Database connection was closed")

        if query_params is not None:
            query = self.format.format(query, **query_params)

        query = query.strip(' \n')

        return self.cls_statement(self, query)

    def _req(self, req):
        self.ws_req_count += 1

        # Build request
        send_data = self._json_encode(req)
        self._logger.log_json(f'WebSocket request #{self.ws_req_count}', req)

        start_ts = time.time()

        # Send request, receive response
        try:
            self._ws_send(send_data)
            recv_data = self._ws_recv()
        except websocket.WebSocketException as e:
            raise ExaCommunicationError(self, str(e))

        self.ws_req_time = time.time() - start_ts

        # Parse response
        ret = self._json_decode(recv_data)
        self._logger.log_json(f'WebSocket response #{self.ws_req_count}', ret)

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
                else:
                    cls_err = ExaQueryError

                raise cls_err(self, req['sqlText'], ret['exception']['sqlCode'], ret['exception']['text'])
            else:
                raise ExaRequestError(self, ret['exception']['sqlCode'], ret['exception']['text'])

    def _init_ws(self):
        """
        Init websocket connection
        Connection redundancy is supported
        Specific Exasol host is chosen for every connection attempt
        """
        if hasattr(self, '_ws'):
            pass

        host_port_list = utils.get_random_host_port_from_dsn(self.dsn)
        failed_attempts = 0

        for i in host_port_list:
            try:
                self._logger.log_message(f'Connection attempt [{i[0]}:{i[1]}]')

                self._ws = websocket.create_connection(f'ws://{i[0]}:{i[1]}'
                                                       , timeout=self.socket_timeout
                                                       , skip_utf8_validation=True)

                self.ws_host = i[0]
                self.ws_port = i[1]

                self._ws_send = self._ws.send
                self._ws_recv = self._ws.recv

                return
            except Exception as e:
                self._logger.log_message(f'Failed to connect [{i[0]}:{i[1]}]')

                failed_attempts += 1

                if failed_attempts == len(host_port_list):
                    raise ExaCommunicationError(self, 'Could not connect to Exasol: ' + str(e))

    def _init_ws_compression(self):
        """ All further communication is transparently compressed after successful login or sublogin command """
        if self.compression:
            self._ws_send = lambda x: self._ws.send_binary(zlib.compress(x.encode(), 1))
            self._ws_recv = lambda: zlib.decompress(self._ws.recv())

    def _init_logger(self):
        if hasattr(self, '_logger'):
            pass

        if self.debug is False:
            log_target = None
        elif self.debug_logdir is None:
            log_target = 'stderr'
        else:
            log_target = pathlib.Path(self.debug_logdir)

        self._logger = self.cls_logger(self, log_target)

    def _init_format(self):
        if hasattr(self, 'format'):
            pass

        self.format = self.cls_formatter(self)

    def _init_json(self):
        """
        Init json functions
        Please overload it if you're unhappy with provided options
        """

        # rapidjson is well maintained library with acceptable performance, default choice
        if self.json_lib == 'rapidjson':
            import rapidjson

            self._json_encode = lambda x: rapidjson.dumps(x, number_mode=rapidjson.NM_NATIVE)
            self._json_decode = lambda x: rapidjson.loads(x, number_mode=rapidjson.NM_NATIVE)

        # ujson provides best performance in our tests, but it is abandoned by maintainers
        elif self.json_lib == 'ujson':
            import ujson

            self._json_encode = ujson.dumps
            self._json_decode = ujson.loads

        # json is native Python library, very safe choice, but slow
        elif self.json_lib == 'json':
            import json

            self._json_encode = json.dumps
            self._json_decode = json.loads

        else:
            raise ValueError(f'Unsupported json library [{self.json_lib}]')

    def _init_ext(self):
        if hasattr(self, 'ext'):
            pass

        self.ext = self.cls_extension(self)
