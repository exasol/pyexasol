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
            , json_lib='json'
            , verbose_error=True
            , debug=False
            , debug_logdir=None):

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

        self.json_lib = json_lib

        self.verbose_error = verbose_error
        self.debug = debug
        self.debug_logdir = debug_logdir

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

        self.is_closed = False
        self.session_id = None

        self._connect()
        self._get_attr()

    def execute(self, query, query_params=None):
        if not isinstance(query, str):
            raise ValueError("Query must be instance of str")

        if self.is_closed:
            raise ExaRuntimeError(self, "Cannot execute query, database connection was closed")

        if query_params is not None:
            query = self.format.format(query, **query_params)

        self.last_stmt = self.cls_statement(self, query)

        return self.last_stmt

    def commit(self):
        return self.execute('COMMIT')

    def rollback(self):
        return self.execute('ROLLBACK')

    def set_autocommit(self, autocommit):
        self._set_attr({
            'autocommit': autocommit
        })

    def open_schema(self, schema):
        self._set_attr({
            'currentSchema': self.format.safe_ident(schema)
        })

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
        from .http_transport import ExaSQLExportThread, ExaHTTPProcess

        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        if callback_params is None:
            callback_params = {}

        if export_params is None:
            export_params = {}

        if query_params is not None:
            query_or_table = self.format.format(**query_params)

        try:
            http_proc = ExaHTTPProcess(self, 'export')
            http_proc.start()

            http_proc.server.server_close()
            http_proc.write_pipe.close()

            sql_thread = ExaSQLExportThread(self, http_proc, query_or_table, export_params)
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
        from .http_transport import ExaSQLImportThread, ExaHTTPProcess

        if callback_params is None:
            callback_params = {}

        if import_params is None:
            import_params = {}

        if not callable(callback):
            raise ValueError('Callback argument is not callable')

        try:
            http_proc = ExaHTTPProcess(self, 'import')
            http_proc.start()

            http_proc.server.server_close()
            http_proc.read_pipe.close()

            sql_thread = ExaSQLImportThread(self, http_proc, table, import_params)
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

    def session_id(self):
        return self.session_id

    def last_statement(self):
        if self.last_stmt is None:
            raise ExaRuntimeError('Last statement not found')

        return self.last_stmt

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

        # All further communication is transparently compressed after login command
        if self.compression:
            self._ws_send = lambda x: self._ws.send_binary(zlib.compress(x.encode(), 1))
            self._ws_recv = lambda: zlib.decompress(self._ws.recv())

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

        self.attr = {**self.attr, **new_attr}

    def _req(self, req):
        start_ts = time.time()
        self.ws_req_count += 1

        # Build request
        send_data = self._json_encode(req)
        self._logger.log_json(f'WebSocket request #{self.ws_req_count}', req)

        # Send request, receive response
        try:
            self._ws_send(send_data)
            recv_data = self._ws_recv()
        except websocket.WebSocketException as e:
            raise ExaCommunicationError(self, str(e))

        # Parse response
        ret = self._json_decode(recv_data)
        self._logger.log_json(f'WebSocket response #{self.ws_req_count}', ret)

        self.ws_req_time = time.time() - start_ts

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

    def _init_logger(self):
        """
        Init debug logger
        """
        if hasattr(self, '_logger'):
            pass

        if self.debug is False:
            log_target = None
        elif self.debug_logdir is None:
            log_target = 'stderr'
        elif isinstance(self.debug_logdir, str):
            log_target = pathlib.Path(self.debug_logdir)

        self._logger = self.cls_logger(self, log_target)

    def _init_format(self):
        """
        Init SQL formatter
        """
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
