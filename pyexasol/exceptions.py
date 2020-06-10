from . import constant


class ExaError(Exception):
    """
    Generic PyEXASOL error, holds basic information about connection
    """
    def __init__(self, connection, message):
        self.connection = connection
        self.message = message

        super().__init__(self, message)

    def get_params_for_print(self):
        return {
            'message': self.message,
            'dsn': self.connection.options['dsn'],
            'user': self.connection.options['user'],
            'schema': self.connection.current_schema(),
            'session_id': self.connection.session_id(),
        }

    def __str__(self):
        if not self.connection.options['verbose_error']:
            return self.message

        params = self.get_params_for_print()
        pad_length = max(len(x) for x in params)
        res = ''

        for k in params:
            res += f"    {k.ljust(pad_length)}  =>  {params[k]}\n"

        return '\n(\n' + res + ')\n'


class ExaRuntimeError(ExaError):
    pass


class ExaCommunicationError(ExaError):
    """
    WebSocket communication failure after connection was established
    """
    pass


class ExaRequestError(ExaError):
    """
    Generic error returned from Exasol server after making a request
    """
    def __init__(self, connection, code, message):
        self.code = code

        super().__init__(connection, message)

    def get_params_for_print(self):
        params = super().get_params_for_print()
        params['code'] = self.code

        return params


class ExaAuthError(ExaRequestError):
    """
    Connection was established successfully, but authorization failed
    """
    pass


class ExaQueryError(ExaRequestError):
    """
    Error returned from Exasol server specifically for SQL query request (EXECUTE)
    """
    def __init__(self, connection, query, code, message):
        self.query = query

        super().__init__(connection, code, message)

    def get_params_for_print(self):
        params = super().get_params_for_print()
        params['session_id'] = self.connection.session_id()

        if len(self.query) > constant.EXCEPTION_QUERY_TEXT_MAX_LENGTH:
            params['query'] = f'{self.query[:constant.EXCEPTION_QUERY_TEXT_MAX_LENGTH]}' \
                              f'\n------ TRUNCATED TOO LONG QUERY ------\n'
        else:
            params['query'] = self.query

        return params


class ExaQueryTimeoutError(ExaQueryError):
    """
    Specific error for SQL query reaching QUERY_TIMEOUT and being terminated by server
    """
    pass


class ExaQueryAbortError(ExaQueryError):
    """
    Specific error for SQL query being aborted with .abort_query() call or KILL STATEMENT
    """
    pass


class ExaConnectionError(ExaError):
    """
    Generic error for connection failures
    """
    pass


class ExaConnectionDsnError(ExaConnectionError):
    """
    Specific error for connection failure related to DSN (connection string) issues
    """
    pass


class ExaConnectionFailedError(ExaConnectionError):
    """
    Specific error related to establishing WebSocket communication
    """
    pass


class ExaConcurrencyError(ExaError):
    """
    Detected an attempt to run multiple queries in multiple threads at the same time
    """
    pass
