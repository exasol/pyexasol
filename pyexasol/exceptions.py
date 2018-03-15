
class ExaError(Exception):
    def __init__(self, connection, message):
        self.connection = connection
        self.message = message

        super().__init__(self, message)

    def get_params_for_print(self):
        return {
            'message': self.message,
            'dsn': self.connection.dsn,
            'user': self.connection.user,
            'schema': self.connection.current_schema(),
        }

    def __str__(self):
        if not self.connection.verbose_error:
            return self.message

        params = self.get_params_for_print()
        pad_length = max(len(x) for x in params)
        res = ''

        for k in params:
            res += (' ' * 4) + k.ljust(pad_length) + '  =>  ' + params[k] + '\n'

        return '\n(\n' + res + ')\n'


class ExaRuntimeError(ExaError):
    pass


class ExaCommunicationError(ExaError):
    pass


class ExaRequestError(ExaError):
    def __init__(self, connection, code, message):
        self.code = code

        super().__init__(connection, message)

    def get_params_for_print(self):
        params = super().get_params_for_print()
        params['code'] = self.code

        return params


class ExaQueryError(ExaRequestError):
    def __init__(self, connection, query, code, message):
        self.query = query

        super().__init__(connection, code, message)

    def get_params_for_print(self):
        params = super().get_params_for_print()
        params['session_id'] = self.connection.session_id
        params['query'] = self.query

        return params


class ExaQueryTimeoutError(ExaQueryError):
    pass
