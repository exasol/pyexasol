import traceback

from . import constant


class ExaError(Exception):
    """
    Generic PyExasol error, holds basic information about connection
    """

    def __init__(self, connection, message):
        self.connection = connection
        self.message = message

        super().__init__(self, message)

    def get_params_for_print(self):
        return {
            "message": self.message,
            "dsn": self.connection.options["dsn"],
            "user": self.connection.options["user"],
            "schema": self.connection.current_schema(),
            "session_id": self.connection.session_id(),
        }

    def __str__(self):
        if not self.connection.options["verbose_error"]:
            return self.message

        params = self.get_params_for_print()
        pad_length = max(len(x) for x in params)
        res = ""

        for k in params:
            res += f"    {k.ljust(pad_length)}  =>  {params[k]}\n"

        return "\n(\n" + res + ")\n"


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
        params["code"] = self.code

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
        params["session_id"] = self.connection.session_id()

        if len(self.query) > constant.EXCEPTION_QUERY_TEXT_MAX_LENGTH:
            params["query"] = (
                f"{self.query[:constant.EXCEPTION_QUERY_TEXT_MAX_LENGTH]}"
                f"\n------ TRUNCATED TOO LONG QUERY ------\n"
            )
        else:
            params["query"] = self.query

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


class ExaCallbackError(ExaError):
    """
    Base class for ExaExportError and ExaImportError
    """

    message = "Error in executing callback"

    def __init__(
        self,
        connection,
        exceptions: tuple[Exception | KeyboardInterrupt | None, ...],
    ):
        # filter out unique Exceptions but preserve order
        self.exceptions = list(dict.fromkeys(x for x in exceptions if x is not None))

        num_exceptions = len(self.exceptions)
        ending = ""
        if num_exceptions > 1:
            ending = "s"

        suffix = f" ({num_exceptions} sub-exception{ending})"
        self.message += suffix
        super().__init__(connection=connection, message=self.message)

    def __str__(self):
        parts = [self.message]
        for i, ex in enumerate(self.exceptions, 1):
            sep = f"+---------------- {i} ----------------"
            parts.append(sep)

            trace_lines = traceback.format_exception(type(ex), ex, ex.__traceback__)

            for line in "".join(trace_lines).strip().split("\n"):
                parts.append(f"| {line}")
        parts.append("+------------------------------------")

        return "\n" + "\n".join(parts)


class ExaExportError(ExaCallbackError):
    """
    Raised when a method relying on :meth:`pyexasol.connection.ExaConnection.export_to_callback`
    fails due to an exception raised in the execution of `export_to_callback`.
    This method relies on two threaded processes `http_thread` and `sql_thread`.
    As such, there may be one or more exceptions simultaneously raised, with a race
    condition as to which is raised and caught first. Additionally, it might be
    possible that re-running the same problematic code can lead to slightly
    different values in this catch-all exception, as potentially an exception was not
    yet experienced on that particularly thread when it was terminated. Furthermore,
    as there are a multitude of different exceptions possible, it is still up to the
    user to use this information, coupled with the traceback, to resolve the root
    cause(s) of the exception(s). It's not easily possible or reliable for PyExasol to
    determine the root cause(s).
    """

    message = "Error in executing `export_to_callback`"


class ExaImportError(ExaCallbackError):
    """
    Raised when a method relying on :meth:`pyexasol.connection.ExaConnection.import_from_callback`
    fails due to an exception raised in the execution of `import_from_callback`.
    This method relies on two threaded processes `http_thread` and `sql_thread`.
    As such, there may be one or more exceptions simultaneously raised, with a race
    condition as to which is raised and caught first. Additionally, it might be
    possible that re-running the same problematic code can lead to slightly
    different values in this catch-all exception, as potentially an exception was not
    yet experienced on that particularly thread when it was terminated. Furthermore,
    as there are a multitude of different exceptions possible, it is still up to the
    user to use this information, coupled with the traceback, to resolve the root
    cause(s) of the exception(s). It's not easily possible or reliable for PyExasol to
    determine the root cause(s).
    """

    message = "Error in executing `import_from_callback`"
