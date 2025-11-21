"""
This module provides `PEP-249`_ DBAPI compliant connection implementation.
(see also `PEP-249-connection`_)

.. _PEP-249-connection: https://peps.python.org/pep-0249/#connection-objects
"""

import ssl
from functools import wraps

import pyexasol
from exasol.driver.websocket._cursor import Cursor as DefaultCursor
from exasol.driver.websocket._errors import Error


def _requires_connection(method):
    """
    Decorator requires the object to have a working connection.

    Raises:
        Error if the connection object has no active connection.
    """

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self._connection:
            raise Error("No active connection available")
        return method(self, *args, **kwargs)

    return wrapper


class Connection:
    """
    Implementation of a websocket-based connection.

    For more details see :class: `Connection` protocol definition.
    """

    def __init__(
        self,
        dsn: str | None = None,
        username: str | None = None,
        password: str | None = None,
        schema: str = "",
        autocommit: bool = True,
        tls: bool = True,
        certificate_validation: bool = True,
        client_name: str = "EXASOL:DBAPI2:WS",
        client_version: str = "unknown",
    ):
        """
        Create a Connection object.

        Args:

            dsn: Connection string, same format as for standard JDBC / ODBC drivers.
            username: which will be used for the authentication.
            password: which will be used for the authentication.
            schema: to open after connecting.
            autocommit: enable autocommit.
            tls: enable tls.
            certificate_validation: disable certificate validation.
            client_name: which is communicated to the DB server.
        """

        # for more details see pyexasol.connection.ExaConnection
        self._options = {
            "dsn": dsn,
            "user": username,
            "password": password,
            "schema": schema,
            "autocommit": autocommit,
            "snapshot_transactions": None,
            "connection_timeout": 10,
            "socket_timeout": 30,
            "query_timeout": 0,
            "compression": False,
            "encryption": tls,
            "fetch_dict": False,
            "fetch_mapper": None,
            "fetch_size_bytes": 5 * 1024 * 1024,
            "lower_ident": False,
            "quote_ident": False,
            "json_lib": "json",
            "verbose_error": True,
            "debug": False,
            "debug_logdir": None,
            "udf_output_bind_address": None,
            "udf_output_connect_address": None,
            "udf_output_dir": None,
            "http_proxy": None,
            "client_name": client_name,
            "client_version": client_version,
            "protocol_version": 3,
            "websocket_sslopt": (
                {"cert_reqs": ssl.CERT_REQUIRED}
                if certificate_validation
                else {"cert_reqs": ssl.CERT_NONE}
            ),
            "access_token": None,
            "refresh_token": None,
        }
        self._connection = None

    def connect(self):
        """See also :py:meth: `Connection.connect`"""
        try:
            self._connection = pyexasol.connect(**self._options)
        except pyexasol.exceptions.ExaConnectionError as ex:
            raise Error(f"Connection failed, {ex}") from ex
        except Exception as ex:
            raise Error() from ex
        return self

    @property
    def connection(self):
        """Underlying connection used by this Connection"""
        return self._connection

    def close(self):
        """See also :py:meth: `Connection.close`"""
        connection_to_close = self._connection
        self._connection = None
        if connection_to_close is None or connection_to_close.is_closed:
            return
        try:
            connection_to_close.close()
        except Exception as ex:
            raise Error() from ex

    @_requires_connection
    def commit(self):
        """See also :py:meth: `Connection.commit`"""
        try:
            self._connection.commit()
        except Exception as ex:
            raise Error() from ex

    @_requires_connection
    def rollback(self):
        """See also :py:meth: `Connection.rollback`"""
        try:
            self._connection.rollback()
        except Exception as ex:
            raise Error() from ex

    @_requires_connection
    def cursor(self):
        """See also :py:meth: `Connection.cursor`"""
        return DefaultCursor(self)

    def __del__(self):
        if self._connection is None:
            return

        # Currently, the only way to handle this gracefully is to invoke the`__del__`
        # method of the underlying connection rather than calling an explicit `close`.
        #
        # For more details, see also:
        # * https://github.com/exasol/sqlalchemy-exasol/issues/390
        # * https://github.com/exasol/pyexasol/issues/108
        #
        # If the above tickets are resolved, it should be safe to switch back to using
        # `close` instead of `__del__`.
        self._connection.__del__()
