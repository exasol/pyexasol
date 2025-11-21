"""
This module provides `PEP-249`_ DBAPI compliant cursor implementation.
(see also `PEP-249-cursor`_)

.. _PEP-249-cursor: https://peps.python.org/pep-0249/#cursor-objects
"""

import datetime
import decimal
from collections import defaultdict
from dataclasses import (
    astuple,
    dataclass,
)
from functools import wraps

import pyexasol.exceptions
from exasol.driver.websocket._errors import (
    Error,
    NotSupportedError,
)
from exasol.driver.websocket._types import TypeCode


@dataclass
class MetaData:
    """Metadata describing a result column"""

    name: str
    type_code: TypeCode
    display_size: int | None = None
    internal_size: int | None = None
    precision: int | None = None
    scale: int | None = None
    null_ok: bool | None = None


def _pyexasol2dbapi_metadata(name, metadata) -> MetaData:
    type_mapping = {t.value: t for t in TypeCode}
    key_mapping = {
        "name": "name",
        "type_code": "type",
        "precision": "precision",
        "scale": "scale",
        "display_size": "unknown",
        "internal_size": "size",
        "null_ok": "unknown",
    }
    metadata = defaultdict(lambda: None, metadata)
    metadata["type"] = type_mapping[metadata["type"]]
    metadata["name"] = name
    return MetaData(**{k: metadata[v] for k, v in key_mapping.items()})


def _is_not_closed(method):
    """
    Mark a function to require an open connection.

    Raises:
        An Error if the marked function is called without an open connection.
    """

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self._is_closed:
            raise Error(
                f"Unable to execute operation <{method.__name__}>, because cursor was already closed."
            )
        return method(self, *args, **kwargs)

    return wrapper


def _requires_result(method):
    """
    Decorator requires the object to have a result.

    Raises:
        Error if the cursor object has not produced a result yet.
    """

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self._cursor:
            raise Error("No result has been produced.")
        return method(self, *args, **kwargs)

    return wrapper


def _identity(value):
    return value


def _pyexasol2dbapi(value, metadata):
    members = (
        "name",
        "type_code",
        "display_size",
        "internal_size",
        "precision",
        "scale",
        "null_ok",
    )
    metadata = MetaData(**{k: v for k, v in zip(members, metadata)})

    def to_date(v):
        if not isinstance(v, str):
            return v
        return datetime.date.fromisoformat(v)

    def to_float(v):
        if not isinstance(v, str):
            return v
        return float(v)

    converters = defaultdict(
        lambda: _identity,
        {
            TypeCode.Date: to_date,
            TypeCode.Double: to_float,
        },
    )
    converter = converters[metadata.type_code]
    return converter(value)


def _dbapi2pyexasol(value):
    converters = defaultdict(
        lambda: _identity,
        {decimal.Decimal: str, float: str, datetime.date: str, datetime.datetime: str},
    )
    converter = converters[type(value)]
    return converter(value)


class Cursor:
    """
    Implementation of a cursor based on the DefaultConnection.

    For more details see :class: `Cursor` protocol definition.
    """

    # see https://peps.python.org/pep-0249/#arraysize
    DBAPI_DEFAULT_ARRAY_SIZE = 1

    def __init__(self, connection):
        self._connection = connection
        self._cursor = None
        self._is_closed = False

    # Note: Needed for compatibility with sqlalchemy_exasol base dialect.
    def __enter__(self):
        return self

    # Note: Needed for compatibility with sqlalchemy_exasol base dialect.
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    @_is_not_closed
    def arraysize(self):
        """See also :py:meth: `Cursor.arraysize`"""
        return self.DBAPI_DEFAULT_ARRAY_SIZE

    @property
    @_is_not_closed
    def description(self):
        """See also :py:meth: `Cursor.description`"""
        if not self._cursor:
            return None
        columns_metadata = (
            _pyexasol2dbapi_metadata(name, metadata)
            for name, metadata in self._cursor.columns().items()
        )
        columns_metadata = tuple(astuple(metadata) for metadata in columns_metadata)
        columns_metadata = columns_metadata if columns_metadata != () else None
        return columns_metadata

    @property
    @_is_not_closed
    def rowcount(self):
        """
        See also :py:meth: `Cursor.rowcount`

        Attention: This implementation of the rowcount attribute deviates slightly
                   from what the dbapi2 requires.

               Difference:

                    If the rowcount of the last operation cannot be determined it will
                    return 0.

                    Expected by DBAPI2 = -1
                    Actually returned  = 0

               Rational:

                    With the usage of pyexasol as underlying driver, there is no trivial
                    way to do this.
        """
        if not self._cursor:
            return -1
        return self._cursor.rowcount()

    @_is_not_closed
    def callproc(self, procname, parameters=None):
        """See also :py:meth: `Cursor.callproc`"""
        raise NotSupportedError("Optional and therefore not supported")

    @_is_not_closed
    def close(self):
        """See also :py:meth: `Cursor.close`"""
        self._is_closed = True
        if not self._cursor:
            return
        self._cursor.close()

    @_is_not_closed
    def execute(self, operation, parameters=None):
        """See also :py:meth: `Cursor.execute`"""
        if parameters:
            self.executemany(operation, [parameters])
            return

        connection = self._connection.connection
        try:
            self._cursor = connection.execute(operation)
        except pyexasol.exceptions.ExaError as ex:
            raise Error() from ex

    @staticmethod
    def _adapt_to_requested_db_types(parameters, db_response):
        """
        Adapt parameter types to match the types requested by the DB in the
        `createPreparedStatement <https://github.com/exasol/websocket-api/blob/master/docs/commands/createPreparedStatementV1.md>`_
        response.

        Args:

            parameters: which will be passed/send to the database.
            db_response: contains the DB response including the required types.


        Attention:

            This shim method currently only patches the following types:

                * VARCHAR
                * DOUBLE

            therefore it the future it may be necessary to improve or extend this.

            A hint that patching of a specific type is required, could be and
            error message similar to this one:

            .. code-block::

                pyexasol.exceptions.ExaRequestError:
                ...
                message  => getString: JSON value is not a string. (...)
                ...
        """

        def varchar(value):
            if value is None:
                return None
            return str(value)

        def double(value):
            if value is None:
                return None
            return float(value)

        converters = defaultdict(
            lambda: _identity, {"VARCHAR": varchar, "DOUBLE": double}
        )
        selected_converters = (
            converters[column["dataType"]["type"]] for column in db_response["columns"]
        )
        parameters = zip(selected_converters, parameters)
        parameters = [converter(value) for converter, value in parameters]
        return parameters

    @_is_not_closed
    def executemany(self, operation, seq_of_parameters):
        """See also :py:meth: `Cursor.executemany`"""
        parameters = [
            [_dbapi2pyexasol(p) for p in params] for params in seq_of_parameters
        ]
        connection = self._connection.connection
        self._cursor = connection.cls_statement(connection, operation, prepare=True)

        parameter_data = self._cursor.parameter_data
        parameters = [
            Cursor._adapt_to_requested_db_types(params, parameter_data)
            for params in parameters
        ]
        try:
            self._cursor.execute_prepared(parameters)
        except pyexasol.exceptions.ExaError as ex:
            raise Error() from ex

    def _convert(self, rows):
        if rows is None:
            return None

        return tuple(self._convert_row(row) for row in rows)

    def _convert_row(self, row):
        if row is None:
            return row

        return tuple(
            _pyexasol2dbapi(value, metadata)
            for value, metadata in zip(row, self.description)
        )

    @_requires_result
    @_is_not_closed
    def fetchone(self):
        """See also :py:meth: `Cursor.fetchone`"""
        row = self._cursor.fetchone()
        return self._convert_row(row)

    @_requires_result
    @_is_not_closed
    def fetchmany(self, size=None):
        """See also :py:meth: `Cursor.fetchmany`"""
        size = size if size is not None else self.arraysize
        rows = self._cursor.fetchmany(size)
        return self._convert(rows)

    @_requires_result
    @_is_not_closed
    def fetchall(self):
        """See also :py:meth: `Cursor.fetchall`"""
        rows = self._cursor.fetchall()
        return self._convert(rows)

    @_is_not_closed
    def nextset(self):
        """See also :py:meth: `Cursor.nextset`"""
        raise NotSupportedError("Optional and therefore not supported")

    @_is_not_closed
    def setinputsizes(self, sizes):
        """See also :py:meth: `Cursor.setinputsizes`

        Attention:
            This method does nothing.
        """

    @_is_not_closed
    def setoutputsize(self, size, column):
        """See also :py:meth: `Cursor.setoutputsize`

        Attention:
            This method does nothing.
        """

    def __del__(self):
        if self._is_closed:
            return
        self.close()
