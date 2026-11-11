"""
This module provides `PEP-249`_ DBAPI compliant cursor implementation.
(see also `PEP-249-cursor`_)

.. _PEP-249-cursor: https://peps.python.org/pep-0249/#cursor-objects
"""

from collections import defaultdict
from dataclasses import (
    astuple,
    dataclass,
)
from functools import wraps
from typing import Any

import pyexasol.exceptions
from exasol.driver.websocket._errors import (
    DataError,
    InterfaceError,
    NotSupportedError,
    ProgrammingError,
    translate_exception,
)
from pyexasol.data_types.websocket_types import WebSocketDataType
from pyexasol.data_types.websocket_validation import validate_value_for_websocket


@dataclass
class MetaData:
    """Metadata describing a result column"""

    name: str
    type_code: WebSocketDataType
    display_size: int | None = None
    internal_size: int | None = None
    precision: int | None = None
    scale: int | None = None
    null_ok: bool | None = None


def _pyexasol2dbapi_metadata(name, metadata) -> MetaData:
    type_mapping = {t.value: t for t in WebSocketDataType}
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
        InterfaceError if the marked function is called without an open connection.
    """

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self._is_closed:
            raise InterfaceError(
                f"Unable to execute operation <{method.__name__}>, because cursor was already closed."
            )
        return method(self, *args, **kwargs)

    return wrapper


def _requires_result(method):
    """
    Decorator requires the object to have a result.

    Raises:
        InterfaceError if the cursor object has not produced a result yet.
    """

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self._cursor:
            raise InterfaceError("No result has been produced.")
        return method(self, *args, **kwargs)

    return wrapper


def _check_parameter_count(
    row_index: int, parameter_row: list[Any], columns: list[dict[str, Any]]
) -> None:
    """Raise ProgrammingError unless the parameter_row has one value per column."""
    if len(parameter_row) != len(columns):
        raise ProgrammingError(
            f"Input shape mismatch for row {row_index + 1}: received "
            f"{len(parameter_row)} parameters, {len(columns)} columns"
        )


def _map_and_validate_row(insert_mapper, row, columns, row_index):
    """
    Map and validate each value of the row against its column, return the new row.

    Raises DataError if a value is invalid. The message gives the row number and
    the column location, both counted from a starting index of 1, the column type
    and the error text of the mapper or validator, which for a wrong Python type
    lists the allowed types.
    """
    mapped_row = []
    for column_index, (value, column) in enumerate(zip(row, columns)):
        data_type = column["dataType"]
        column_type = data_type["type"]
        try:
            mapped_value = insert_mapper(value, data_type)
            validate_value_for_websocket(mapped_value, column_type)
        except Exception as ex:
            raise DataError(
                f"Invalid value in seq_of_parameters row {row_index + 1}:\n"
                f"  * Column location: {column_index + 1}\n"
                f"  * Column type: {column_type}\n"
                f"  * {ex}"
            ) from ex
        mapped_row.append(mapped_value)
    return mapped_row


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

        try:
            self._connection.validate_session_datetime_formats(
                operation, for_insert=False
            )
            self._cursor = self._connection.connection.execute(operation)
        except pyexasol.exceptions.ExaError as ex:
            raise translate_exception(ex) from ex

    @_is_not_closed
    def executemany(self, operation, seq_of_parameters):
        """See also :py:meth: `Cursor.executemany`"""
        parameters = [list(params) for params in seq_of_parameters]
        insert_mapper = self._connection.insert_mapper
        try:
            self._connection.validate_session_datetime_formats(
                operation, for_insert=True
            )

            # Prepare the statement on the database to get the target column types.
            statement = self._connection.connection.cls_statement(
                self._connection.connection, operation, prepare=True
            )
            try:
                # Perform data checks and mapping on each row against the target
                # columns.
                mapped_rows = []
                column_data = statement.parameter_data["columns"]
                for row_index, parameter_row in enumerate(parameters):
                    _check_parameter_count(row_index, parameter_row, column_data)
                    mapped_row = _map_and_validate_row(
                        insert_mapper, parameter_row, column_data, row_index
                    )
                    mapped_rows.append(mapped_row)

                statement.execute_prepared(mapped_rows)
            except BaseException:
                # Don't leak the prepared statement on the server, and leave the
                # cursor's previous state untouched.
                statement.close()
                raise
            self._cursor = statement
        except pyexasol.exceptions.ExaError as ex:
            raise translate_exception(ex) from ex

    def _convert(self, rows):
        if rows is None:
            return None

        return tuple(self._convert_row(row) for row in rows)

    def _convert_row(self, row):
        if row is None:
            return row
        return tuple(row)

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
