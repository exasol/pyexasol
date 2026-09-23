"""
This module provides `PEP-249`_ compliant DBAPI exceptions.
(see also `PEP-249-exceptions`_)

.. _PEP-249-exceptions: https://peps.python.org/pep-0249/#exceptions
"""

from pyexasol.exceptions import (
    ExaCommunicationError,
    ExaConcurrencyError,
    ExaConnectionError,
    ExaError,
    ExaQueryError,
)


class Warning(Exception):  # Required by spec. pylint: disable=W0622
    """
    Exception raised for important warnings like data truncations while inserting, etc.
    """


class Error(Exception):
    """
    Base class of all other error exceptions.
    You can use this to catch all errors with one single except statement.
    Warnings are not considered errors and thus should not use this class as base.
    """


class InterfaceError(Error):
    """
    Exception raised for errors that are related to the database interface rather than
    the database itself.
    """


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""


class DataError(DatabaseError):
    """
    Exception raised for errors that are due to problems with the processed
    data like division by zero, numeric value out of range, etc.
    """


class OperationalError(DatabaseError):
    """
    Exception raised for errors that are related to the database’s operation
    and not necessarily under the control of the programmer, e.g. an unexpected
    disconnect occurs, the data source name is not found, a transaction
    could not be processed, a memory allocation error occurred during processing, etc.
    """


class IntegrityError(DatabaseError):
    """
    Exception raised when the relational integrity of the database is affected,
    e.g. a foreign key check fails.
    """


class InternalError(DatabaseError):
    """
    Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of sync, etc.
    """


class ProgrammingError(DatabaseError):
    """
    Exception raised for programming errors, e.g. table not found or already exists,
    syntax error in the SQL statement, wrong number of parameters specified, etc.
    """


class NotSupportedError(DatabaseError):
    """
    Exception raised in case a method or database API was used which is not supported
    by the database, e.g. requesting a .rollback() on a connection that does not
    support transaction or has transactions turned off.
    """


def translate_exception(exception: ExaError) -> Error:
    """Translate a PyExasol exception into a PEP-249 DBAPI exception.

    The complete string representation is copied so the translated exception
    remains useful even when the original cause is not inspected.
    """
    exception_message = str(exception)
    if isinstance(exception, ExaQueryError):
        return ProgrammingError(exception_message)
    elif isinstance(exception, (ExaConnectionError, ExaCommunicationError)):
        return OperationalError(exception_message)
    elif isinstance(exception, ExaConcurrencyError):
        return InterfaceError(exception_message)
    return DatabaseError(exception_message)
