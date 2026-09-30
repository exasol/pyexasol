"""
This module provides a `PEP-249`_ compliant DBAPI interface, for a websocket based
database driver (see also `exasol-websocket-api`_).

.. _PEP-249: https://peps.python.org/pep-0249/#interfaceerror
.. _exasol-websocket-api: https://github.com/exasol/websocket-api
"""

from exasol.driver.websocket._connection import Connection as DefaultConnection

# Re-export types and definitions required by dbapi2
from exasol.driver.websocket._errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from exasol.driver.websocket._protocols import (
    Connection,
    Cursor,
)
from exasol.driver.websocket._types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    TypeCode,
)

# Add remaining definitions

apilevel = "2.0"  # Required by spec. pylint: disable=C0103
threadsafety = 1  # Required by spec. pylint: disable=C0103
paramstyle = "qmark"  # Required by spec. pylint: disable=C0103


def connect(connection_class=DefaultConnection, **kwargs) -> Connection:
    """
    Creates a connection to the database.

    Args:
        connection_class: which shall be used to construct a connection object.
        kwargs: compatible with the provided connection_class.

    Returns:

        returns a dbapi2 complaint connection object.
    """
    connection = connection_class(**kwargs)
    return connection.connect()


__all__ = [
    # ----- Constants -----
    "apilevel",
    "threadsafety",
    "paramstyle",
    # ----- Errors -----
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "InternalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    # ----- Protocols -----
    "Connection",
    "Cursor",
    # ----- Types and Type-Conversions -----
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    # ----- Functions ------
    "connect",
    # ----- Non DBAPI exports -----
    "TypeCode",
]
