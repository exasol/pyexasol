"""
This module contains compatibility tests for pythons dbapi module interface
"""

import datetime
import importlib

import pytest

from exasol.driver.websocket._connection import _requires_connection
from exasol.driver.websocket._cursor import (
    MetaData,
    _pyexasol2dbapi_metadata,
)
from exasol.driver.websocket.dbapi2 import (
    Error,
    TypeCode,
)


@pytest.fixture
def dbapi():
    yield importlib.import_module("exasol.driver.websocket.dbapi2")


def test_defines_api_level(dbapi):
    assert dbapi.apilevel in {"1.0", "2.0"}


def test_defines_threadsafety(dbapi):
    assert dbapi.threadsafety in {0, 1, 2, 3}


def test_defines_paramstyle(dbapi):
    assert dbapi.paramstyle in {"qmark", "numeric", "named", "format", "pyformat"}


@pytest.mark.parametrize(
    "exception",
    [
        "Warning",
        "Error",
        "InterfaceError",
        "DatabaseError",
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
    ],
)
def test_all_exceptions_are_available(dbapi, exception):
    assert issubclass(getattr(dbapi, exception), Exception)


@pytest.mark.parametrize("year,month,day", [(2022, 12, 24), (2023, 1, 1)])
def test_date_constructor(dbapi, year, month, day):
    actual = dbapi.Date(year, month, day)
    expected = datetime.date(year, month, day)
    assert actual == expected


@pytest.mark.parametrize("hour,minute,second", [(12, 1, 24), (23, 1, 1)])
def test_time_constructor(dbapi, hour, minute, second):
    actual = dbapi.Time(hour, minute, second)
    expected = datetime.time(hour, minute, second)
    assert actual == expected


@pytest.mark.parametrize(
    "year,month,day,hour,minute,second",
    [(2022, 12, 24, 12, 1, 24), (2023, 1, 1, 23, 1, 1)],
)
def test_timestamp_constructor(dbapi, year, month, day, hour, minute, second):
    actual = dbapi.Timestamp(year, month, day, hour, minute, second)
    expected = datetime.datetime(year, month, day, hour, minute, second)
    assert actual == expected


def test_requires_connection_decorator_throws_exception_if_no_connection_is_available():
    class MyConnection:
        def __init__(self, con=None):
            self._connection = con

        @_requires_connection
        def close(self):
            pass

        def connect(self):
            self._connection = object()

    connection = MyConnection()
    with pytest.raises(Error) as e_info:
        connection.close()

    assert "No active connection available" == f"{e_info.value}"


def test_requires_connection_decorator_does_not_throw_exception_connection_is_available():
    class MyConnection:
        def __init__(self, con=None):
            self._connection = con

        @_requires_connection
        def close(self):
            return self._connection

        def connect(self):
            self._connection = object()

    connection = MyConnection(con=object())
    assert connection.close()


def test_requires_connection_decorator_does_use_wrap():
    class MyConnection:
        @_requires_connection
        def close(self):
            return True

    connection = MyConnection()
    assert "close" == connection.close.__name__


@pytest.mark.parametrize(
    "name,metadata,expected",
    (
        (
            (
                "A",
                {"type": "DECIMAL", "precision": 18, "scale": 0},
                MetaData(name="A", type_code=TypeCode.Decimal, precision=18, scale=0),
            ),
            (
                "B",
                {"type": "VARCHAR", "size": 100, "characterSet": "UTF8"},
                MetaData(name="B", type_code=TypeCode.String, internal_size=100),
            ),
            ("C", {"type": "BOOLEAN"}, MetaData(name="C", type_code=TypeCode.Bool)),
            ("D", {"type": "DOUBLE"}, MetaData(name="D", type_code=TypeCode.Double)),
        )
    ),
    ids=str,
)
def test_metadata_from_pyexasol_metadata(name, metadata, expected):
    actual = _pyexasol2dbapi_metadata(name, metadata)
    assert actual == expected
