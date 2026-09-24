"""
This module contains compatibility tests for pythons dbapi module interface
"""

import datetime
import importlib
from unittest.mock import Mock

import pytest

from exasol.driver.websocket._cursor import (
    MetaData,
    _pyexasol2dbapi_metadata,
)
from exasol.driver.websocket._errors import translate_exception
from exasol.driver.websocket.dbapi2 import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    TypeCode,
)
from pyexasol.exceptions import (
    ExaCallbackError,
    ExaCommunicationError,
    ExaConcurrencyError,
    ExaConnectionError,
    ExaQueryError,
    ExaRequestError,
    ExaRuntimeError,
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


class TestTranslateException:
    def test_connection_error(self, source_connection):
        source = ExaConnectionError(source_connection, "server diagnostic")

        translated = translate_exception(source)

        assert isinstance(translated, OperationalError)
        assert str(translated) == str(source)

    def test_communication_error(self, source_connection):
        source = ExaCommunicationError(source_connection, "server diagnostic")

        translated = translate_exception(source)

        assert isinstance(translated, OperationalError)
        assert str(translated) == str(source)

    def test_runtime_error(self, source_connection):
        source = ExaRuntimeError(source_connection, "server diagnostic")

        translated = translate_exception(source)

        assert isinstance(translated, DatabaseError)
        assert str(translated) == str(source)

    def test_query_error(self, source_connection):
        source = ExaQueryError(source_connection, "SELECT 1", 1234, "server diagnostic")

        translated = translate_exception(source)

        assert isinstance(translated, ProgrammingError)
        assert str(translated) == str(source)

    def test_request_error(self, source_connection):
        source = ExaRequestError(source_connection, 1234, "server diagnostic")

        translated = translate_exception(source)

        assert isinstance(translated, DatabaseError)
        assert str(translated) == str(source)

    def test_concurrency_error(self, source_connection):
        source = ExaConcurrencyError(source_connection, "server diagnostic")

        translated = translate_exception(source)

        assert isinstance(translated, InterfaceError)
        assert str(translated) == str(source)

    def test_callback_error(self, source_connection):
        source = ExaCallbackError(source_connection, ())
        source.message = "server diagnostic"

        translated = translate_exception(source)

        assert isinstance(translated, DatabaseError)
        assert str(translated) == str(source)


def test_cursor_execute_passes_message_and_preserves_cause():
    from exasol.driver.websocket._cursor import Cursor

    source_connection = Mock()
    source_connection.options = {"verbose_error": False}
    source = ExaQueryError(
        source_connection,
        "select * from no_such_table",
        1234,
        "object NO_SUCH_TABLE not found",
    )
    underlying_connection = Mock()
    underlying_connection.execute.side_effect = source
    connection = Mock(connection=underlying_connection)
    cursor = Cursor(connection)

    with pytest.raises(ProgrammingError) as exception_info:
        cursor.execute("select * from no_such_table")

    assert str(exception_info.value) == "object NO_SUCH_TABLE not found"
    assert exception_info.value.args == ("object NO_SUCH_TABLE not found",)
    assert exception_info.value.__cause__ is source


def test_cursor_execute_propagates_non_exasol_exception():
    from exasol.driver.websocket._cursor import Cursor

    source = RuntimeError("unexpected adapter failure")
    underlying_connection = Mock()
    underlying_connection.execute.side_effect = source
    connection = Mock(connection=underlying_connection)
    cursor = Cursor(connection)

    with pytest.raises(RuntimeError) as exception_info:
        cursor.execute("select 1")

    assert exception_info.value is source


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
