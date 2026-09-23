"""Unit tests for the WebSocket DBAPI connection wrapper."""

from unittest.mock import Mock

import pytest

from exasol.driver.websocket._connection import (
    Connection,
    _is_alter_session,
    _requires_connection,
)
from exasol.driver.websocket._errors import (
    InterfaceError,
    ProgrammingError,
)
from pyexasol.exceptions import ExaQueryError


class TestIsAlterSession:
    @pytest.mark.parametrize(
        "operation",
        [
            pytest.param(
                "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
                id="standard-spacing-and-casing",
            ),
            pytest.param(
                "Alter  Session SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
                id="mixed-case-and-multiple-spaces",
            ),
            pytest.param(
                "ALTER\tSESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
                id="tab-separated-keywords",
            ),
            pytest.param(
                "  ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'  ",
                id="leading-and-trailing-whitespace",
            ),
        ],
    )
    def test_matches(self, operation):
        assert _is_alter_session(operation)

    @pytest.mark.parametrize(
        "operation",
        [
            pytest.param("SELECT 1", id="other-statement"),
            pytest.param(
                "ALTER SESSION_FORMAT SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
                id="session-suffix",
            ),
            pytest.param(
                "SELECT 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD'''",
                id="text-containing-alter-session",
            ),
            pytest.param(None, id="non-string-operation"),
        ],
    )
    def test_rejects_other_text(self, operation):
        assert not _is_alter_session(operation)


class TestValidateSessionDatetimeFormats:
    def test_skips_alter_session_operations(self):
        connection = Connection()
        connection._connection = Mock()

        connection.validate_session_datetime_formats(
            "ALTER  SESSION SET NLS_TIMESTAMP_FORMAT = 'unsupported'"
        )

        connection._connection.execute.assert_not_called()
        connection._connection = None


class TestRequiresConnection:
    @staticmethod
    def test_translates_exasol_errors_and_preserves_cause(source_connection):
        source = ExaQueryError(
            source_connection,
            "COMMIT",
            1234,
            "transaction failed",
        )
        underlying_connection = Mock()
        underlying_connection.commit.side_effect = source
        connection = Connection()
        connection._connection = underlying_connection

        try:
            with pytest.raises(ProgrammingError) as exception_info:
                connection.commit()

            assert str(exception_info.value) == "transaction failed"
            assert exception_info.value.__cause__ is source
        finally:
            connection._connection = None

    @staticmethod
    def test_no_connection():
        class MyConnection:
            def __init__(self, con=None):
                self._connection = con

            @_requires_connection
            def close(self):
                pass

            def connect(self):
                self._connection = object()

        connection = MyConnection()
        with pytest.raises(InterfaceError) as exception:
            connection.close()

        assert f"{exception.value}" == "No active connection available"

    @staticmethod
    def test_connection_available():
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

    @staticmethod
    def test_preserves_wrapped_name():
        class MyConnection:
            @_requires_connection
            def close(self):
                return True

        connection = MyConnection()
        assert connection.close.__name__ == "close"
