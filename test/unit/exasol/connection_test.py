"""Unit tests for the WebSocket DBAPI connection wrapper."""

from contextlib import contextmanager
from unittest.mock import Mock

import pytest

from exasol.driver.websocket._connection import (
    SUPPORTED_DATE_FORMATS,
    SUPPORTED_TIMESTAMP_FORMATS,
    Connection,
    _is_alter_session,
    _requires_connection,
    _validate_session_datetime_formats,
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
            pytest.param(
                "/* application tag */ ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
                id="leading-block-comment",
            ),
            pytest.param(
                "/* application\n                   tag */ ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
                id="leading-multiline-block-comment",
            ),
            pytest.param(
                "-- application tag\nALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
                id="leading-line-comment",
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
            pytest.param("/* unclosed comment", id="unclosed-block-comment"),
            pytest.param("-- comment without statement", id="line-comment-only"),
            pytest.param(None, id="non-string-operation"),
        ],
    )
    def test_rejects_other_text(self, operation):
        assert not _is_alter_session(operation)


class TestValidateSessionDatetimeFormats:
    @staticmethod
    @contextmanager
    def _connection_with_session_formats(date_format, timestamp_format):
        connection = Connection()
        connection._connection = Mock()
        result = Mock()
        result.fetchall.return_value = [
            ("NLS_DATE_FORMAT", date_format),
            ("NLS_TIMESTAMP_FORMAT", timestamp_format),
        ]
        connection._connection.execute.return_value = result
        try:
            yield connection
        finally:
            connection._connection = None

    def test_accepts_supported_formats(self):
        with self._connection_with_session_formats(
            SUPPORTED_DATE_FORMATS[0],
            SUPPORTED_TIMESTAMP_FORMATS[0],
        ) as connection:
            connection.validate_session_datetime_formats("SELECT 1")

    def test_rejects_unsupported_formats(self):
        with self._connection_with_session_formats(
            "DD.MM.YYYY",
            "DD.MM.YYYY HH24:MI:SS",
        ) as connection:
            with pytest.raises(InterfaceError) as exception_info:
                connection.validate_session_datetime_formats("SELECT 1")

        message = str(exception_info.value)
        assert "Unsupported NLS_DATE_FORMAT 'DD.MM.YYYY'" in message
        assert "Unsupported NLS_TIMESTAMP_FORMAT 'DD.MM.YYYY HH24:MI:SS'" in message

    def test_skips_alter_session_operations(self):
        connection = Connection()
        connection._connection = Mock()

        connection.validate_session_datetime_formats(
            "ALTER  SESSION SET NLS_TIMESTAMP_FORMAT = 'unsupported'"
        )

        connection._connection.execute.assert_not_called()
        connection._connection = None


class TestValidateSessionDatetimeFormatsHelper:
    @staticmethod
    @pytest.mark.parametrize("timestamp_format", SUPPORTED_TIMESTAMP_FORMATS)
    def test_accepts_formats(timestamp_format):
        _validate_session_datetime_formats(
            {
                "NLS_DATE_FORMAT": SUPPORTED_DATE_FORMATS[0],
                "NLS_TIMESTAMP_FORMAT": timestamp_format,
            }
        )

    @staticmethod
    def test_rejects_format():
        with pytest.raises(InterfaceError, match="Unsupported NLS_DATE_FORMAT"):
            _validate_session_datetime_formats(
                {
                    "NLS_DATE_FORMAT": "DD.MM.YYYY",
                    "NLS_TIMESTAMP_FORMAT": SUPPORTED_TIMESTAMP_FORMATS[0],
                }
            )

    @staticmethod
    def test_reports_all_errors():
        with pytest.raises(InterfaceError) as exception_info:
            _validate_session_datetime_formats(
                {
                    "NLS_DATE_FORMAT": "DD.MM.YYYY",
                    "NLS_TIMESTAMP_FORMAT": "DD.MM.YYYY HH24:MI:SS",
                }
            )

        message = str(exception_info.value)
        assert "Unsupported NLS_DATE_FORMAT" in message
        assert "Unsupported NLS_TIMESTAMP_FORMAT" in message
        assert (
            "- Fix with: ALTER SESSION SET NLS_DATE_FORMAT = '<supported-format>';"
            in message
        )
        assert (
            "- Fix with: ALTER SESSION SET NLS_TIMESTAMP_FORMAT = "
            "'<supported-format>';" in message
        )
        assert "\n\n" in message


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
