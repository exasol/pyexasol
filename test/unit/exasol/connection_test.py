"""Unit tests for the WebSocket DBAPI connection wrapper."""

from unittest.mock import Mock

import pytest

from exasol.driver.websocket._connection import (
    Connection,
    _is_alter_session,
)


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
