import pytest

from exasol.driver.websocket._connection import (
    SUPPORTED_DATE_FORMATS,
    SUPPORTED_TIMESTAMP_FORMATS,
)
from exasol.driver.websocket.dbapi2 import InterfaceError


class TestSessionDatetimeFormats:
    @staticmethod
    def test_accepts_supported_formats(cursor):
        cursor.execute(
            f"ALTER SESSION SET NLS_DATE_FORMAT = '{SUPPORTED_DATE_FORMATS[0]}'"
        )
        cursor.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = "
            f"'{SUPPORTED_TIMESTAMP_FORMATS[0]}'"
        )

        cursor.execute("SELECT 1")

        assert cursor.fetchone() == (1,)

    @staticmethod
    def test_rejects_unsupported_formats(cursor):
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY'")
        cursor.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'DD.MM.YYYY HH24:MI:SS'"
        )

        with pytest.raises(InterfaceError) as exception_info:
            cursor.execute("SELECT 1")

        message = str(exception_info.value)
        assert "Unsupported NLS_DATE_FORMAT 'DD.MM.YYYY'" in message
        assert "Unsupported NLS_TIMESTAMP_FORMAT 'DD.MM.YYYY HH24:MI:SS'" in message
        assert (
            "- Fix with: ALTER SESSION SET NLS_DATE_FORMAT = '<supported-format>';"
            in message
        )
        assert (
            "- Fix with: ALTER SESSION SET NLS_TIMESTAMP_FORMAT = "
            "'<supported-format>';" in message
        )
        assert "\n\n" in message
