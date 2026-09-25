import pytest

from exasol.driver.websocket._connection import (
    SUPPORTED_DATE_FORMATS,
    SUPPORTED_TIMESTAMP_FORMATS,
)
from exasol.driver.websocket._errors import DatabaseError
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


class TestExecuteMany:
    @staticmethod
    def test_inserts_multiple_rows(empty_table, rows, cursor):
        cursor.execute(f"SELECT COUNT(*) FROM {empty_table};")
        assert cursor.fetchone()[0] == 0

        cursor.executemany(
            f"INSERT INTO {empty_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            rows,
        )

        cursor.execute(f"SELECT COUNT(*) FROM {empty_table};")
        assert cursor.fetchone()[0] == len(rows)

    @staticmethod
    def test_rejects_rows_with_wrong_column_count(cursor, empty_table):
        with pytest.raises(DatabaseError):
            cursor.executemany(
                f"INSERT INTO {empty_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [(1, 2)],
            )


class TestRowCount:
    @staticmethod
    @pytest.mark.parametrize(
        "sql_statement,expected",
        (
            ("SELECT 1;", 1),
            ("SELECT * FROM VALUES TRUE, FALSE as T(A);", 2),
            ("SELECT * FROM VALUES TRUE, FALSE, TRUE as T(A);", 3),
            # ATTENTION: As of today 03.02.2023 it seems there is no trivial way to make this test pass.
            #            Also, it is unclear if this semantic is required in order to function correctly
            #            with SQLA.
            #
            #            NOTE: In order to implement this semantic, subclassing pyexasol.ExaConnection and
            #                  pyexasol.ExaStatement most likely will be required.
            pytest.param("DROP SCHEMA IF EXISTS FOOBAR;", -1, marks=pytest.mark.xfail),
        ),
    )
    def test_after_execute(cursor, sql_statement, expected):
        cursor.execute(sql_statement)
        assert cursor.rowcount == expected

    @staticmethod
    def test_after_fetchall(cursor, filled_table, rows):
        cursor.execute(f"SELECT decimal_integer FROM {filled_table};")
        cursor.fetchall()

        assert cursor.rowcount == len(rows)

    @staticmethod
    def test_before_execute(cursor):
        assert cursor.rowcount == -1
