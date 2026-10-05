import pytest

from exasol.driver.websocket.dbapi2 import (
    DatabaseError,
    InterfaceError,
    TypeCode,
)
from pyexasol.data_types.websocket_to_python import (
    SUPPORTED_DATE_FORMATS,
    SUPPORTED_TIMESTAMP_FORMATS,
)


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
    @pytest.mark.dbapi_type_conversion
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
    @pytest.mark.dbapi_type_conversion
    def test_execute_with_parameters_inserts_row(empty_table, rows, cursor):
        """Verify execute(parameters) adapts and inserts one all-types fixture row."""
        cursor.execute(
            f"INSERT INTO {empty_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            rows[0],
        )

        cursor.execute(f"SELECT COUNT(*) FROM {empty_table};")
        assert cursor.fetchone()[0] == 1

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


class TestFetchAll:
    @staticmethod
    @pytest.mark.dbapi_type_conversion
    def test_fetches_all_rows(cursor, filled_table, rows):
        """Verify DBAPI fetching and Python conversion for every supported type."""
        cursor.execute(f"SELECT * FROM {filled_table};")
        result = cursor.fetchall()

        assert result == rows


class TestFetchMany:
    @staticmethod
    @pytest.mark.dbapi_type_conversion
    def test_fetches_rows_in_batches(cursor, filled_table, rows):
        size = 2
        assert size < len(rows)
        cursor.execute(f"SELECT * FROM {filled_table};")

        assert cursor.fetchmany(size) == rows[:size]
        assert cursor.fetchmany(size) == rows[size:]
        assert cursor.fetchmany(size) == ()

    @staticmethod
    def test_fetches_rows_using_arraysize(cursor, filled_table, rows):
        cursor.execute(f"SELECT * FROM {filled_table};")

        for expected_row in rows:
            assert cursor.fetchmany() == (expected_row,)

        assert cursor.fetchmany() == ()

    @staticmethod
    def test_fetches_all_rows_when_size_exceeds_result(cursor, filled_table, rows):
        size = 4
        assert size > len(rows)
        cursor.execute(f"SELECT * FROM {filled_table};")

        assert cursor.fetchmany(size) == rows
        assert cursor.fetchmany(size) == ()


class TestFetchOne:
    @staticmethod
    @pytest.mark.dbapi_type_conversion
    def test_fetches_rows_one_at_a_time(cursor, filled_table, rows):
        cursor.execute(f"SELECT * FROM {filled_table};")

        for expected_row in rows:
            assert cursor.fetchone() == expected_row

        assert cursor.fetchone() is None


class TestDescription:
    @staticmethod
    def test_before_execute(cursor):
        assert cursor.description is None

    @staticmethod
    def test_table_description(cursor, filled_table, rows):
        cursor.execute(f"SELECT * FROM {filled_table};")
        cursor.fetchone()

        assert cursor.description == (
            ("DECIMAL_INTEGER", TypeCode.Decimal, None, None, 18, 0, None),
            ("DECIMAL_FRACTION", TypeCode.Decimal, None, None, 18, 3, None),
            ("DOUBLE_VALUE", TypeCode.Double, None, None, None, None, None),
            ("CHAR_VALUE", TypeCode.Char, None, 5, None, None, None),
            ("VARCHAR_VALUE", TypeCode.String, None, 20, None, None, None),
            ("DATE_VALUE", TypeCode.Date, None, 4, None, None, None),
            ("TIMESTAMP_VALUE", TypeCode.Timestamp, None, 16, None, None, None),
            ("TIMESTAMP_LOCAL_VALUE", TypeCode.TimestampTz, None, 16, None, None, None),
            (
                "INTERVAL_YEAR_MONTH",
                TypeCode.IntervalYearToMonth,
                None,
                8,
                4,
                None,
                None,
            ),
            (
                "INTERVAL_DAY_SECOND",
                TypeCode.IntervalDayToSecond,
                None,
                26,
                9,
                None,
                None,
            ),
            ("BOOLEAN_VALUE", TypeCode.Bool, None, None, None, None, None),
            ("GEOMETRY_VALUE", TypeCode.Geometry, None, 2000000, None, None, None),
            ("HASHTYPE_VALUE", TypeCode.Hashtype, None, 32, None, None, None),
        )

    @staticmethod
    def test_for_query(cursor):
        cursor.execute(
            "SELECT "
            "CAST(1 AS INT) AS DECIMAL_VALUE, "
            "CAST(1 AS DOUBLE) AS DOUBLE_VALUE, "
            "CAST(TRUE AS BOOLEAN) AS BOOLEAN_VALUE, "
            "CAST('Foo' AS VARCHAR(10)) AS VARCHAR_VALUE;"
        )

        assert cursor.description == (
            ("DECIMAL_VALUE", TypeCode.Decimal, None, None, 18, 0, None),
            ("DOUBLE_VALUE", TypeCode.Double, None, None, None, None, None),
            ("BOOLEAN_VALUE", TypeCode.Bool, None, None, None, None, None),
            ("VARCHAR_VALUE", TypeCode.String, None, 10, None, None, None),
        )
