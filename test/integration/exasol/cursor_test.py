import datetime
import decimal
import uuid

import pytest

import pyexasol
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


def test_fetches_all_exasol_data_types_as_python_values(connection):
    table_name = f"DBAPI_ALL_TYPES_{uuid.uuid4().hex.upper()}"
    cursor = connection.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                decimal_integer DECIMAL(18, 0),
                decimal_fraction DECIMAL(18, 3),
                double_value DOUBLE,
                char_value CHAR(5),
                varchar_value VARCHAR(20),
                date_value DATE,
                timestamp_value TIMESTAMP(6),
                timestamp_local_value TIMESTAMP(6) WITH LOCAL TIME ZONE,
                interval_year_month INTERVAL YEAR(4) TO MONTH,
                interval_day_second INTERVAL DAY(9) TO SECOND(6),
                boolean_value BOOLEAN,
                geometry_value GEOMETRY,
                hashtype_value HASHTYPE(16 BYTE)
            )
            """)
        cursor.execute(f"""
            INSERT INTO {table_name} VALUES (
                42,
                42.125,
                3.5,
                'abc',
                'hello',
                DATE '2020-01-02',
                TIMESTAMP '2020-01-02 03:04:05.123456',
                TIMESTAMP '2020-01-02 03:04:05.123456',
                INTERVAL '2-3' YEAR TO MONTH,
                INTERVAL '2 03:04:05.123456' DAY TO SECOND,
                TRUE,
                'POINT (10 20)',
                '550e8400-e29b-11d4-a716-446655440000'
            )
            """)
        cursor.execute(f"SELECT * FROM {table_name}")
        actual = cursor.fetchone()

        expected = (
            42,
            decimal.Decimal("42.125"),
            3.5,
            "abc  ",
            "hello",
            datetime.date(2020, 1, 2),
            datetime.datetime(2020, 1, 2, 3, 4, 5, 123456),
            datetime.datetime(2020, 1, 2, 3, 4, 5, 123456),
            "+0002-03",
            pyexasol.mapper.ExaTimeDelta(
                days=2, hours=3, minutes=4, seconds=5, microseconds=123000
            ),
            True,
            "POINT (10 20)",
            "550e8400e29b11d4a716446655440000",
        )
        assert actual == expected
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.close()
