import datetime

import pytest

from exasol.driver.websocket.dbapi2 import connect


@pytest.fixture
def connection(dsn, user, password, schema):
    dbapi_connection = connect(
        dsn=dsn,
        username=user,
        password=password,
        schema=schema,
        certificate_validation=False,
    )
    yield dbapi_connection
    dbapi_connection.close()


@pytest.fixture
def cursor(connection):
    dbapi_cursor = connection.cursor()
    yield dbapi_cursor
    dbapi_cursor.close()


@pytest.fixture(scope="session")
def schema_table(schema):
    return f"{schema}.DATA_TYPES"


@pytest.fixture
def empty_table(cursor, schema_table):
    """Create an empty table using Exasol's supported data types.

    See https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm
    """
    cursor.execute(f"""
        CREATE TABLE {schema_table} (
            decimal_integer DECIMAL(18, 0),
            decimal_fraction DECIMAL(18, 3),
            double_value DOUBLE,
            char_value CHAR(5),
            varchar_value VARCHAR(20),
            date_value DATE,
            timestamp_value TIMESTAMP(6),
            -- Custom fractional-second precision for this type was introduced in Exasol 8.32.0:
            -- https://docs.exasol.com/db/latest/changelogs/13409.htm
            timestamp_local_value TIMESTAMP WITH LOCAL TIME ZONE,
            interval_year_month INTERVAL YEAR(4) TO MONTH,
            interval_day_second INTERVAL DAY(9) TO SECOND(6),
            boolean_value BOOLEAN,
            geometry_value GEOMETRY,
            hashtype_value HASHTYPE(16 BYTE)
        )
        """)

    yield schema_table

    cursor.execute(f"DROP TABLE IF EXISTS {schema_table};")


@pytest.fixture
def rows():
    """Return three Python value rows covering the data-types table columns."""
    return (
        (
            42,
            "42.125",
            3.5,
            "abc  ",
            "hello",
            datetime.date(2020, 1, 2),
            "2020-01-02 03:04:05.123000",
            "2020-01-02 03:04:05.123000",
            "+0002-03",
            "+000000002 03:04:05.123000",
            True,
            "POINT (10 20)",
            "550e8400e29b11d4a716446655440000",
        ),
        (
            -7,
            "-7.5",
            -2.25,
            "xy   ",
            "world",
            datetime.date(2021, 3, 4),
            "2021-03-04 05:06:07.654000",
            "2021-03-04 05:06:07.654000",
            "+0001-11",
            "+000000003 04:05:06.654000",
            False,
            "LINESTRING (10 20, 30 40)",
            "6ba7b8109dad11d180b400c04fd430c8",
        ),
        (
            0,
            "0.001",
            0.0,
            "z    ",
            "Exasol",
            datetime.date(2022, 12, 31),
            "2022-12-31 23:59:59.000000",
            "2022-12-31 23:59:59.000000",
            "+0000-00",
            "+000000000 00:00:00.000000",
            True,
            "POLYGON ((10 20, 30 40, 50 20, 10 20))",
            "00000000000000000000000000000000",
        ),
    )


@pytest.fixture
def filled_table(cursor, empty_table, rows):
    """Insert the data-types fixture rows into the empty table."""
    cursor.executemany(
        f"INSERT INTO {empty_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return empty_table
