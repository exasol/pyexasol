import datetime
import decimal
from test.data_type_cases import DATA_TYPE_CASES
from typing import NamedTuple

import pytest

from exasol.driver.websocket.dbapi2 import connect
from pyexasol.data_types import ExaTimeDelta


class DataTypeRow(NamedTuple):
    """Input and fetched values for one row of the all-types test table."""

    decimal_integer: decimal.Decimal | int | str
    decimal_fraction: decimal.Decimal | float | str
    double_value: decimal.Decimal | float | str
    char_value: str
    varchar_value: str
    date_value: datetime.date | str
    timestamp_value: datetime.datetime | str
    timestamp_local_value: str
    interval_year_month: str
    interval_day_second: str | ExaTimeDelta
    boolean_value: bool
    geometry_value: str
    hashtype_value: str


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
        CREATE OR REPLACE TABLE {schema_table} (
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
        DataTypeRow(
            DATA_TYPE_CASES.decimal_scale_0.python_value,
            DATA_TYPE_CASES.decimal_scale_2.python_value,
            DATA_TYPE_CASES.double.python_value,
            DATA_TYPE_CASES.char.python_value,
            DATA_TYPE_CASES.varchar.python_value,
            DATA_TYPE_CASES.date.python_value,
            DATA_TYPE_CASES.timestamp.python_value,
            DATA_TYPE_CASES.timestamp_with_local_time_zone.python_value,
            # Prepared interval values must fit the target column's declared precision.
            # Values whose year or fractional-second precision exceeds that precision
            # are not handled by the driver; the database rejects them. ExaTimeDelta
            # values are not handled by the driver either. This is tracked in:
            # https://github.com/exasol/pyexasol/issues/428
            DATA_TYPE_CASES.interval_year_to_month.python_value,
            DATA_TYPE_CASES.interval_day_to_second.websocket_value,
            DATA_TYPE_CASES.boolean.python_value,
            DATA_TYPE_CASES.geometry.python_value,
            DATA_TYPE_CASES.hashtype.python_value,
        ),
        DataTypeRow(
            decimal.Decimal("-7"),
            -7.5,
            decimal.Decimal("-2.25"),
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
        DataTypeRow(
            "0",
            "0.001",
            "0.0",
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
def expected_rows(rows):
    """Return rows as returned after fetch_mapper conversion."""
    return (
        rows[0]._replace(
            decimal_fraction=decimal.Decimal("123.45"),
            # CHAR(5) pads the four-character input with a trailing space.
            char_value="text ",
            date_value=datetime.date(2026, 9, 11),
            timestamp_value=datetime.datetime(2026, 9, 11, 12, 34, 56),
            # TIMESTAMP WITH LOCAL TIME ZONE is not converted to datetime yet;
            # the fetched value remains a string. This will change with:
            # https://github.com/exasol/pyexasol/issues/116
            timestamp_local_value="2026-09-11 12:34:56.000000",
            interval_day_second=ExaTimeDelta(
                days=3,
                seconds=39599,
                microseconds=123000,
            ),
        ),
        rows[1]._replace(
            decimal_integer=-7,
            decimal_fraction=decimal.Decimal("-7.5"),
            double_value=-2.25,
            char_value="xy   ",
            date_value=datetime.date(2021, 3, 4),
            timestamp_value=datetime.datetime(2021, 3, 4, 5, 6, 7, 654000),
            timestamp_local_value="2021-03-04 05:06:07.654000",
            interval_day_second=ExaTimeDelta(
                days=3,
                seconds=14706,
                microseconds=654000,
            ),
        ),
        rows[2]._replace(
            decimal_integer=0,
            decimal_fraction=decimal.Decimal("0.001"),
            double_value=0.0,
            char_value="z    ",
            date_value=datetime.date(2022, 12, 31),
            timestamp_value=datetime.datetime(2022, 12, 31, 23, 59, 59),
            timestamp_local_value="2022-12-31 23:59:59.000000",
            interval_day_second=ExaTimeDelta(0),
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
