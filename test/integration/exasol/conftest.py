import datetime
import decimal
from test.data_type_cases import DATA_TYPE_CASES
from typing import NamedTuple

import pytest

from exasol.driver.websocket.dbapi2 import connect
from pyexasol.data_types import ExaTimeDelta


class DataTypeRow(NamedTuple):
    """Values for one row, used as input or expected fetched values."""

    decimal_integer: decimal.Decimal | int | str
    decimal_fraction: decimal.Decimal | float | str
    double_value: decimal.Decimal | float | str
    char_value: str
    varchar_value: str
    date_value: datetime.date | str
    timestamp_value: datetime.datetime | str
    timestamp_local_value: datetime.datetime | str
    interval_year_month: str
    interval_day_second: str | datetime.timedelta | ExaTimeDelta
    boolean_value: bool
    geometry_value: str
    hashtype_value: str


class DataTypeRowPair(NamedTuple):
    """Input and fetched values for one data-types test row."""

    input_row: DataTypeRow
    fetched_row: DataTypeRow


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
            interval_day_second INTERVAL DAY(4) TO SECOND(5),
            boolean_value BOOLEAN,
            geometry_value GEOMETRY,
            hashtype_value HASHTYPE(16 BYTE)
        )
        """)

    yield schema_table

    cursor.execute(f"DROP TABLE IF EXISTS {schema_table};")


@pytest.fixture
def data_type_rows():
    """Return input rows together with their expected fetched values.

    Three rows are used to exercise multi-row insertion and batched fetching.
    The batch-fetch test uses a size of two, leaving one row for the next batch;
    there is no domain-specific significance to the exact number three.

    The three rows intentionally use different input representations:
    - row 1 uses mostly native Python values from ``DATA_TYPE_CASES``. This
      verifies Python-to-WebSocket adaptation and deterministic conversion back
      to Python values.
    - row 2 uses strings for every type that accepts them. Boolean remains a
      Python ``bool`` because the adapter only supports boolean values as bools.
    - row 3 uses a mixture of alternative Python values and strings.

    Expected rows contain the normalized Python values returned after insertion,
    database conversion, fetching, and ``fetch_mapper`` conversion.
    """
    first_input = DataTypeRow(
        decimal_integer=DATA_TYPE_CASES.decimal_scale_0.python_value,
        decimal_fraction=DATA_TYPE_CASES.decimal_scale_2.python_value,
        double_value=DATA_TYPE_CASES.double.python_value,
        char_value=DATA_TYPE_CASES.char.python_value,
        varchar_value=DATA_TYPE_CASES.varchar.python_value,
        date_value=DATA_TYPE_CASES.date.python_value,
        timestamp_value=DATA_TYPE_CASES.timestamp.python_value,
        timestamp_local_value=(
            DATA_TYPE_CASES.timestamp_with_local_time_zone.python_value
        ),
        # Prepared interval values must fit the target column's declared precision.
        # Values whose year or fractional-second precision exceeds that precision
        # are not handled by the driver; the database rejects them. ExaTimeDelta
        # values are not handled by the driver either. The values below are
        # converted to fixed-width WebSocket interval strings where necessary.
        # This is tracked in:
        # https://github.com/exasol/pyexasol/issues/428
        interval_year_month=DATA_TYPE_CASES.interval_year_to_month.python_value,
        interval_day_second=DATA_TYPE_CASES.interval_day_to_second.websocket_value,
        boolean_value=DATA_TYPE_CASES.boolean.python_value,
        geometry_value=DATA_TYPE_CASES.geometry.python_value,
        hashtype_value=DATA_TYPE_CASES.hashtype.python_value,
    )
    first_row = DataTypeRowPair(
        input_row=first_input,
        fetched_row=first_input._replace(
            # CHAR(5) pads the four-character input with a trailing space.
            char_value="text ",
            interval_day_second=ExaTimeDelta(
                days=3,
                seconds=39599,
                microseconds=123000,
            ),
        ),
    )

    second_input = DataTypeRow(
        decimal_integer="0",
        decimal_fraction="0.001",
        double_value="0.0",
        char_value="z    ",
        varchar_value="Exasol",
        date_value="2022-12-31",
        timestamp_value="2022-12-31 23:59:59.000000",
        timestamp_local_value="2022-12-31 23:59:59.000000",
        interval_year_month="+0000-00",
        interval_day_second="+00 00:00:00.000",
        boolean_value=True,
        geometry_value="POLYGON ((10 20, 30 40, 50 20, 10 20))",
        hashtype_value="00000000000000000000000000000000",
    )
    second_row = DataTypeRowPair(
        input_row=second_input,
        fetched_row=second_input._replace(
            decimal_integer=0,
            decimal_fraction=decimal.Decimal("0.001"),
            double_value=0.0,
            char_value="z    ",
            date_value=datetime.date(2022, 12, 31),
            timestamp_value=datetime.datetime(2022, 12, 31, 23, 59, 59),
            timestamp_local_value=datetime.datetime(2022, 12, 31, 23, 59, 59),
            interval_day_second=ExaTimeDelta(0),
        ),
    )

    third_input = DataTypeRow(
        decimal_integer=decimal.Decimal("-7"),
        decimal_fraction=-7.5,
        double_value=decimal.Decimal("-2.25"),
        char_value="xy   ",
        varchar_value="world",
        date_value="2021-03-04",
        timestamp_value="2021-03-04 05:06:07.654000",
        timestamp_local_value="2021-03-04 05:06:07.654000",
        interval_year_month="+0001-11",
        interval_day_second="+02 03:04:05.500",
        boolean_value=False,
        geometry_value="LINESTRING (10 20, 30 40)",
        hashtype_value="6ba7b8109dad11d180b400c04fd430c8",
    )
    third_row = DataTypeRowPair(
        input_row=third_input,
        fetched_row=third_input._replace(
            decimal_integer=-7,
            decimal_fraction=decimal.Decimal("-7.5"),
            double_value=-2.25,
            char_value="xy   ",
            date_value=datetime.date(2021, 3, 4),
            timestamp_value=datetime.datetime(2021, 3, 4, 5, 6, 7, 654000),
            timestamp_local_value=datetime.datetime(2021, 3, 4, 5, 6, 7, 654000),
            interval_day_second=ExaTimeDelta(
                days=2,
                seconds=11045,
                microseconds=500000,
            ),
        ),
    )

    return first_row, second_row, third_row


@pytest.fixture
def input_rows(data_type_rows):
    """Return the rows inserted into the data-types table."""
    return tuple(row_pair.input_row for row_pair in data_type_rows)


@pytest.fixture
def fetched_rows(data_type_rows):
    """Return the rows expected from fetching and conversion."""
    return tuple(row_pair.fetched_row for row_pair in data_type_rows)


@pytest.fixture
def filled_table(cursor, empty_table, input_rows):
    """Insert the data-types fixture rows into the empty table."""
    cursor.executemany(
        f"INSERT INTO {empty_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        input_rows,
    )
    return empty_table
