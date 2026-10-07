import datetime
import decimal

import pytest

from pyexasol.data_types import (
    ExaTimeDelta,
    convert_websocket_to_python,
)


# For the fetch_mapper tests we need to configure the connection accordingly
@pytest.fixture
def connection(connection_factory):
    con = connection_factory(fetch_mapper=convert_websocket_to_python)
    yield con
    con.close()


@pytest.mark.fetch_mapper
@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT CAST(1 AS DECIMAL(18,0));", int),
        ("SELECT CAST(1 AS DECIMAL(18,2));", decimal.Decimal),
        ("SELECT CAST(1 AS DOUBLE);", float),
        ("SELECT DATE '2024-05-13';", datetime.date),
        ("SELECT TIMESTAMP '2024-05-13 12:34:56';", datetime.datetime),
        (
            "SELECT CAST('2024-05-13 12:34:56' AS TIMESTAMP WITH LOCAL TIME ZONE);",
            datetime.datetime,
        ),
        ("SELECT TO_DSINTERVAL('3 10:59:59.123');", ExaTimeDelta),
        ("SELECT CAST(1 AS BOOLEAN);", bool),
        ("SELECT CAST(1 AS VARCHAR(1));", str),
        ("SELECT CAST(1 AS CHAR);", str),
        ("SELECT ST_BOUNDARY('POINT (10 20)');", str),
    ],
)
def test_fetch_mapper_with_convert_websocket_to_python(connection, expected, sql):
    result = connection.execute(sql)
    actual = type(result.fetchval())
    assert expected == actual
