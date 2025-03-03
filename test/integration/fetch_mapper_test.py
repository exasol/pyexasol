import datetime
import decimal

import pytest

import pyexasol


# For the fetch_mapper tests we need to configure the connection accordingly
@pytest.fixture
def connection(dsn, user, password, schema, websocket_sslopt):
    con = pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        websocket_sslopt=websocket_sslopt,
        fetch_mapper=pyexasol.exasol_mapper,
    )
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
        ("SELECT TO_DSINTERVAL('3 10:59:59.123');", pyexasol.mapper.ExaTimeDelta),
        ("SELECT CAST(1 AS BOOLEAN);", bool),
        ("SELECT CAST(1 AS VARCHAR(1));", str),
        ("SELECT CAST(1 AS CHAR);", str),
        ("SELECT ST_BOUNDARY('POINT (10 20)');", str),
    ],
)
def test_fetch_mapper_setting_together_with_exasol_mapper(connection, expected, sql):
    result = connection.execute(sql)
    actual = type(result.fetchval())
    assert expected == actual
