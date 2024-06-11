import pytest
import pyexasol
import decimal

from inspect import cleandoc


@pytest.fixture
def connection_factory(dsn, user, password, schema):
    connections = []

    def factory(json_lib):
        con = pyexasol.connect(
            dsn=dsn,
            user=user,
            password=password,
            schema=schema,
            json_lib=json_lib,
        )
        connections.append(con)
        return con

    yield factory

    for c in connections:
        c.close()


@pytest.fixture
def table(connection):
    name = "edge_case"
    ddl = cleandoc(
        f"""CREATE OR REPLACE TABLE {name}
        (
            dec36_0         DECIMAL(36,0),
            dec36_36        DECIMAL(36,36),
            dbl             DOUBLE,
            bl              BOOLEAN,
            dt              DATE,
            ts              TIMESTAMP,
            var100          VARCHAR(100),
            var2000000      VARCHAR(2000000)
        )
        """
    )
    connection.execute(ddl)
    connection.commit()

    yield name

    delete_stmt = f"DROP TABLE IF EXISTS {name};"
    connection.execute(delete_stmt)
    connection.commit()


@pytest.fixture
def edge_cases():
    return [
        # Biggest values
        {
            "DEC36_0": decimal.Decimal("+" + ("9" * 36)),
            "DEC36_36": decimal.Decimal("+0." + ("9" * 36)),
            "DBL": 1.7e308,
            "BL": True,
            "DT": "9999-12-31",
            "TS": "9999-12-31 23:59:59.999",
            "VAR100": "ひ" * 100,
            "VAR2000000": "ひ" * 2000000,
        },
        # Smallest values
        {
            "DEC36_0": decimal.Decimal("-" + ("9" * 36)),
            "DEC36_36": decimal.Decimal("-0." + ("9" * 36)),
            "DBL": -1.7e308,
            "BL": False,
            "DT": "0001-01-01",
            "TS": "0001-01-01 00:00:00",
            "VAR100": "",
            "VAR2000000": "ひ",
        },
        # All nulls
        {
            "DEC36_0": None,
            "DEC36_36": None,
            "DBL": None,
            "BL": None,
            "DT": None,
            "TS": None,
            "VAR100": None,
            "VAR2000000": None,
        },
    ]


@pytest.mark.json
@pytest.mark.parametrize("json_lib", ["orjson", "ujson", "rapidjson"])
def test_insert(table, connection_factory, edge_cases, json_lib):
    connection = connection_factory(json_lib)
    insert_stmt = (
        "INSERT INTO edge_case VALUES"
        "({DEC36_0!d}, {DEC36_36!d}, {DBL!f}, {BL}, {DT}, {TS}, {VAR100}, {VAR2000000})"
    )
    for edge_case in edge_cases:
        connection.execute(insert_stmt, edge_case)

    expected = len(edge_cases)
    actual = connection.execute(f"SELECT COUNT(*) FROM {table};").fetchval()

    assert actual == expected


@pytest.mark.json
@pytest.mark.parametrize("json_lib", ["orjson", "ujson", "rapidjson"])
def test_select(table, connection_factory, edge_cases, json_lib):
    connection = connection_factory(json_lib)

    insert_stmt = (
        "INSERT INTO edge_case VALUES"
        "({DEC36_0!d}, {DEC36_36!d}, {DBL!f}, {BL}, {DT}, {TS}, {VAR100}, {VAR2000000})"
    )
    for edge_case in edge_cases:
        connection.execute(insert_stmt, edge_case)

    select_stmt = (
        "SELECT DEC36_0, DEC36_36, DBL, BL, DT, TS, VAR100, LENGTH(VAR2000000) "
        "AS LEN_VAR FROM EDGE_CASE"
    )

    expected = {
        # Biggest values
        ("9" * 36, f"0.{'9' * 36}", 1.7e308, True, "9999-12-31", "9999-12-31 23:59:59.999000", "ひ" * 100, 2000000),
        # Smallest values
        (f"-{'9' * 36}", f"-0.{'9' * 36}", -1.7e308, False, "0001-01-01", "0001-01-01 00:00:00.000000", None, 1),
        # All nulls
        (None, None, None, None, None, None, None, None),
    }
    actual = set(connection.execute(select_stmt).fetchall())

    assert actual == expected
