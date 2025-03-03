import pytest

import pyexasol


@pytest.fixture
def connection_factory(dsn, user, password, schema, websocket_sslopt):
    connections = []

    def factory(json_lib):
        con = pyexasol.connect(
            dsn=dsn,
            user=user,
            password=password,
            schema=schema,
            websocket_sslopt=websocket_sslopt,
            json_lib=json_lib,
        )
        connections.append(con)
        return con

    yield factory

    for c in connections:
        c.close()


@pytest.fixture
def empty_table(connection, edge_case_ddl):
    table_name, ddl = edge_case_ddl
    connection.execute(ddl)
    connection.commit()

    yield table_name

    delete_stmt = f"DROP TABLE IF EXISTS {table_name};"
    connection.execute(delete_stmt)
    connection.commit()


@pytest.mark.json
@pytest.mark.parametrize("json_lib", ["orjson", "ujson", "rapidjson"])
def test_insert(empty_table, connection_factory, edge_cases, json_lib):
    connection = connection_factory(json_lib)
    insert_stmt = (
        "INSERT INTO edge_case VALUES"
        "({DEC36_0!d}, {DEC36_36!d}, {DBL!f}, {BL}, {DT}, {TS}, {VAR100}, {VAR2000000})"
    )
    for edge_case in edge_cases.values():
        connection.execute(insert_stmt, edge_case)

    expected = len(edge_cases)
    actual = connection.execute(f"SELECT COUNT(*) FROM {empty_table};").fetchval()

    assert actual == expected


@pytest.mark.json
@pytest.mark.parametrize("json_lib", ["orjson", "ujson", "rapidjson"])
def test_select(empty_table, connection_factory, edge_cases, json_lib):
    connection = connection_factory(json_lib)

    insert_stmt = (
        "INSERT INTO edge_case VALUES"
        "({DEC36_0!d}, {DEC36_36!d}, {DBL!f}, {BL}, {DT}, {TS}, {VAR100}, {VAR2000000})"
    )
    for edge_case in edge_cases.values():
        connection.execute(insert_stmt, edge_case)

    select_stmt = (
        "SELECT DEC36_0, DEC36_36, DBL, BL, DT, TS, VAR100, LENGTH(VAR2000000) "
        "AS LEN_VAR FROM EDGE_CASE"
    )

    expected = {
        # Biggest values
        (
            "9" * 36,
            f"0.{'9' * 36}",
            1.7e308,
            True,
            "9999-12-31",
            "9999-12-31 23:59:59.999000",
            "ひ" * 100,
            2000000,
        ),
        # Smallest values
        (
            f"-{'9' * 36}",
            f"-0.{'9' * 36}",
            -1.7e308,
            False,
            "0001-01-01",
            "0001-01-01 00:00:00.000000",
            None,
            1,
        ),
        # All nulls
        (None, None, None, None, None, None, None, None),
    }
    actual = set(connection.execute(select_stmt).fetchall())

    assert actual == expected
