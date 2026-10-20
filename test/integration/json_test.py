import pytest


@pytest.fixture
def multiple_connection_factory(connection_factory):
    connections = []

    def factory(json_lib):
        con = connection_factory(json_lib=json_lib)
        connections.append(con)
        return con

    yield factory

    for c in connections:
        c.close()


@pytest.mark.json
@pytest.mark.parametrize("json_lib", ["orjson", "ujson", "rapidjson"])
def test_insert(edge_case_ddl, multiple_connection_factory, edge_cases, json_lib):
    connection = multiple_connection_factory(json_lib)
    insert_stmt = (
        "INSERT INTO edge_case VALUES"
        "({DEC36_0!d}, {DEC36_36!d}, {DBL!f}, {BL}, {DT}, {TS}, {VAR100}, {VAR2000000})"
    )
    for edge_case in edge_cases.values():
        connection.execute(insert_stmt, edge_case)

    expected = len(edge_cases)
    actual = connection.execute(f"SELECT COUNT(*) FROM {edge_case_ddl};").fetchval()

    assert actual == expected


@pytest.mark.json
@pytest.mark.parametrize("json_lib", ["orjson", "ujson", "rapidjson"])
def test_select(edge_case_ddl, multiple_connection_factory, edge_cases, json_lib):
    connection = multiple_connection_factory(json_lib)

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
