import pytest


@pytest.fixture
def empty_table(connection, edge_case_ddl):
    table_name, ddl = edge_case_ddl
    connection.execute(ddl)
    connection.commit()

    yield table_name

    delete_stmt = f"DROP TABLE IF EXISTS {table_name};"
    connection.execute(delete_stmt)
    connection.commit()


@pytest.fixture
def poplulate_edge_case_table(connection, empty_table, edge_cases):
    table = "edge_case"
    stmt = (
        f"INSERT INTO {table} VALUES ({{DEC36_0!d}}, {{DEC36_36!d}}, {{DBL!f}}, "
        "{BL}, {DT}, {TS}, {VAR100}, {VAR2000000})"
    )
    for case in edge_cases.values():
        connection.execute(stmt, dict(case))

    connection.commit()

    yield table


@pytest.mark.edge_case
def test_insert(connection, empty_table, edge_cases):
    stmt = (
        f"INSERT INTO {empty_table} VALUES ({{DEC36_0!d}}, {{DEC36_36!d}}, {{DBL!f}}, "
        "{BL}, {DT}, {TS}, {VAR100}, {VAR2000000})"
    )
    for case in edge_cases.values():
        connection.execute(stmt, dict(case))

    expected = len(edge_cases)
    actual = connection.execute(f"SELECT COUNT(*) FROM {empty_table};").fetchval()

    assert actual == expected


@pytest.mark.edge_case
def test_select_and_fetch(connection, edge_cases, poplulate_edge_case_table):
    query = (
        "SELECT DEC36_0, DEC36_36, DBL, BL, DT, TS, VAR100, "
        "LENGTH(var2000000) AS len_var FROM edge_case"
    )

    result = connection.execute(query).fetchall()

    expected = len(edge_cases.values())
    actual = len(result)

    assert actual == expected


@pytest.mark.edge_case
def test_very_long_query(connection, edge_cases):
    query = (
        "SELECT {VAL1} AS VAL1, {VAL2} AS VAL2, "
        "{VAL3} AS VAL3, {VAL4} AS VAL4, {VAL5} AS VAL5"
    )
    case = "Biggest-Values"
    params = {
        "VAL1": edge_cases[case]["VAR2000000"],
        "VAL2": edge_cases[case]["VAR2000000"],
        "VAL3": edge_cases[case]["VAR2000000"],
        "VAL4": edge_cases[case]["VAR2000000"],
        "VAL5": edge_cases[case]["VAR2000000"],
    }

    result = connection.execute(query, params)

    expected = 10000065
    actual = len(result.query)

    assert actual == expected
