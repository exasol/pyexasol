import pytest


@pytest.fixture
def populate_edge_case_table(connection, edge_case_ddl, edge_cases):
    stmt = (
        f"INSERT INTO {edge_case_ddl} VALUES ({{DEC36_0!d}}, {{DEC36_36!d}}, {{DBL!f}}, "
        "{BL}, {DT}, {TS}, {VAR100}, {VAR2000000})"
    )
    for case in edge_cases.values():
        connection.execute(stmt, dict(case))

    connection.commit()

    yield edge_case_ddl


@pytest.mark.edge_cases
def test_insert(connection, edge_case_ddl, edge_cases):
    stmt = (
        f"INSERT INTO {edge_case_ddl} VALUES ({{DEC36_0!d}}, {{DEC36_36!d}}, {{DBL!f}}, "
        "{BL}, {DT}, {TS}, {VAR100}, {VAR2000000})"
    )
    for case in edge_cases.values():
        connection.execute(stmt, dict(case))

    expected = len(edge_cases)
    actual = connection.execute(f"SELECT COUNT(*) FROM {edge_case_ddl};").fetchval()

    assert actual == expected


@pytest.mark.edge_cases
def test_select_and_fetch(connection, edge_cases, populate_edge_case_table):
    query = (
        "SELECT DEC36_0, DEC36_36, DBL, BL, DT, TS, VAR100, "
        "LENGTH(var2000000) AS len_var FROM edge_case"
    )

    result = connection.execute(query).fetchall()

    expected = len(edge_cases.values())
    actual = len(result)

    assert actual == expected


@pytest.mark.edge_cases
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
