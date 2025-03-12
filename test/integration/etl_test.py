from inspect import cleandoc

import pytest

import pyexasol


@pytest.fixture
def connection(connection_factory):
    with connection_factory(compression=True) as con:
        yield con


@pytest.fixture
def export_file(tmp_path):
    yield tmp_path / "export"


@pytest.fixture
def table(connection):
    yield "USERS"


@pytest.fixture
def import_table(connection, table):
    name = f"{table}_IMPORT"
    ddl = cleandoc(
        f"""
    CREATE TABLE IF NOT EXISTS {name}
    (
        user_id         DECIMAL(18,0),
        user_name       VARCHAR(255),
        register_dt     DATE,
        last_visit_ts   TIMESTAMP,
        is_female       BOOLEAN,
        user_rating     DECIMAL(10,5),
        user_score      DOUBLE,
        status          VARCHAR(50)
    );
    """
    )
    connection.execute(ddl)
    connection.commit()

    yield name

    ddl = f"DROP TABLE IF EXISTS {name};"
    connection.execute(ddl)
    connection.commit()


@pytest.mark.etl
@pytest.mark.parametrize(
    "params",
    [
        {},
        {"format": "gz"},
        {"encoding": "WINDOWS-1251"},
        {"columns": ["register_dt", "user_id", "status", "user_name"]},
        {
            "csv_cols": [
                "1",
                "2",
                "3 FORMAT='DD-MM-YYYY'",
                "4..6",
                "7 FORMAT='999.99999'",
                "8",
            ]
        },
    ],
)
def test_export_import_round_trip_to_and_from_file(
    connection, export_file, table, import_table, params
):
    connection.export_to_file(export_file, table, export_params=params)
    connection.import_from_file(export_file, import_table, import_params=params)

    query = cleandoc(
        f"""
        SELECT * FROM {table}
        WHERE USER_ID NOT IN (SELECT USER_ID from {import_table});
        """
    )
    result = connection.execute(query)

    expected = []
    actual = result.fetchall()

    assert actual == expected
