from inspect import cleandoc

import polars as pl
import pytest

import pyexasol


@pytest.fixture
def connection_with_compression(dsn, user, password, schema):
    with pyexasol.connect(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        compression=True,
    ) as connection:
        yield connection


@pytest.fixture
def empty_table(connection):
    name = "USER_NAMES"
    ddl = cleandoc(
        f"""
        CREATE OR REPLACE TABLE {name}
        (
            FIRST_NAME VARCHAR(200),
            LAST_NAME VARCHAR(200)
        );
        """
    )
    connection.execute(ddl)
    connection.commit()

    yield name

    ddl = f"DROP TABLE IF EXISTS {name};"
    connection.execute(ddl)
    connection.commit()


@pytest.fixture
def names(faker):
    yield tuple(
        {"FIRST_NAME": faker.first_name(), "LAST_NAME": faker.last_name()}
        for _ in range(0, 10)
    )


@pytest.fixture
def table(connection, empty_table, names):
    insert = "INSERT INTO {table} VALUES({{FIRST_NAME}}, {{LAST_NAME}});"
    for name in names:
        stmt = insert.format(table=empty_table)
        connection.execute(stmt, name)

    yield empty_table, names


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.polars
def test_export_table_to_polars(connection, table):
    table_name, values = table

    expected = pl.from_records(values)
    actual = connection.export_to_polars(table_name)

    assert actual.equals(expected)


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.polars
def test_export_sql_result_to_polars(connection):

    query = "SELECT USER_NAME, USER_ID FROM USERS ORDER BY USER_ID ASC LIMIT 5;"

    expected = pl.from_records(
        data=[
            ("Jessica Mccoy", 0),
            ("Beth James", 1),
            ("Mrs. Teresa Ryan", 2),
            ("Tommy Henderson", 3),
            ("Jessica Christian", 4),
        ],
        schema=["USER_NAME", "USER_ID"],
        orient="row",
    )
    actual = connection.export_to_polars(query)

    assert actual.equals(expected)


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.polars
def test_import_from_polars(connection, empty_table, names):
    table_name = empty_table

    df = pl.from_records(names)
    connection.import_from_polars(df, table_name)
    connection.commit()

    result = connection.execute(
        f"SELECT FIRST_NAME, LAST_NAME FROM {table_name} ORDER BY FIRST_NAME ASC;"
    )
    actual = result.fetchall()
    expected = df.sort(pl.nth(0)).rows()

    assert actual == expected
