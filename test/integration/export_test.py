import csv
import pytest
import pyexasol
from inspect import cleandoc


@pytest.fixture
def connection(dsn, user, password, schema):
    with pyexasol.connect(
        dsn=dsn, user=user, password=password, schema=schema, compression=True
    ) as con:
        yield con


@pytest.fixture
def table_name():
    yield "CLIENT_NAMES"


@pytest.fixture
def empty_table(connection, table_name):
    ddl = cleandoc(f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    (
        FIRSTNAME       VARCHAR(255),
        LASTNAME        VARCHAR(255)
    );
    """)
    connection.execute(ddl)
    connection.commit()

    yield table_name

    ddl = f"DROP TABLE IF EXISTS {table_name};"
    connection.execute(ddl)
    connection.commit()


@pytest.fixture
def data(faker):
    yield [(faker.first_name(), faker.last_name()) for _ in range(0, 10)]


@pytest.fixture
def swaped_data(data):
    yield [(lastname, firstname) for firstname, lastname in data]


@pytest.fixture
def table(connection, empty_table, data):
    name = empty_table
    stmt = "INSERT INTO {table} VALUES ({{col1}}, {{col2}});"
    for col1, col2 in data:
        connection.execute(stmt.format(table=name), {"col1": col1, "col2":col2})
    connection.commit()
    yield name


@pytest.fixture
def export_file(tmp_path, data):
    yield tmp_path / "names.csv"


@pytest.mark.etl
def test_export_with_column_names():
    assert False


@pytest.mark.etl
def test_skip_rows_in_export():
    assert False


@pytest.mark.etl
def test_custom_export_callback(connection, table, data, export_file):
    def export_cb(pipe, dst):
        dst.write_bytes(pipe.read())

    connection.export_to_callback(export_cb, export_file, table)

    expected = 0 # TBD
    actual = None # TBD

    assert actual == expected


@pytest.mark.etl
def test_export_with_reodered_columns():
    assert False


@pytest.mark.etl
def test_export_with_custom_csv_format():
    assert False
