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
def csv_file(tmp_path, data):
    file = tmp_path / "names.csv"
    with open(file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, dialect="unix")
        for row in data:
            writer.writerow(row)
    yield file


@pytest.mark.etl
def test_import_csv(connection, empty_table, csv_file, data):
    connection.import_from_file(csv_file, empty_table)
    result = connection.execute(f"SELECT * FROM {empty_table};")

    expected = set(data)
    actual = set(result.fetchall())

    assert actual == expected


@pytest.mark.etl
def test_import_with_reordered_columns(connection, empty_table, csv_file, swaped_data):
    params = {"columns": ["LASTNAME", "FIRSTNAME"]}
    connection.import_from_file(csv_file, empty_table, import_params=params)
    result = connection.execute(f"SELECT * FROM {empty_table};")

    expected = set(swaped_data)
    actual = set(result.fetchall())

    assert actual == expected


@pytest.mark.etl
def test_custom_import_callback(connection, empty_table, csv_file, data):
    def import_cb(pipe, src):
        pipe.write(src.read_bytes())

    connection.import_from_callback(import_cb, csv_file, empty_table)
    result = connection.execute(f"SELECT * FROM {empty_table};")

    expected = set(data)
    actual = set(result.fetchall())

    assert actual == expected


@pytest.mark.etl
def test_skip_rows_in_import(connection, empty_table, csv_file, data):
    offset = 2
    params = {"skip": offset}
    connection.import_from_file(csv_file, empty_table, import_params=params)
    result = connection.execute(f"SELECT * FROM {empty_table};")

    expected = set(data[offset::])
    actual = set(result.fetchall())

    assert actual == expected
