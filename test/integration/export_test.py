import csv
import io
from inspect import cleandoc

import pytest

import pyexasol


@pytest.fixture
def connection(connection_factory):
    with connection_factory(compression=True) as con:
        yield con


@pytest.fixture
def connection_without_resolving_hostnames(connection_factory):
    with connection_factory(compression=True, resolve_hostnames=False) as con:
        yield con


@pytest.fixture
def table_name():
    yield "CLIENT_NAMES"


@pytest.fixture
def empty_table(connection, table_name):
    ddl = cleandoc(
        f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    (
        FIRSTNAME       VARCHAR(255),
        LASTNAME        VARCHAR(255)
    );
    """
    )
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
def table(connection, empty_table, data):
    name = empty_table
    stmt = "INSERT INTO {table} VALUES ({{col1}}, {{col2}});"
    for col1, col2 in data:
        connection.execute(stmt.format(table=name), {"col1": col1, "col2": col2})
    connection.commit()
    yield name


@pytest.fixture
def export_file(tmp_path, data):
    yield tmp_path / "names.csv"


@pytest.fixture
def csv_dialect():
    class PyexasolCsvDialect(csv.Dialect):
        lineterminator = "\n"
        delimiter = ","
        quoting = csv.QUOTE_MINIMAL
        quotechar = '"'

    yield PyexasolCsvDialect()


@pytest.fixture
def expected_csv(csv_dialect):
    def create_csv(table, data, **params):
        csvfile = io.StringIO()
        if "with_column_names" in params and params["with_column_names"]:
            data = [("FIRSTNAME", "LASTNAME")] + data
        writer = csv.writer(csvfile, csv_dialect)
        writer.writerows(data)
        return csvfile.getvalue()

    yield create_csv


@pytest.mark.etl
def test_export_with_column_names(connection, table, data, export_file, expected_csv):
    params = {"with_column_names": True}
    connection.export_to_file(export_file, table, export_params=params)

    expected = expected_csv(table, data, **params)
    actual = export_file.read_text()

    assert actual == expected


@pytest.mark.etl
def test_export_without_resolving_hostname(
    connection_without_resolving_hostnames, table, data, export_file, expected_csv
):
    params = {"with_column_names": True}
    connection_without_resolving_hostnames.export_to_file(
        export_file, table, export_params=params
    )

    expected = expected_csv(table, data, **params)
    actual = export_file.read_text()

    assert actual == expected


@pytest.mark.etl
def test_custom_export_callback(connection, table, data, export_file, expected_csv):
    def export_cb(pipe, dst):
        dst.write_bytes(pipe.read())

    connection.export_to_callback(export_cb, export_file, table)

    expected = expected_csv(table, data)
    actual = export_file.read_text()

    assert actual == expected


@pytest.mark.etl
def test_export_csv_cols(connection, table, data, export_file, expected_csv):
    params = {"csv_cols": ["1..2"]}
    connection.export_to_file(export_file, table, export_params=params)

    expected = expected_csv(table, data, **params)
    actual = export_file.read_text()

    assert actual == expected
