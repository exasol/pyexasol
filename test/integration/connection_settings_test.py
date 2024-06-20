import pytest
import pyexasol
from inspect import cleandoc


@pytest.fixture
def connection_with_quote_indent(dsn, user, password, schema):
    yield pyexasol.connect(
        dsn=dsn, user=user, password=password, schema=schema, quote_ident=True
    )


@pytest.fixture
def empty_camel_case_table(connection):
    table_name = "camelCaseTable"
    column_name = "camelCaseColun!"
    ddl = cleandoc(
        f"""
        CREATE OR REPLACE TABLE "{table_name}"
        (
            "{column_name}" DECIMAL(18,0)
        )
        """
    )
    connection.execute(ddl)
    connection.commit()

    yield table_name, column_name

    ddl = f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'
    connection.execute(ddl)
    connection.commit()


@pytest.fixture
def camel_case_table(connection, empty_camel_case_table):
    table_name, column_name = empty_camel_case_table
    ddl = cleandoc(
        f"""
        INSERT INTO "{table_name}" VALUES
        (1), (2), (3);
        """
    )
    connection.execute(ddl)
    connection.commit()

    yield table_name, column_name


@pytest.mark.configuration
def test_export_camel_case_table_without_quote_ident_fails(
    connection, camel_case_table
):
    with pytest.raises(pyexasol.ExaQueryError) as exec_info:
        connection.export_to_pandas("camelCaseTable")

    expected = "object CAMELCASETABLE not found"
    actual = exec_info.value.message
    assert expected in actual


@pytest.mark.configuration
def test_export_camel_case_table_with_quote_ident(
    connection_with_quote_indent, camel_case_table
):
    connection = connection_with_quote_indent
    df = connection.export_to_pandas("camelCaseTable")
    expected = {"camelCaseColun!": {0: 1, 1: 2, 2: 3}}
    actual = df.to_dict()
    assert actual == expected


@pytest.mark.configuration
def test_import_to_camel_case_table_without_quote_ident_fails(
    connection, empty_camel_case_table
):
    import pandas as pd

    table_name, column_name = empty_camel_case_table

    df = pd.DataFrame({column_name: [1, 2, 3]})
    with pytest.raises(pyexasol.ExaQueryError) as exec_info:
        connection.import_from_pandas(df, table_name)

    expected = "object CAMELCASETABLE not found"
    actual = exec_info.value.message
    assert expected in actual


@pytest.mark.configuration
def test_import_to_camel_case_table_with_quote_ident(
    connection_with_quote_indent, empty_camel_case_table
):
    import pandas as pd

    connection = connection_with_quote_indent
    table_name, column_name = empty_camel_case_table

    df = pd.DataFrame({column_name: [1, 2, 3]})
    connection.import_from_pandas(df, table_name)

    expected = df.to_dict()
    actual = connection.export_to_pandas("camelCaseTable").to_dict()

    assert actual == expected
