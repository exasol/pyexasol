from inspect import cleandoc
from operator import itemgetter

import pandas as pd
import pytest
from pandas import testing as pd_testing

import pyexasol


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.pandas
def test_export_table_to_pandas(connection, table):
    table_name, values = table

    expected = pd.DataFrame.from_records(values)
    actual = connection.export_to_pandas(table_name)

    assert actual.equals(expected)


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.pandas
def test_export_sql_result_to_pandas(connection):

    query = "SELECT USER_NAME, USER_ID FROM USERS ORDER BY USER_ID ASC LIMIT 5;"

    expected = pd.DataFrame.from_records(
        data=[
            ("Jessica Mccoy", 0),
            ("Beth James", 1),
            ("Mrs. Teresa Ryan", 2),
            ("Tommy Henderson", 3),
            ("Jessica Christian", 4),
        ],
        columns=["USER_NAME", "USER_ID"],
    )
    actual = connection.export_to_pandas(query)

    assert actual.equals(expected)


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.pandas
def test_import_from_pandas(connection, empty_table, names):
    table_name = empty_table

    df = pd.DataFrame.from_records(names)
    connection.import_from_pandas(df, table_name)
    connection.commit()

    result = connection.execute(
        f"SELECT FIRST_NAME, LAST_NAME FROM {table_name} ORDER BY FIRST_NAME ASC;"
    )
    actual = result.fetchall()
    expected = sorted(
        [(first_name, last_name) for first_name, last_name in df.values],
        key=itemgetter(0),
    )

    assert actual == expected
