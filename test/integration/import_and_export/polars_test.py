import polars as pl
import pytest


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.polars
def test_export_table_to_polars(connection, fill_table, data_dict):
    expected = pl.from_records(data_dict)
    actual = connection.export_to_polars(fill_table)

    assert actual.equals(expected)


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.polars
def test_export_sql_result_to_polars(connection, fill_table, all_data):

    query = (
        f"SELECT {', '.join(all_data.columns)} FROM {fill_table} "
        "ORDER BY FIRST_NAME ASC LIMIT 5;"
    )

    expected = pl.from_records(all_data.list_dict[:5], orient="row")
    actual = connection.export_to_polars(query)

    assert actual.equals(expected)


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.polars
def test_import_from_polars(connection, empty_table, data_dict, all_data):
    table_name = empty_table

    df = pl.from_records(data_dict)
    connection.import_from_polars(
        df,
        table_name,
        callback_params={"datetime_format": "%Y-%m-%d %H:%M:%S.%f"},
    )
    connection.commit()

    result = connection.execute(
        f"SELECT {', '.join(all_data.columns)} FROM {table_name} "
        "ORDER BY FIRST_NAME ASC;"
    )
    actual = result.fetchall()
    expected = all_data.list_tuple()

    assert actual == expected


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.polars
def test_import_from_polars_lazy(connection, empty_table, data_dict, all_data):
    table_name = empty_table

    lf = pl.from_records(data_dict).lazy()
    connection.import_from_polars(
        lf,
        table_name,
        callback_params={"datetime_format": "%Y-%m-%d %H:%M:%S.%f"},
    )
    connection.commit()

    result = connection.execute(
        f"SELECT {', '.join(all_data.columns)} FROM {table_name} "
        "ORDER BY FIRST_NAME ASC;"
    )
    actual = result.fetchall()
    expected = all_data.list_tuple()

    assert actual == expected
