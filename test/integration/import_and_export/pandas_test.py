import pandas as pd
import pytest


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.pandas
def test_export_table_to_pandas(connection, fill_table, data_dict):
    expected = pd.DataFrame.from_records(data_dict).assign(
        LAST_VISIT_TS=lambda frame: frame["LAST_VISIT_TS"].dt.strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
        IS_GRADUATING=lambda frame: frame["IS_GRADUATING"].astype(int),
    )
    actual = connection.export_to_pandas(fill_table)

    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.pandas
def test_export_sql_result_to_pandas(connection, fill_table, all_data):
    query = (
        f"SELECT {', '.join(all_data.columns)} FROM {fill_table} "
        "ORDER BY FIRST_NAME ASC LIMIT 5;"
    )

    expected = pd.DataFrame.from_records(all_data.list_dict[:5]).assign(
        LAST_VISIT_TS=lambda frame: frame["LAST_VISIT_TS"].dt.strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
        IS_GRADUATING=lambda frame: frame["IS_GRADUATING"].astype(int),
    )
    actual = connection.export_to_pandas(query)

    assert actual.equals(expected)


@pytest.mark.parametrize(
    "connection", ["connection", "connection_with_compression"], indirect=True
)
@pytest.mark.pandas
def test_import_from_pandas(connection, empty_table, data_dict, all_data):
    table_name = empty_table

    df = pd.DataFrame.from_records(data_dict)
    connection.import_from_pandas(
        df,
        table_name,
        callback_params={"date_format": "%Y-%m-%d %H:%M:%S.%f"},
    )
    connection.commit()

    result = connection.execute(
        f"SELECT {', '.join(all_data.columns)} FROM {table_name} "
        "ORDER BY FIRST_NAME ASC;"
    )
    actual = result.fetchall()
    expected = all_data.list_tuple()

    assert actual == expected
