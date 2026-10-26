import pytest

from test.integration.import_and_export.helper import select_result


@pytest.fixture
def connection(connection_factory):
    with connection_factory(compression=True) as con:
        yield con


@pytest.mark.etl
class TestExportToFile:
    @staticmethod
    def test_export_to_csv(connection, fill_table, tmp_path, table_name, all_data):
        actual_filepath = tmp_path / "actual.csv"
        connection.export_to_file(dst=actual_filepath, query_or_table=table_name)

        assert actual_filepath.read_text() == all_data.csv_str()


@pytest.mark.etl
class TestImportFromFile:
    @staticmethod
    def test_import_from_csv(connection, empty_table, table_name, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)

        connection.import_from_file(src=filepath, table=table_name)

        assert select_result(connection) == all_data.list_tuple()


@pytest.mark.etl
@pytest.mark.parametrize(
    "params",
    [
        pytest.param({}, id="default"),
        pytest.param({"format": "gz"}, id="gzip-compression"),
        pytest.param({"encoding": "WINDOWS-1251"}, id="windows-1251-encoding"),
        pytest.param(
            {"columns": ["REGISTER_DT", "FIRST_NAME", "SCORE", "LAST_NAME"]},
            id="selected-columns",
        ),
        pytest.param(
            {
                "csv_cols": [
                    "1",
                    "2",
                    "3 FORMAT='DD-MM-YYYY'",
                    "4..6",
                    "7 FORMAT='999.99999'",
                ]
            },
            id="custom-csv-columns",
        ),
    ],
)
def test_export_import_round_trip_to_and_from_file(
    connection, fill_table, empty_table, tmp_path, all_data, params
):
    export_file = tmp_path / "export"
    connection.export_to_file(export_file, fill_table, export_params=params)
    connection.import_from_file(export_file, empty_table, import_params=params)

    columns = params.get("columns", all_data.columns)
    query = (
        f"SELECT {', '.join(columns)} FROM {fill_table} "
        f"EXCEPT SELECT {', '.join(columns)} FROM {empty_table};"
    )
    result = connection.execute(query)

    actual = result.fetchall()

    assert actual == []
