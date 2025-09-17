import pytest
from integration.import_and_export.helper import select_result


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
    def test_import_from_csv(connection, table_name, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)

        connection.import_from_file(src=filepath, table=table_name)

        assert select_result(connection) == all_data.list_tuple()
