from test.integration.import_and_export.helper import select_result

import pytest


@pytest.mark.etl
class TestImportParams:
    @staticmethod
    def test_with_no_params(connection, empty_table, table_name, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)

        connection.import_from_file(src=filepath, table=table_name)

        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_csv_cols(connection, empty_table, table_name, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)
        params = {"csv_cols": ["1..7"]}

        connection.import_from_file(
            src=filepath, table=table_name, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_skip(connection, empty_table, table_name, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)
        params = {"skip": 1}

        connection.import_from_file(
            src=filepath, table=table_name, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()[1:]

    @staticmethod
    def test_trim(connection, empty_table, table_name, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)
        params = {"trim": "TRIM"}

        connection.import_from_file(
            src=filepath, table=table_name, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()
