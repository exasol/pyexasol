import pytest
from integration.import_and_export.helper import select_result


@pytest.mark.etl
class TestImportParams:
    @staticmethod
    def test_with_no_params(connection, empty_table, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)

        connection.import_from_file(src=filepath, table=empty_table)

        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_csv_cols(connection, empty_table, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)
        params = {"csv_cols": ["1..7"]}

        connection.import_from_file(
            src=filepath, table=empty_table, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()

    @staticmethod
    def test_skip(connection, empty_table, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)
        offset = 2
        params = {"skip": offset}

        connection.import_from_file(
            src=filepath, table=empty_table, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()[offset:]

    @staticmethod
    def test_trim(connection, empty_table, tmp_path, all_data):
        filepath = all_data.write_csv(directory=tmp_path)
        params = {"trim": "TRIM"}

        connection.import_from_file(
            src=filepath, table=empty_table, import_params=params
        )

        assert select_result(connection) == all_data.list_tuple()


@pytest.mark.etl
class TestImportGeneral:
    @staticmethod
    def test_without_resolving_hostname(
        connection_without_resolving_hostnames, empty_table, tmp_path, all_data
    ):
        filepath = all_data.write_csv(directory=tmp_path)
        connection_without_resolving_hostnames.import_from_file(
            src=filepath, table=empty_table
        )

        assert (
            select_result(connection_without_resolving_hostnames)
            == all_data.list_tuple()
        )
