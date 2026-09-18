import io
from test.integration.import_and_export.helper import select_result

import pytest


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


def make_files(which_file: str, tmp_path, all_data):
    all_data_tuple = all_data.list_tuple()
    if which_file == "StringIO":
        return (
            io.StringIO("a,b\n"),
            "Source must be either a file path, or a binary stream , i.e. a file opened in binary mode",
        )
    elif which_file == "BytesIO":
        return (
            io.BytesIO(
                b"Anna,Thompson,1993-08-18,1993-08-18 12:00:00.900000,False,29,80.94"
            ),
            all_data_tuple[0:1],
        )
    filepath = all_data.write_csv(directory=tmp_path)
    if which_file == "openCsv":
        return (
            open(filepath),
            "Source must be either a file path, or a binary stream , i.e. a file opened in binary mode",
        )
    elif which_file == "openBinaryCsv":
        return open(filepath, "rb"), all_data_tuple
    elif which_file == "CsvPath":
        return filepath, all_data_tuple
    else:
        return None, ""


@pytest.mark.etl
@pytest.mark.parametrize(
    "which_file", ["StringIO", "BytesIO", "openCsv", "openBinaryCsv", "CsvPath"]
)
class TestImportFromFile:
    @staticmethod
    def test_import_from_non_binary_gives_correct_error(
        which_file, connection, empty_table, table_name, tmp_path, all_data
    ):
        scr, expected = make_files(
            which_file=which_file, tmp_path=tmp_path, all_data=all_data
        )

        if isinstance(expected, list):
            connection.import_from_file(src=scr, table=table_name)
            assert select_result(connection) == expected
        else:
            with pytest.raises(TypeError) as e:
                connection.import_from_file(src=scr, table=table_name)
                assert e.value == expected
