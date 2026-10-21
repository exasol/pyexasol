from test.import_from_file_test_helpers import (
    BINARY_SOURCE_FACTORIES,
    MISSING_PATH_FACTORIES,
    TEXT_SOURCE_FACTORIES,
)
from test.integration.import_and_export.helper import select_result

import pytest

from pyexasol.callback import check_import_from_file_source


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


class TestCheckImportFromFileSource:
    @staticmethod
    @pytest.mark.parametrize(
        "source_factory",
        BINARY_SOURCE_FACTORIES,
    )
    def test_binary_sources_are_accepted(source_factory, tmp_path):
        filepath = tmp_path / "data.csv"
        filepath.write_text("col a, col b")

        with source_factory(filepath) as src:
            check_import_from_file_source(src)

    @staticmethod
    @pytest.mark.parametrize(
        "source_factory",
        TEXT_SOURCE_FACTORIES,
    )
    def test_text_sources_are_rejected(source_factory, tmp_path):
        filepath = tmp_path / "data.csv"
        filepath.write_text("col a, col b")

        with source_factory(filepath) as src:
            with pytest.raises(TypeError) as exception_info:
                check_import_from_file_source(src)
        assert str(exception_info.value) == (
            "Source must be either a file path or a binary stream "
            "(a file opened in binary mode)."
        )

    @staticmethod
    @pytest.mark.parametrize(
        "path_factory",
        MISSING_PATH_FACTORIES,
    )
    def test_missing_paths_are_rejected(path_factory, tmp_path):
        filepath = path_factory(tmp_path / "missing.csv")

        with pytest.raises(
            FileNotFoundError,
            match=r"Source file does not exist:",
        ):
            check_import_from_file_source(filepath)
