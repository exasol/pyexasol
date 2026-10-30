import pytest

from pyexasol.callback import check_import_from_file_source
from test.import_from_file_test_helpers import (
    ACCEPTED_SOURCE_FACTORIES,
    MISSING_PATH_FACTORIES,
    NO_PATH_NO_STREAM_FACTORIES,
    TEXT_SOURCE_FACTORIES,
)


class TestCheckImportFromFileSource:
    @staticmethod
    @pytest.mark.parametrize(
        "source_factory",
        ACCEPTED_SOURCE_FACTORIES,
    )
    def test_sources_are_accepted(source_factory, tmp_path):
        filepath = tmp_path / "data.csv"
        filepath.write_text("col a, col b")

        with source_factory(filepath) as src:
            check_import_from_file_source(src)

    @staticmethod
    @pytest.mark.parametrize(
        "source_factory",
        TEXT_SOURCE_FACTORIES,  # all of them?
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

    @staticmethod
    @pytest.mark.parametrize(
        "src",
        NO_PATH_NO_STREAM_FACTORIES,
    )
    def test_non_path_non_stream_sources_are_rejected(src):
        with pytest.raises(TypeError) as exception_info:
            check_import_from_file_source(src)

        assert str(exception_info.value) == (
            "Source must be either a file path or a binary stream "
            "(a file opened in binary mode)."
        )
