from test.import_from_file_test_helpers import (
    BINARY_SOURCE_FACTORIES,
    TEXT_SOURCE_FACTORIES,
)

import pytest

from pyexasol.callback import check_import_from_file_source


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
