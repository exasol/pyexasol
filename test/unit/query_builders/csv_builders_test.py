import pytest
from pydantic import ValidationError

from pyexasol.query_builders.csv_builders import (
    ALLOWED_FORMAT,
    ImportBuilder,
)


class TestImportBuilderFormat:
    @staticmethod
    @pytest.mark.parametrize("file_format", ALLOWED_FORMAT + (None,))
    def test_accepts_supported_file_format(file_format):
        builder = ImportBuilder(compression=False, format=file_format)

        assert builder.format == file_format

    @staticmethod
    def test_rejects_unsupported_file_format():
        file_format = "test"
        with pytest.raises(ValidationError, match=f"format' {file_format} not in"):
            ImportBuilder(compression=False, format=file_format)


class TestImportBuilderFileExt:
    @staticmethod
    @pytest.mark.parametrize("file_format", ALLOWED_FORMAT)
    def test_keeps_explicit_file_format(file_format):
        builder = ImportBuilder(compression=False, format=file_format)
        assert builder.file_ext == file_format

    @staticmethod
    @pytest.mark.parametrize(
        "compression,expected_file_ext",
        [(True, "gz"), (False, "csv")],
    )
    def test_resolves_file_ext_from_compression(compression, expected_file_ext):
        builder = ImportBuilder(compression=compression, format=None)

        assert builder.file_ext == expected_file_ext


class TestImportBuilderComment:
    @staticmethod
    @pytest.mark.parametrize(
        "comment,expected_comment",
        [(None, None), ("", "/**/"), ("valid comment", "/*valid comment*/")],
    )
    def test_accepts_and_formats_valid_comment(comment, expected_comment):
        builder = ImportBuilder(compression=False, comment=comment)

        assert builder.comment == expected_comment

    @staticmethod
    @pytest.mark.parametrize("comment", ("invalid /* comment", "invalid */ comment"))
    def test_rejects_comment_delimiters(comment):

        with pytest.raises(ValidationError, match=r"must not contain '/\*' or '\*/'"):
            ImportBuilder(compression=False, comment=comment)
