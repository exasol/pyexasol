import pytest
from pydantic import ValidationError

from pyexasol.query_builders.csv.builders import (
    ALLOWED_DELIMIT,
    ALLOWED_FORMAT,
    ALLOWED_TRIM,
    ExportBuilder,
    ImportBuilder,
)


@pytest.fixture(params=(ImportBuilder, ExportBuilder))
def csv_builder(request):
    return request.param


class TestCsvBuilderFormat:
    @staticmethod
    @pytest.mark.parametrize("file_format", ALLOWED_FORMAT + (None,))
    def test_accepts_supported_file_format(csv_builder, file_format):
        builder = csv_builder(compression=False, format=file_format)

        assert builder.format == file_format

    @staticmethod
    @pytest.mark.parametrize("file_format", ("test",))
    def test_rejects_unsupported_file_format(csv_builder, file_format):
        with pytest.raises(ValidationError, match=f"format' {file_format} not in"):
            csv_builder(compression=False, format=file_format)


class TestCsvBuilderCsvCols:
    @staticmethod
    @pytest.mark.parametrize("csv_cols", (["1"], ["1..3", "4 FORMAT='YYYY'"]))
    def test_accepts_csv_column_specifications(csv_builder, csv_cols):
        builder = csv_builder(compression=False, csv_cols=csv_cols)
        assert tuple(builder.csv_cols) == tuple(csv_cols)

    @staticmethod
    def test_rejects_all_unsafe_csv_column_specifications(csv_builder):
        with pytest.raises(
            ValidationError,
            match=r"'csv_cols' had unsafe parts: \[1\.2, 3\.4\]",
        ):
            csv_builder(compression=False, csv_cols=["1.2", "3.4"])


class TestExportBuilderDelimit:
    @staticmethod
    @pytest.mark.parametrize(
        "delimit,expected",
        [(value.lower(), value) for value in ALLOWED_DELIMIT]
        + [(value.title(), value) for value in ALLOWED_DELIMIT]
        + [(None, None)],
    )
    def test_accepts_and_normalizes_delimit(delimit, expected):
        builder = ExportBuilder(compression=False, delimit=delimit)

        assert builder.delimit == expected

    @staticmethod
    def test_rejects_unsupported_delimit():
        delimit = "invalid"
        with pytest.raises(ValidationError, match=f"'delimit' {delimit} not in"):
            ExportBuilder(compression=False, delimit=delimit)


class TestExportBuilderWithColumnNames:
    @staticmethod
    @pytest.mark.parametrize("value", [True, False])
    def test_accepts_boolean(value):
        builder = ExportBuilder(compression=False, with_column_names=value)

        assert builder.with_column_names is value

    @staticmethod
    @pytest.mark.parametrize("value", ["False", "true", "abc", 1, 0])
    def test_rejects_non_boolean(value):
        with pytest.raises(ValidationError, match="Input should be a valid boolean"):
            ExportBuilder(compression=False, with_column_names=value)


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


class TestCsvBuilderComment:
    @staticmethod
    @pytest.mark.parametrize(
        "comment,expected_comment",
        [(None, None), ("", "/**/"), ("valid comment", "/*valid comment*/")],
    )
    def test_accepts_and_formats_valid_comment(csv_builder, comment, expected_comment):
        builder = csv_builder(compression=False, comment=comment)

        assert builder.comment == expected_comment

    @staticmethod
    @pytest.mark.parametrize("comment", ("invalid /* comment", "invalid */ comment"))
    def test_rejects_comment_delimiters(csv_builder, comment):

        with pytest.raises(ValidationError, match=r"must not contain '/\*' or '\*/'"):
            csv_builder(compression=False, comment=comment)


class TestImportBuilderTrim:
    @staticmethod
    @pytest.mark.parametrize(
        "trim,expected_trim",
        [(trim.lower(), trim) for trim in ALLOWED_TRIM]
        + [(trim.title(), trim) for trim in ALLOWED_TRIM]
        + [(None, None)],
    )
    def test_accepts_and_normalizes_trim(trim, expected_trim):
        builder = ImportBuilder(compression=False, trim=trim)

        assert builder.trim == expected_trim

    @staticmethod
    def test_rejects_unsupported_trim():
        trim = "invalid"
        with pytest.raises(ValidationError, match=f"'trim' {trim} not in"):
            ImportBuilder(compression=False, trim=trim)
