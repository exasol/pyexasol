import pytest
from pydantic import ValidationError

from pyexasol.query_builders.csv.builders import (
    Delimit,
    ExportBuilder,
    FileFormat,
    ImportBuilder,
    Trim,
)
from pyexasol.query_builders.csv.clause_formatter import ExportSourceType


@pytest.fixture(params=(ImportBuilder, ExportBuilder))
def csv_builder(request):
    if request.param is ImportBuilder:
        return lambda **kwargs: ImportBuilder(table="TABLE", **kwargs)
    return lambda **kwargs: ExportBuilder(query_or_table="TABLE", **kwargs)


class TestCsvBuilderFormat:
    @staticmethod
    @pytest.mark.parametrize("file_format", tuple(FileFormat) + (None,))
    def test_accepts_supported_file_format(csv_builder, file_format):
        builder = csv_builder(compression=False, format=file_format)
        expected_format = file_format.value if file_format is not None else None
        assert builder.format == expected_format

    @staticmethod
    @pytest.mark.parametrize("file_format", ["CSV", FileFormat.CSV])
    def test_accepts_case_insensitive_enum_values(csv_builder, file_format):
        builder = csv_builder(compression=False, format=file_format)
        assert builder.format == "csv"

    @staticmethod
    @pytest.mark.parametrize(
        "enum_value,expected",
        [
            (Delimit.AUTO, "AUTO"),
            (FileFormat.CSV, "csv"),
            (Trim.TRIM, "TRIM"),
        ],
    )
    def test_string_enums_behave_as_strings(enum_value, expected):
        assert enum_value == expected
        assert isinstance(enum_value, str)

    @staticmethod
    def test_rejects_unsupported_file_format(csv_builder):
        file_format = "test"
        with pytest.raises(
            ValidationError,
            match=r"Input should be 'bz2', 'csv', 'gz' or 'zip'",
        ):
            csv_builder(compression=False, format=file_format)


class TestCsvBuilderColumns:
    @staticmethod
    @pytest.mark.parametrize("columns", [["FIRST", "SECOND"], ("FIRST", "SECOND")])
    def test_stores_columns_as_reusable_list(csv_builder, columns):
        builder = csv_builder(compression=False, columns=columns)
        assert builder.columns == ["FIRST", "SECOND"]
        assert isinstance(builder.columns, list)


class TestImportBuilderTable:
    @staticmethod
    def test_accepts_schema_qualified_table():
        builder = ImportBuilder(compression=False, table=("SCHEMA", "TABLE"))
        assert builder.table == ("SCHEMA", "TABLE")


class TestCsvBuilderCsvCols:
    @staticmethod
    @pytest.mark.parametrize("csv_cols", (["1"], ["1..3", "4 FORMAT='YYYY'"]))
    def test_accepts_csv_column_specifications(csv_builder, csv_cols):
        builder = csv_builder(compression=False, csv_cols=csv_cols)
        assert tuple(builder.csv_cols) == tuple(csv_cols)
        assert isinstance(builder.csv_cols, list)

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
        [(value.lower(), str(value)) for value in Delimit]
        + [(value.title(), str(value)) for value in Delimit]
        + [(None, None)],
    )
    def test_accepts_and_normalizes_delimit(delimit, expected):
        builder = ExportBuilder(
            compression=False, query_or_table="TABLE", delimit=delimit
        )

        assert builder.delimit == expected

    @staticmethod
    def test_rejects_unsupported_delimit():
        delimit = "invalid"
        with pytest.raises(
            ValidationError,
            match=r"Input should be 'AUTO', 'ALWAYS' or 'NEVER'",
        ):
            ExportBuilder(compression=False, query_or_table="TABLE", delimit=delimit)


class TestExportBuilderWithColumnNames:
    @staticmethod
    @pytest.mark.parametrize("value", [True, False])
    def test_accepts_boolean(value):
        builder = ExportBuilder(
            compression=False, query_or_table="TABLE", with_column_names=value
        )

        assert builder.with_column_names is value

    @staticmethod
    @pytest.mark.parametrize("value", ["False", "true", "abc", 1, 0])
    def test_rejects_non_boolean(value):
        with pytest.raises(ValidationError, match="Input should be a valid boolean"):
            ExportBuilder(
                compression=False, query_or_table="TABLE", with_column_names=value
            )


class TestExportBuilderSourceType:
    @staticmethod
    @pytest.mark.parametrize(
        "query_or_table,expected",
        [
            ("TABLE", ExportSourceType.TABLE),
            (("SCHEMA", "TABLE"), ExportSourceType.TABLE),
            ("SELECT * FROM TABLE", ExportSourceType.QUERY),
        ],
    )
    def test_identifies_export_source_type(query_or_table, expected):
        builder = ExportBuilder(compression=False, query_or_table=query_or_table)

        assert builder.source_type is expected
        assert "source_type" not in ExportBuilder.model_fields

    @staticmethod
    def test_accepts_empty_columns_for_query_source():
        builder = ExportBuilder(
            compression=False,
            query_or_table="SELECT * FROM TABLE",
            columns=[],
        )

        assert builder.columns == []

    @staticmethod
    @pytest.mark.parametrize("columns", [["COLUMN"]])
    def test_rejects_columns_for_query_source(columns):
        with pytest.raises(
            ValidationError,
            match=(
                r"'query_or_table' was identified as a query, and 'columns' is not "
                r"compatible with a query export source"
            ),
        ):
            ExportBuilder(
                compression=False,
                query_or_table="SELECT * FROM TABLE",
                columns=columns,
            )


class TestImportBuilderFileExt:
    @staticmethod
    @pytest.mark.parametrize("file_format", tuple(FileFormat))
    def test_keeps_explicit_file_format(file_format):
        builder = ImportBuilder(table="TABLE", compression=False, format=file_format)
        assert builder.file_ext == file_format

    @staticmethod
    @pytest.mark.parametrize(
        "compression,expected_file_ext",
        [(True, "gz"), (False, "csv")],
    )
    def test_resolves_file_ext_from_compression(compression, expected_file_ext):
        builder = ImportBuilder(table="TABLE", compression=compression, format=None)
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
        [(trim.value.lower(), trim.value) for trim in Trim]
        + [(trim.value.title(), trim.value) for trim in Trim]
        + [(None, None)],
    )
    def test_accepts_and_normalizes_trim(trim, expected_trim):
        builder = ImportBuilder(compression=False, table="TABLE", trim=trim)

        assert builder.trim == expected_trim

    @staticmethod
    def test_rejects_unsupported_trim():
        trim = "invalid"
        with pytest.raises(
            ValidationError,
            match=r"Input should be 'TRIM', 'LTRIM' or 'RTRIM'",
        ):
            ImportBuilder(compression=False, table="TABLE", trim=trim)
