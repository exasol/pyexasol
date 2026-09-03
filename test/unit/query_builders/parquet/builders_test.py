import pytest
from packaging.version import Version
from pydantic import ValidationError

from pyexasol.database_versions import MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT
from pyexasol.query_builders.parquet.builders import (
    ImportBuilder,
    validate_parquet_skip_cols,
)


@pytest.mark.parametrize(
    "database_version",
    [MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT.version, Version("2026.1.1")],
)
def test_build_query_works_for_supported_database_versions(formatter, database_version):
    result = ImportBuilder(table=("SCHEMA", "TABLE")).build_query(
        database_version=database_version,
        encryption=False,
        exa_address_list=["127.0.0.1:8563", "127.0.0.2:8563"],
        formatter=formatter,
    )
    assert result == (
        'IMPORT INTO "SCHEMA"."TABLE" FROM PARQUET\n'
        "AT 'http://127.0.0.1:8563;MaxConcurrentReads=1;MaxConnections=1' "
        "FILE '000.parquet'\n"
        "AT 'http://127.0.0.2:8563;MaxConcurrentReads=1;MaxConnections=1' "
        "FILE '001.parquet'"
    )


def test_build_query_rejects_unsupported_database_version(formatter):
    import_builder = ImportBuilder(table="TABLE")
    unsupported_database_version = Version("2025.2.0")

    with pytest.raises(
        ValueError,
        match=(
            r"Native Parquet import requires Exasol 2026\.1\.0 or newer, "
            r"but 2025\.2\.0 was provided\."
        ),
    ):
        import_builder.build_query(
            database_version=unsupported_database_version,
            encryption=False,
            exa_address_list=[],
            formatter=formatter,
        )


def test_build_query_with_comment(formatter):
    result = ImportBuilder(table="TABLE", comment="valid comment").build_query(
        database_version=Version("2026.1.0"),
        encryption=False,
        exa_address_list=["127.0.0.1:8563"],
        formatter=formatter,
    )

    assert result.startswith('/*valid comment*/\nIMPORT INTO "TABLE" FROM PARQUET')


class TestConnectionParameters:
    @staticmethod
    def test_default_works(formatter):
        import_builder = ImportBuilder(table=("SCHEMA", "TABLE"))
        assert import_builder.max_concurrent_reads == 1
        assert import_builder.max_connections == 1
        assert import_builder.max_batch_fetch_size is None
        assert import_builder.parquet_skip_cols is None

    @staticmethod
    @pytest.mark.parametrize("skip_cols", [["1"], ["1", "3..8", "11"]])
    def test_accepts_parquet_skip_cols(skip_cols):
        assert validate_parquet_skip_cols(skip_cols) == ",".join(skip_cols)

    @staticmethod
    def test_rejects_string_as_iterable_skip_cols():
        with pytest.raises(
            ValidationError,
            match=(
                r"parquet_skip_cols\s+Value error, must be an iterable, "
                r"not a single string\."
            ),
        ):
            ImportBuilder(table="TABLE", parquet_skip_cols="12")

    @staticmethod
    @pytest.mark.parametrize(
        "skip_cols",
        [["1,3..8,11"], [""], ["1,"], ["1...3"], ["1", "foo"], ["1 3"]],
    )
    def test_rejects_invalid_parquet_skip_cols(skip_cols):
        with pytest.raises(
            ValidationError,
            match=(
                r"parquet_skip_cols\s+Value error, 'parquet_skip_cols' "
                r"had unsafe parts"
            ),
        ):
            ImportBuilder(table="TABLE", parquet_skip_cols=skip_cols)

    @staticmethod
    @pytest.mark.parametrize(
        "parameter_name", ["max_concurrent_reads", "max_connections"]
    )
    def test_rejects_other_connection_parameter_values(parameter_name):
        with pytest.raises(
            ValidationError,
            match=rf"{parameter_name}\s+Input should be 1",
        ):
            ImportBuilder(table=("SCHEMA", "TABLE"), **{parameter_name: 2})

    @staticmethod
    @pytest.mark.parametrize("parameter_name", ["max_batch_fetch_size", "max_rows"])
    def test_rejects_non_positive_connection_parameter_values(parameter_name):
        with pytest.raises(
            ValidationError,
            match=rf"{parameter_name}\s+Input should be greater than 0",
        ):
            ImportBuilder(table=("SCHEMA", "TABLE"), **{parameter_name: 0})

    @staticmethod
    def test_includes_optional_connection_parameters_when_configured(formatter):
        query = ImportBuilder(
            table="TABLE",
            max_batch_fetch_size=100,
            max_rows=200,
            parquet_skip_cols=["1", "3..8", "11"],
        ).build_query(
            database_version=Version("2026.1.0"),
            encryption=False,
            exa_address_list=["127.0.0.1:8563"],
            formatter=formatter,
        )

        assert ";MaxBatchFetchSize=100;" in query
        assert ";MaxRows=200;" in query
        assert ";SkipCols=1,3..8,11'" in query
