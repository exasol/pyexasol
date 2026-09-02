import pytest
from packaging.version import Version
from pydantic import ValidationError

from pyexasol.query_builders.parquet.builders import ImportBuilder


def test_build_query_default_works(formatter):
    result = ImportBuilder(table=("SCHEMA", "TABLE")).build_query(
        database_version=Version("2026.1.0"),
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
        ).build_query(
            database_version=Version("2026.1.0"),
            encryption=False,
            exa_address_list=["127.0.0.1:8563"],
            formatter=formatter,
        )

        assert ";MaxBatchFetchSize=100;" in query
        assert ";MaxRows=200'" in query
