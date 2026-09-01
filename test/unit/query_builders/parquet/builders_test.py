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
        "AT 'http://127.0.0.1:8563;MaxConcurrentReads=1' FILE '000.parquet'\n"
        "AT 'http://127.0.0.2:8563;MaxConcurrentReads=1' FILE '001.parquet'"
    )


class TestConnectionParameters:
    @staticmethod
    def test_default_works(formatter):
        import_builder = ImportBuilder(table=("SCHEMA", "TABLE"))
        assert import_builder.max_concurrent_reads == 1

    @staticmethod
    def test_rejects_other_max_concurrent_reads_values():
        with pytest.raises(
            ValidationError,
            match=r"max_concurrent_reads\s+Input should be less than or equal to 1",
        ):
            ImportBuilder(
                table=("SCHEMA", "TABLE"),
                max_concurrent_reads=2,
            )
