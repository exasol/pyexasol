import pytest

from pyexasol.query_builders.common_formattings import TransportEndpoint
from pyexasol.query_builders.parquet.clause_formatter import ClauseFormatter


@pytest.fixture
def clause_formatter(formatter):
    return ClauseFormatter(formatter)


@pytest.mark.parametrize(
    "table,expected",
    [
        ("TABLE", 'IMPORT INTO "TABLE" FROM PARQUET'),
        (("SCHEMA", "TABLE"), 'IMPORT INTO "SCHEMA"."TABLE" FROM PARQUET'),
    ],
)
def test_import_statement_formats_table(clause_formatter, table, expected):
    result = clause_formatter.import_statement(table)
    assert result == expected


def test_file_clauses_applies_connection_parameters():
    transport_endpoint = TransportEndpoint(database_version=None, encryption=False)

    result = ClauseFormatter.file_clauses(
        transport_endpoint,
        ["127.0.0.1:8563", "127.0.0.2:8563"],
        connection_parameters={"MaxConcurrentReads": 1},
    )

    assert result == [
        "AT 'http://127.0.0.1:8563;MaxConcurrentReads=1' FILE '000.parquet'",
        "AT 'http://127.0.0.2:8563;MaxConcurrentReads=1' FILE '001.parquet'",
    ]
