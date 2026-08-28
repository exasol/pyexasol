from unittest.mock import Mock

import pytest

from pyexasol.query_builders.csv.clause_formatter import ClauseFormatter


@pytest.fixture
def clause_formatter():
    formatter = Mock()
    formatter.quote.side_effect = lambda value: f"'{value}'"
    formatter.default_format_ident.side_effect = lambda column: f'"{column}"'
    formatter.safe_decimal.side_effect = str
    return ClauseFormatter(formatter)


class TestClauseFormatter:
    @staticmethod
    @pytest.mark.parametrize(
        "csv_cols,expected",
        [(None, ""), ([], ""), (["1..3", "4"], "(1..3,4)")],
    )
    def test_csv_cols(clause_formatter, csv_cols, expected):
        assert clause_formatter._csv_cols(csv_cols) == expected

    @staticmethod
    def test_file_clauses(clause_formatter):
        transport_endpoint = Mock()
        transport_endpoint.build_endpoint_clause.side_effect = (
            lambda endpoint_address: f"AT '{endpoint_address}'"
        )

        result = clause_formatter.file_clauses(
            transport_endpoint=transport_endpoint,
            exa_address_list=["127.0.0.1:8563", "127.0.0.2:8563"],
            file_ext="gz",
            csv_cols=["1..3"],
        )

        assert result == [
            "AT '127.0.0.1:8563' FILE '000.gz'(1..3)",
            "AT '127.0.0.2:8563' FILE '001.gz'(1..3)",
        ]

    @staticmethod
    @pytest.mark.parametrize(
        "columns,expected",
        [
            (None, ""),
            ([], ""),
            (["LASTNAME", "FIRSTNAME"], '("LASTNAME","FIRSTNAME")'),
        ],
    )
    def test_column_specification(clause_formatter, columns, expected):
        assert clause_formatter._column_specification(columns) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "columns,expected",
        [
            (None, 'IMPORT INTO "TABLE" FROM CSV'),
            (
                ["LASTNAME", "FIRSTNAME"],
                'IMPORT INTO "TABLE"("LASTNAME","FIRSTNAME") FROM CSV',
            ),
        ],
    )
    def test_import_statement(clause_formatter, columns, expected):
        assert clause_formatter.import_statement("TABLE", columns) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "columns,expected",
        [
            (None, "EXPORT TABLE INTO CSV"),
            (
                ["LASTNAME", "FIRSTNAME"],
                'EXPORT TABLE("LASTNAME","FIRSTNAME") INTO CSV',
            ),
        ],
    )
    def test_export_statement(clause_formatter, columns, expected):
        assert clause_formatter.export_statement("TABLE", columns) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "column_delimiter,expected",
        [(";", "COLUMN DELIMITER = ';'"), (None, None)],
    )
    def test_column_delimiter(clause_formatter, column_delimiter, expected):
        assert clause_formatter.column_delimiter(column_delimiter) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "column_separator,expected",
        [("TAB", "COLUMN SEPARATOR = 'TAB'"), (None, None)],
    )
    def test_column_separator(clause_formatter, column_separator, expected):
        assert clause_formatter.column_separator(column_separator) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "encoding,expected",
        [("UTF-8", "ENCODING = 'UTF-8'"), (None, None)],
    )
    def test_encoding(clause_formatter, encoding, expected):
        assert clause_formatter.encoding(encoding) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "null,expected",
        [("NONE", "NULL = 'NONE'"), (None, None)],
    )
    def test_null(clause_formatter, null, expected):
        assert clause_formatter.null(null) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "row_separator,expected",
        [("LF", "ROW SEPARATOR = 'LF'"), (None, None)],
    )
    def test_row_separator(clause_formatter, row_separator, expected):
        assert clause_formatter.row_separator(row_separator) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "skip,expected",
        [("1", "SKIP = 1"), (1, "SKIP = 1"), (None, None)],
    )
    def test_skip(clause_formatter, skip, expected):
        assert clause_formatter.skip(skip) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "delimit,expected",
        [("AUTO", "DELIMIT = AUTO"), (None, None)],
    )
    def test_delimit(clause_formatter, delimit, expected):
        assert clause_formatter.delimit(delimit) == expected

    @staticmethod
    @pytest.mark.parametrize(
        "with_column_names,expected",
        [(True, "WITH COLUMN NAMES"), (False, None)],
    )
    def test_with_column_names(clause_formatter, with_column_names, expected):
        assert clause_formatter.with_column_names(with_column_names) == expected
