from unittest.mock import Mock

import pytest

from pyexasol.query_builders.csv.clause_formatter import ClauseFormatter


@pytest.fixture
def clause_formatter():
    formatter = Mock()
    formatter.quote.side_effect = lambda value: f"'{value}'"
    return ClauseFormatter(formatter)


class TestClauseFormatter:
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
