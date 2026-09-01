from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyexasol import ExaFormatter

from enum import Enum
from typing import TYPE_CHECKING

from ..common_formattings import TransportEndpoint

if TYPE_CHECKING:
    from pyexasol import ExaFormatter


class ExportSourceType(str, Enum):
    TABLE = "table"
    QUERY = "query"

    @classmethod
    def from_query_or_table(
        cls, query_or_table: str | tuple[str, ...]
    ) -> ExportSourceType:
        """Classify an export source as a table identifier or SQL query.

        Tuples are table identifiers, including schema-qualified identifiers.
        Strings containing a space are treated as SQL queries for compatibility
        with the existing ``query_or_table`` API; other strings are table names.
        """
        if isinstance(query_or_table, tuple):
            return cls.TABLE
        if " " not in query_or_table.strip():
            return cls.TABLE
        return cls.QUERY


@dataclass(frozen=True)
class ClauseFormatter:
    formatter: ExaFormatter

    def _column_specification(self, columns: list[str] | None) -> str:
        if columns is None:
            return ""

        formatted_columns = [
            self.formatter.default_format_ident(column) for column in columns
        ]
        if formatted_columns:
            return f"({','.join(formatted_columns)})"
        return ""

    @staticmethod
    def _csv_cols(csv_cols: list[str] | None) -> str:
        if csv_cols is None:
            return ""

        formatted_csv_cols = ",".join(csv_cols)
        if formatted_csv_cols == "":
            return ""
        return f"({formatted_csv_cols})"

    def column_delimiter(self, column_delimiter: str | None) -> str | None:
        if column_delimiter is None:
            return None
        return f"COLUMN DELIMITER = {self.formatter.quote(column_delimiter)}"

    def column_separator(self, column_separator: str | None) -> str | None:
        if column_separator is None:
            return None
        return f"COLUMN SEPARATOR = {self.formatter.quote(column_separator)}"

    @staticmethod
    def delimit(delimit: str | None) -> str | None:
        """Format the EXPORT-only ``DELIMIT`` clause."""
        if delimit is None:
            return None
        return f"DELIMIT = {delimit}"

    def encoding(self, encoding: str | None) -> str | None:
        if encoding is None:
            return None
        return f"ENCODING = {self.formatter.quote(encoding)}"

    def export_statement(
        self,
        query_or_table: str | tuple[str, ...],
        source_type: ExportSourceType,
        columns: list[str] | None,
    ) -> str:
        if source_type is ExportSourceType.TABLE:
            export_source = self.formatter.default_format_ident(query_or_table)
            column_specification = self._column_specification(columns)
            return f"EXPORT {export_source}{column_specification} INTO CSV"

        if not isinstance(query_or_table, str):
            raise TypeError("A SQL query export source must be a string")
        # New lines are mandatory to handle queries with single-line comments '--'
        export_query = query_or_table.lstrip(" \n").rstrip(" \n;")
        export_source = f"(\n{export_query}\n)"
        return f"EXPORT {export_source} INTO CSV"

    def file_clauses(
        self,
        transport_endpoint: TransportEndpoint,
        exa_address_list: list[str],
        file_ext: str,
        csv_cols: list[str] | None,
    ) -> list[str]:
        """Build the transport endpoint and FILE clauses for a CSV query."""
        csv_cols_clause = self._csv_cols(csv_cols)

        file_clauses = []
        for index, exa_address in enumerate(exa_address_list):
            endpoint_clause = transport_endpoint.build_endpoint_clause(
                endpoint_address=exa_address
            )
            file_name = f"{str(index).rjust(3, '0')}.{file_ext}"
            file_clauses.append(
                f"{endpoint_clause} FILE '{file_name}'{csv_cols_clause}"
            )
        return file_clauses

    def import_statement(
        self, table: str | tuple[str, ...], columns: list[str] | None
    ) -> str:
        formatted_table = self.formatter.default_format_ident(table)
        column_specification = self._column_specification(columns)
        return f"IMPORT INTO {formatted_table}{column_specification} FROM CSV"

    def null(self, null: str | None) -> str | None:
        if null is None:
            return None
        return f"NULL = {self.formatter.quote(null)}"

    def row_separator(self, row_separator: str | None) -> str | None:
        if row_separator is None:
            return None
        return f"ROW SEPARATOR = {self.formatter.quote(row_separator)}"

    def skip(self, skip: str | int | None) -> str | None:
        """Format the IMPORT-only ``SKIP`` clause."""
        if skip is None:
            return None
        return f"SKIP = {self.formatter.safe_decimal(skip)}"

    @staticmethod
    def with_column_names(with_column_names: bool) -> str | None:
        """Format the EXPORT-only ``WITH COLUMN NAMES`` clause."""
        if not with_column_names:
            return None
        return "WITH COLUMN NAMES"
