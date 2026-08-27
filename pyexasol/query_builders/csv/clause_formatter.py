from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyexasol import ExaFormatter


@dataclass(frozen=True)
class ClauseFormatter:
    formatter: ExaFormatter

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
