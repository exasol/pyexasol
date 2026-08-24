from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import (
    dataclass,
    field,
)
from typing import TYPE_CHECKING

from packaging.version import Version

if TYPE_CHECKING:
    from pyexasol import ExaConnection


@dataclass
class SqlQuery:
    connection: ExaConnection
    compression: bool
    # set these values in param dictionary to ExaConnection
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Iterable[str] | None = None
    comment: str | None = None
    csv_cols: Iterable[str] | None = None
    encoding: str | None = None
    format: str | None = None
    null: str | None = None
    row_separator: str | None = None

    def _build_csv_cols(self) -> str:
        if self.csv_cols is not None:
            safe_csv_cols_regexp = re.compile(
                r"^(\d+|\d+\.\.\d+)(\sFORMAT='[^'\n]+')?$", re.IGNORECASE
            )
            for column in self.csv_cols:
                if not safe_csv_cols_regexp.match(column):
                    raise ValueError(f"Value [{column}] is not a safe csv_cols part")

            csv_cols = ",".join(self.csv_cols)
            if csv_cols != "":
                return f"({csv_cols})"

        return ""

    @staticmethod
    def _split_exa_address_into_components(exa_address: str) -> tuple[str, str | None]:
        """
        Split ip_address:port and public key from exa address, where the expected
        patterns are:
            ip_address:port
            ip_address:port/public_key
        The value for public key is expected to be a SHA-256 hash of the public key,
        which is then base64-encoded.
        """
        pattern = r"^([\d\.]+:\d+)(?:\/([a-zA-Z0-9_\-+\/]+=))?$"
        match = re.match(pattern, exa_address)
        if match is None:
            raise ValueError(
                f"Could not split exa_address {exa_address} into known components"
            )
        ip_address, public_key = match.groups()
        if not public_key:
            return ip_address, None
        return ip_address, public_key

    def _get_file_list(self, exa_address_list: list[str]) -> list[str]:
        file_ext = self._file_ext
        prefix = self._url_prefix

        csv_cols = self._build_csv_cols()
        files = []
        for index, exa_address in enumerate(exa_address_list):
            ip_address_port, public_key = self._split_exa_address_into_components(
                exa_address
            )
            statement = f"AT '{prefix}{ip_address_port}'"
            if self._requires_tls_public_key():
                if not public_key:
                    raise ValueError(
                        "Public key is required to be in the 'exa_address' for encrypted connections with Exasol DB >= 8.32.0"
                    )
                statement += f" PUBLIC KEY 'sha256//{public_key}'"
            statement += f" FILE '{str(index).rjust(3, '0')}.{file_ext}'{csv_cols}"
            files.append(statement)
        return files

    @staticmethod
    def _get_query_str(query_lines: list[str | None]) -> str:
        filtered_query_lines = [query for query in query_lines if query is not None]
        return "\n".join(filtered_query_lines)

    def _requires_tls_public_key(self) -> bool:
        version = self.connection.exasol_db_version
        return (
            version is not None
            and version >= Version("8.32.0")
            and self.connection.options["encryption"]
        )

    @property
    def _column_spec(self) -> str:
        """
        Return either empty string or comma-separated list of columns in parentheses,
        e.g. '("A", "B")'
        """
        if self.columns is not None:
            formatted = [
                self.connection.format.default_format_ident(column)
                for column in self.columns
            ]
            comma_sep = ",".join(formatted)
            if comma_sep != "":
                return f"({comma_sep})"
        return ""

    @property
    def _column_delimiter(self) -> str | None:
        if self.column_delimiter is None:
            return None
        return (
            f"COLUMN DELIMITER = {self.connection.format.quote(self.column_delimiter)}"
        )

    @property
    def _column_separator(self) -> str | None:
        if self.column_separator is None:
            return None
        return (
            f"COLUMN SEPARATOR = {self.connection.format.quote(self.column_separator)}"
        )

    @property
    def _comment(self) -> str | None:
        if self.comment is None:
            return None

        if "*/" in self.comment:
            raise ValueError(
                f'Invalid comment "{self.comment}". Comment must not contain "*/".'
            )
        return f"/*{self.comment}*/"

    @property
    def _encoding(self) -> str | None:
        if self.encoding is None:
            return None
        return f"ENCODING = {self.connection.format.quote(self.encoding)}"

    @property
    def _file_ext(self) -> str:
        if self.format is None:
            if self.compression:
                return "gz"
            return "csv"
        if self.format not in ("gz", "bz2", "zip"):
            raise ValueError(f"Unsupported compression format: {self.format}")
        return self.format

    @property
    def _null(self) -> str | None:
        if self.null is None:
            return None
        return f"NULL = {self.connection.format.quote(self.null)}"

    @property
    def _url_prefix(self) -> str:
        if self.connection.options["encryption"]:
            return "https://"
        return "http://"

    @property
    def _row_separator(self) -> str | None:
        if self.row_separator is None:
            return None
        return f"ROW SEPARATOR = {self.connection.format.quote(self.row_separator)}"


@dataclass
class ImportQuery(SqlQuery):
    # set these values in param dictionary to ExaConnection
    skip: str | int | None = None
    trim: str | None = None
    table: str = field(kw_only=True)

    def build_query(self, exa_address_list: list[str]) -> str:
        table = self.connection.format.default_format_ident(self.table)
        query_lines = [
            self._comment,
            self._get_import(table=table),
            *self._get_file_list(exa_address_list=exa_address_list),
            self._encoding,
            self._null,
            self._skip,
            self._trim,
            self._row_separator,
            self._column_separator,
            self._column_delimiter,
        ]
        return self._get_query_str(query_lines)

    @staticmethod
    def load_from_dict(
        connection: ExaConnection,
        compression: bool,
        params: dict,
        table: str,
    ) -> ImportQuery:
        """Load the params dictionary into the ImportQuery class."""
        return ImportQuery(
            connection=connection,
            compression=compression,
            table=table,
            **params,
        )

    def _get_import(self, table: str) -> str:
        return f"IMPORT INTO {table}{self._column_spec} FROM CSV"

    @property
    def _skip(self) -> str | None:
        if self.skip is None:
            return None
        return f"SKIP = {self.connection.format.safe_decimal(self.skip)}"

    @property
    def _trim(self) -> str | None:
        if self.trim is None:
            return None

        trim = str(self.trim).upper()
        if trim not in ("TRIM", "LTRIM", "RTRIM"):
            raise ValueError(f"Invalid value for import parameter TRIM: {trim}")
        return trim


@dataclass
class ExportQuery(SqlQuery):
    # set these values in param dictionary to ExaConnection
    delimit: str | None = None
    with_column_names: bool = False
    query_or_table: object = field(kw_only=True)

    def build_query(self, exa_address_list: list[str]) -> str:
        query_or_table = self._format_query_or_table()
        query_lines = [
            self._comment,
            self._get_export(query_or_table=query_or_table),
            *self._get_file_list(exa_address_list=exa_address_list),
            self._delimit,
            self._encoding,
            self._null,
            self._row_separator,
            self._column_separator,
            self._column_delimiter,
            self._with_column_names,
        ]
        return self._get_query_str(query_lines)

    @staticmethod
    def load_from_dict(
        connection: ExaConnection,
        compression: bool,
        params: dict,
        query_or_table: object,
    ) -> ExportQuery:
        """Load the params dictionary into the ExportQuery class."""
        return ExportQuery(
            connection=connection,
            compression=compression,
            query_or_table=query_or_table,
            **params,
        )

    def _format_query_or_table(self) -> str:
        if (
            isinstance(self.query_or_table, tuple)
            or str(self.query_or_table).strip().find(" ") == -1
        ):
            return self.connection.format.default_format_ident(self.query_or_table)

        if self.columns:
            raise ValueError(
                "Export option 'columns' is not compatible with SQL query export source"
            )
        # New lines are mandatory to handle queries with single-line comments '--'.
        export_query = str(self.query_or_table).lstrip(" \n").rstrip(" \n;")
        return f"(\n{export_query}\n)"

    def _get_export(self, query_or_table: str) -> str:
        return f"EXPORT {query_or_table}{self._column_spec} INTO CSV"

    @property
    def _delimit(self) -> str | None:
        if self.delimit is None:
            return None

        delimit = str(self.delimit).upper()
        if delimit not in ("AUTO", "ALWAYS", "NEVER"):
            raise ValueError(f"Invalid value for export parameter DELIMIT: {delimit}")
        return f"DELIMIT={delimit}"

    @property
    def _with_column_names(self) -> str | None:
        if not isinstance(self.with_column_names, bool):
            raise ValueError(
                "Invalid value for export parameter WITH_COLUMNS: "
                f"{self.with_column_names}. Only a boolean is allowed."
            )
        if self.with_column_names is False:
            return None
        return "WITH COLUMN NAMES"
