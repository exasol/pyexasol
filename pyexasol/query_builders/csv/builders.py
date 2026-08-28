from __future__ import annotations

import re
from typing import (
    TYPE_CHECKING,
    Annotated,
)

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    StrictBool,
    computed_field,
    model_validator,
)

from ..common_formattings import TransportEndpoint
from .clause_formatter import (
    ClauseFormatter,
    ExportSourceType,
)

ALLOWED_DELIMIT = ("AUTO", "ALWAYS", "NEVER")
ALLOWED_FORMAT = ("bz2", "csv", "gz", "zip")
ALLOWED_TRIM = ("TRIM", "LTRIM", "RTRIM")
REGEX_CSV_COLS = re.compile(r"^(\d+|\d+\.\.\d+)(\sFORMAT='[^'\n]+')?$", re.IGNORECASE)

if TYPE_CHECKING:
    from packaging.version import Version

    from pyexasol import ExaFormatter


def validate_format(file_format: str | None) -> str | None:
    """Validate a CSV transport format"""
    if file_format is not None and file_format not in ALLOWED_FORMAT:
        raise ValueError(f"'format' {file_format} not in {ALLOWED_FORMAT}")
    return file_format


def resolve_format(file_format: str | None, compression: bool) -> str:
    if file_format is not None:
        return file_format
    if compression:
        return "gz"
    return "csv"


def validate_comment(comment: str | None) -> str | None:
    """Validate that a comment can be safely embedded in a SQL comment."""
    if comment is None:
        return comment
    if "/*" in comment or "*/" in comment:
        raise ValueError(f"'comment' {comment} must not contain '/*' or '*/'")
    return f"/*{comment}*/"


def validate_csv_cols(csv_cols: list[str] | None) -> list[str] | None:
    """Validate that CSV column specifications are safe for SQL embedding."""
    if csv_cols is None:
        return None

    validated_csv_cols = list(csv_cols)
    invalid_csv_cols = [
        column_specification
        for column_specification in validated_csv_cols
        if not REGEX_CSV_COLS.match(column_specification)
    ]
    if invalid_csv_cols:
        raise ValueError(
            f"'csv_cols' had unsafe parts: [{', '.join(invalid_csv_cols)}]. "
            "Each value must be a column number, a range (for example 1..3), "
            "optionally followed by FORMAT='...'."
        )

    return validated_csv_cols


def validate_columns(columns: list[str] | None) -> list[str] | None:
    """Materialize columns so the validated value can be reused."""
    if columns is None:
        return None
    return list(columns)


def validate_trim(trim: str | None) -> str | None:
    """Validate and normalize the import trim option."""
    if trim is None:
        return None

    normalized_trim = trim.upper()
    if normalized_trim not in ALLOWED_TRIM:
        raise ValueError(f"'trim' {trim} not in {ALLOWED_TRIM}")
    return normalized_trim


def validate_delimit(delimit: str | None) -> str | None:
    """Validate and normalize the export delimit option."""
    if delimit is None:
        return None

    normalized_delimit = delimit.upper()
    if normalized_delimit not in ALLOWED_DELIMIT:
        raise ValueError(f"'delimit' {delimit} not in {ALLOWED_DELIMIT}")
    return normalized_delimit


Comment = Annotated[str | None, AfterValidator(validate_comment)]
CsvCols = Annotated[list[str] | None, AfterValidator(validate_csv_cols)]
Columns = Annotated[list[str] | None, AfterValidator(validate_columns)]
Delimit = Annotated[str | None, AfterValidator(validate_delimit)]
Format = Annotated[str | None, AfterValidator(validate_format)]
Trim = Annotated[str | None, AfterValidator(validate_trim)]


def _join_query_lines(*query_lines: str | None) -> str:
    return "\n".join(filter(None, query_lines))


class ImportBuilder(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    compression: bool
    table: str
    # set these values in the param dictionary to `ExaConnection`
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Columns = None
    comment: Comment = None
    csv_cols: CsvCols = None
    encoding: str | None = None
    format: Format = None
    null: str | None = None
    row_separator: str | None = None
    skip: str | int | None = None
    trim: Trim = None

    @computed_field  # type: ignore[misc]
    @property
    def file_ext(self) -> str:
        return resolve_format(self.format, self.compression)

    def build_query(
        self,
        database_version: Version | None,
        encryption: bool,
        exa_address_list: list[str],
        formatter: ExaFormatter,
    ) -> str:
        """Build an IMPORT query using this builder's options."""
        clause_formatter = ClauseFormatter(formatter)
        transport_endpoint = TransportEndpoint(
            database_version=database_version,
            encryption=encryption,
        )
        query_lines = [
            self.comment,
            clause_formatter.import_statement(table=self.table, columns=self.columns),
            *clause_formatter.file_clauses(
                transport_endpoint=transport_endpoint,
                exa_address_list=exa_address_list,
                file_ext=self.file_ext,
                csv_cols=self.csv_cols,
            ),
            clause_formatter.encoding(self.encoding),
            clause_formatter.null(self.null),
            clause_formatter.skip(self.skip),
            self.trim,
            clause_formatter.row_separator(self.row_separator),
            clause_formatter.column_separator(self.column_separator),
            clause_formatter.column_delimiter(self.column_delimiter),
        ]
        return _join_query_lines(*query_lines)


class ExportBuilder(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    compression: bool
    query_or_table: str | tuple[str, ...]
    # set these values in the param dictionary to `ExaConnection`
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Columns = None
    comment: Comment = None
    csv_cols: CsvCols = None
    delimit: Delimit = None
    encoding: str | None = None
    format: Format = None
    null: str | None = None
    row_separator: str | None = None
    with_column_names: StrictBool = False

    @computed_field  # type: ignore[misc]
    @property
    def source_type(self) -> ExportSourceType:
        """Identify whether the export source is a table or a query."""
        return ExportSourceType.from_query_or_table(self.query_or_table)

    @model_validator(mode="after")
    def validate_query_columns(self) -> ExportBuilder:
        """Reject columns when the export source is a SQL query."""
        if self.source_type is ExportSourceType.QUERY and self.columns is not None:
            raise ValueError(
                "'query_or_table' was identified as a query, and 'columns' is not "
                "compatible with a query export source. 'columns' may only be None."
            )
        return self

    @computed_field  # type: ignore[misc]
    @property
    def file_ext(self) -> str:
        return resolve_format(self.format, self.compression)

    def build_query(
        self,
        database_version: Version | None,
        encryption: bool,
        exa_address_list: list[str],
        formatter: ExaFormatter,
    ) -> str:
        """Build an EXPORT query using this builder's options."""
        clause_formatter = ClauseFormatter(formatter)
        transport_endpoint = TransportEndpoint(
            database_version=database_version,
            encryption=encryption,
        )
        query_lines = [
            self.comment,
            clause_formatter.export_statement(
                query_or_table=self.query_or_table,
                source_type=self.source_type,
                columns=self.columns,
            ),
            *clause_formatter.file_clauses(
                transport_endpoint=transport_endpoint,
                exa_address_list=exa_address_list,
                file_ext=self.file_ext,
                csv_cols=self.csv_cols,
            ),
            clause_formatter.delimit(self.delimit),
            clause_formatter.encoding(self.encoding),
            clause_formatter.null(self.null),
            clause_formatter.row_separator(self.row_separator),
            clause_formatter.column_separator(self.column_separator),
            clause_formatter.column_delimiter(self.column_delimiter),
            clause_formatter.with_column_names(self.with_column_names),
        ]
        return _join_query_lines(*query_lines)
