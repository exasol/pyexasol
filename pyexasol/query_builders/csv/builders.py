from __future__ import annotations

import re
from collections.abc import Iterable
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

from ..base_builder import validate_build_query
from ..common_formattings import (
    COLUMN_NUMBER_OR_RANGE,
    Comment,
    StringEnum,
    TransportEndpoint,
    join_query_lines,
)
from .clause_formatter import (
    ClauseFormatter,
    ExportSourceType,
)


class Delimit(StringEnum):
    AUTO = "AUTO"
    ALWAYS = "ALWAYS"
    NEVER = "NEVER"


class FileFormat(StringEnum):
    BZ2 = "bz2"
    CSV = "csv"
    GZ = "gz"
    ZIP = "zip"


class Trim(StringEnum):
    TRIM = "TRIM"
    LTRIM = "LTRIM"
    RTRIM = "RTRIM"


# Match a single column number (for example, ``1``) or a numeric range
# (``1..3``), optionally followed by a case-insensitive FORMAT clause, such as
# ``4 FORMAT='YYYY'`` or ``4 format='YYYY'``.
REGEX_CSV_COLS = re.compile(
    rf"^({COLUMN_NUMBER_OR_RANGE})(\sFORMAT='[^'\n]+')?$", re.IGNORECASE
)

if TYPE_CHECKING:
    from packaging.version import Version

    from pyexasol import ExaFormatter


def resolve_format(file_format: FileFormat | None, compression: bool) -> FileFormat:
    if file_format is not None:
        return file_format
    if compression:
        return FileFormat.GZ
    return FileFormat.CSV


def validate_csv_cols(csv_cols: Iterable[str] | None) -> list[str] | None:
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


def validate_columns(columns: Iterable[str] | None) -> list[str] | None:
    """Materialize columns so the validated value can be reused."""
    if columns is None:
        return None
    return list(columns)


CsvCols = Annotated[Iterable[str] | None, AfterValidator(validate_csv_cols)]
Columns = Annotated[Iterable[str] | None, AfterValidator(validate_columns)]


@validate_build_query
class ImportBuilder(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        use_enum_values=True,
    )

    compression: bool
    table: str | tuple[str, ...]
    # set these values in the param dictionary to `ExaConnection`
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Columns = None
    comment: Comment = None
    csv_cols: CsvCols = None
    encoding: str | None = None
    format: FileFormat | None = None
    null: str | None = None
    row_separator: str | None = None
    skip: str | int | None = None
    trim: Trim | None = None

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
            clause_formatter.import_statement(
                table=self.table,
                # AfterValidator output not inferred by mypy
                columns=self.columns,  # type: ignore[arg-type]
            ),
            *clause_formatter.file_clauses(
                transport_endpoint=transport_endpoint,
                exa_address_list=exa_address_list,
                file_ext=self.file_ext,
                # AfterValidator output not inferred by mypy
                csv_cols=self.csv_cols,  # type: ignore[arg-type]
            ),
            clause_formatter.encoding(self.encoding),
            clause_formatter.null(self.null),
            clause_formatter.skip(self.skip),
            self.trim,
            clause_formatter.row_separator(self.row_separator),
            clause_formatter.column_separator(self.column_separator),
            clause_formatter.column_delimiter(self.column_delimiter),
        ]
        return join_query_lines(*query_lines)


@validate_build_query
class ExportBuilder(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        use_enum_values=True,
    )

    compression: bool
    query_or_table: str | tuple[str, ...]
    # set these values in the param dictionary to `ExaConnection`
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Columns = None
    comment: Comment = None
    csv_cols: CsvCols = None
    delimit: Delimit | None = None
    encoding: str | None = None
    format: FileFormat | None = None
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
        if self.source_type is ExportSourceType.QUERY and self.columns:
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
                # AfterValidator output not inferred by mypy
                columns=self.columns,  # type: ignore[arg-type]
            ),
            *clause_formatter.file_clauses(
                transport_endpoint=transport_endpoint,
                exa_address_list=exa_address_list,
                file_ext=self.file_ext,
                # AfterValidator output not inferred by mypy
                csv_cols=self.csv_cols,  # type: ignore[arg-type]
            ),
            clause_formatter.delimit(self.delimit),
            clause_formatter.encoding(self.encoding),
            clause_formatter.null(self.null),
            clause_formatter.row_separator(self.row_separator),
            clause_formatter.column_separator(self.column_separator),
            clause_formatter.column_delimiter(self.column_delimiter),
            clause_formatter.with_column_names(self.with_column_names),
        ]
        return join_query_lines(*query_lines)
