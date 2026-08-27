"""Build and validate CSV import and export SQL options.

See the Exasol documentation for the `IMPORT <https://docs.exasol.com/db/latest/sql/import.htm>`_
and `EXPORT <https://docs.exasol.com/db/latest/sql/export.htm>`_ statements.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Annotated,
)

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    computed_field,
)

if TYPE_CHECKING:
    from pyexasol import ExaFormatter

ALLOWED_FORMAT = ("bz2", "csv", "gz", "zip")
ALLOWED_TRIM = ("TRIM", "LTRIM", "RTRIM")


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


def validate_trim(trim: str | None) -> str | None:
    """Validate and normalize the import trim option."""
    if trim is None:
        return None

    normalized_trim = trim.upper()
    if normalized_trim not in ALLOWED_TRIM:
        raise ValueError(f"'trim' {trim} not in {ALLOWED_TRIM}")
    return normalized_trim


Format = Annotated[str | None, AfterValidator(validate_format)]
Comment = Annotated[str | None, AfterValidator(validate_comment)]
Trim = Annotated[str | None, AfterValidator(validate_trim)]


class ImportBuilder(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    compression: bool
    # set these values in the param dictionary to `ExaConnection`
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Iterable[str] | None = None
    comment: Comment = None
    csv_cols: Iterable[str] | None = None
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


class ExportBuilder(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    compression: bool
    # set these values in the param dictionary to `ExaConnection`
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Iterable[str] | None = None
    comment: Comment = None
    csv_cols: Iterable[str] | None = None
    delimit: str | None = None
    encoding: str | None = None
    format: Format = None
    null: str | None = None
    row_separator: str | None = None
    with_column_names: bool = False
