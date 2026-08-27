from __future__ import annotations

import re
from collections.abc import Iterable
from typing import (
    Annotated,
)

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    StrictBool,
    computed_field,
)

ALLOWED_DELIMIT = ("AUTO", "ALWAYS", "NEVER")
ALLOWED_FORMAT = ("bz2", "csv", "gz", "zip")
ALLOWED_TRIM = ("TRIM", "LTRIM", "RTRIM")
REGEX_CSV_COLS = re.compile(r"^(\d+|\d+\.\.\d+)(\sFORMAT='[^'\n]+')?$", re.IGNORECASE)


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


def validate_csv_cols(csv_cols: Iterable[str] | None) -> Iterable[str] | None:
    """Validate that CSV column specifications are safe for SQL embedding."""
    if csv_cols is None:
        return None

    validated_csv_cols = tuple(csv_cols)
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
CsvCols = Annotated[Iterable[str] | None, AfterValidator(validate_csv_cols)]
Delimit = Annotated[str | None, AfterValidator(validate_delimit)]
Format = Annotated[str | None, AfterValidator(validate_format)]
Trim = Annotated[str | None, AfterValidator(validate_trim)]


class ImportBuilder(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    compression: bool
    # set these values in the param dictionary to `ExaConnection`
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Iterable[str] | None = None
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


class ExportBuilder(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True, extra="forbid")

    compression: bool
    # set these values in the param dictionary to `ExaConnection`
    column_delimiter: str | None = None
    column_separator: str | None = None
    columns: Iterable[str] | None = None
    comment: Comment = None
    csv_cols: CsvCols = None
    delimit: Delimit = None
    encoding: str | None = None
    format: Format = None
    null: str | None = None
    row_separator: str | None = None
    with_column_names: StrictBool = False
