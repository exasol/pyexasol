from __future__ import annotations

import re
from collections.abc import Iterable
from typing import (
    TYPE_CHECKING,
    Annotated,
    Literal,
)

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
)

from ..base_builder import validate_build_query
from ..common_formattings import (
    COLUMN_NUMBER_OR_RANGE,
    Comment,
    TransportEndpoint,
    join_query_lines,
)
from .clause_formatter import ClauseFormatter

# Match a single column number (for example, ``1``) or a numeric range
# (``1..3``)
REGEX_PARQUET_SKIP_COL = re.compile(rf"^({COLUMN_NUMBER_OR_RANGE})$")

if TYPE_CHECKING:
    from packaging.version import Version

    from pyexasol import ExaFormatter


def validate_parquet_skip_cols(skip_cols: Iterable[str] | None) -> str | None:
    """Validate a Parquet ``SkipCols`` column specification."""
    if skip_cols is None:
        return None

    validated_skip_cols = list(skip_cols)
    invalid_skip_cols = [
        column_specification
        for column_specification in validated_skip_cols
        if not REGEX_PARQUET_SKIP_COL.match(column_specification)
    ]
    if invalid_skip_cols:
        raise ValueError(
            f"'parquet_skip_cols' had unsafe parts: [{', '.join(invalid_skip_cols)}]. "
            "Each value must be a column number or range "
            "(for example 1 or 3..8)."
        )
    return ",".join(validated_skip_cols)


ParquetSkipCols = Annotated[
    Iterable[str] | None, AfterValidator(validate_parquet_skip_cols)
]


@validate_build_query
class ImportBuilder(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
    )

    table: str | tuple[str, ...]
    # set these values in the param dictionary to `ExaConnection`
    comment: Comment = None
    max_batch_fetch_size: int | None = Field(
        default=None, gt=0, alias="MaxBatchFetchSize"
    )
    max_concurrent_reads: Literal[1] = Field(default=1, alias="MaxConcurrentReads")
    max_connections: Literal[1] = Field(default=1, alias="MaxConnections")
    max_rows: int | None = Field(default=None, gt=0, alias="MaxRows")
    parquet_skip_cols: ParquetSkipCols = Field(default=None, alias="SkipCols")

    @property
    def connection_parameters(self) -> dict[str, int | str]:
        return self.model_dump(
            by_alias=True,
            exclude_none=True,
            include={
                "max_batch_fetch_size",
                "max_concurrent_reads",
                "max_connections",
                "max_rows",
                "parquet_skip_cols",
            },
        )

    def build_query(
        self,
        database_version: Version | None,
        encryption: bool,
        exa_address_list: list[str],
        formatter: ExaFormatter,
    ) -> str:
        clause_formatter = ClauseFormatter(formatter)
        transport_endpoint = TransportEndpoint(
            database_version=database_version,
            encryption=encryption,
        )
        query_lines = [
            self.comment,
            clause_formatter.import_statement(self.table),
            *clause_formatter.file_clauses(
                transport_endpoint=transport_endpoint,
                exa_address_list=exa_address_list,
                connection_parameters=self.connection_parameters,
            ),
        ]
        return join_query_lines(*query_lines)
