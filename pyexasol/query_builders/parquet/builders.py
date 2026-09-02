from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Literal,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)

from ..base_builder import validate_build_query
from ..common_formattings import (
    Comment,
    TransportEndpoint,
    join_query_lines,
)
from .clause_formatter import ClauseFormatter

if TYPE_CHECKING:
    from packaging.version import Version

    from pyexasol import ExaFormatter


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

    @property
    def connection_parameters(self) -> dict[str, int]:
        return self.model_dump(
            by_alias=True,
            exclude_none=True,
            include={
                "max_batch_fetch_size",
                "max_concurrent_reads",
                "max_connections",
                "max_rows",
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
