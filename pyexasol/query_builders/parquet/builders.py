from __future__ import annotations

from typing import TYPE_CHECKING

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
    model_config = ConfigDict(frozen=True, extra="forbid")

    table: str | tuple[str, ...]
    # set these values in the param dictionary to `ExaConnection`
    comment: Comment = None
    max_concurrent_reads: int = Field(default=1, ge=1, le=1)
    max_connections: int = Field(default=1, ge=1, le=1)

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
                connection_parameters={
                    "MaxConcurrentReads": self.max_concurrent_reads,
                    "MaxConnections": self.max_connections,
                },
            ),
        ]
        return join_query_lines(*query_lines)
