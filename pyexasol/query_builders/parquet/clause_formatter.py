from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..common_formattings import TransportEndpoint

if TYPE_CHECKING:
    from pyexasol import ExaFormatter


@dataclass(frozen=True)
class ClauseFormatter:
    formatter: ExaFormatter

    def import_statement(self, table: str | tuple[str, ...]) -> str:
        formatted_table = self.formatter.default_format_ident(table)
        return f"IMPORT INTO {formatted_table} FROM PARQUET"

    @staticmethod
    def file_clauses(
        transport_endpoint: TransportEndpoint,
        exa_address_list: list[str],
        connection_parameters: dict[str, int],
    ) -> list[str]:
        file_clauses = []
        for index, endpoint_address in enumerate(exa_address_list):
            endpoint_clause = transport_endpoint.build_endpoint_clause(
                endpoint_address=endpoint_address,
                connection_parameters=connection_parameters,
            )
            file_clauses.append(f"{endpoint_clause} FILE '{index:03d}.parquet'")
        return file_clauses
