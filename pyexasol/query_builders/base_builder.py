from __future__ import annotations

from inspect import signature
from typing import (
    TYPE_CHECKING,
    Protocol,
    TypeVar,
    runtime_checkable,
)

if TYPE_CHECKING:
    from packaging.version import Version

    from pyexasol import ExaFormatter


@runtime_checkable
class QueryBuilder(Protocol):
    """Interface for query builders executable by a SQL worker thread."""

    def build_query(
        self,
        database_version: Version | None,
        encryption: bool,
        exa_address_list: list[str],
        formatter: ExaFormatter,
    ) -> str:
        """Build a SQL query for the configured transport endpoints."""
        ...


BuilderType = TypeVar("BuilderType")


def validate_build_query(builder_class: type[BuilderType]) -> type[BuilderType]:
    """Validate a builder's ``build_query`` signature at class definition time."""
    expected_signature = signature(QueryBuilder.build_query)
    try:
        actual_signature = signature(builder_class.build_query)  # type: ignore[attr-defined]
    except AttributeError as error:
        raise TypeError(
            f"{builder_class.__name__} must define build_query()"
        ) from error

    if actual_signature != expected_signature:
        raise TypeError(
            f"{builder_class.__name__}.build_query() does not implement the "
            "SqlQueryBuilder signature"
        )

    return builder_class
