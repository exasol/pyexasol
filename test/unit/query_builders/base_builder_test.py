from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pyexasol.query_builders.base_builder import validate_build_query

if TYPE_CHECKING:
    from packaging.version import Version

    from pyexasol import ExaFormatter


class TestValidateBuildQuery:
    @staticmethod
    def test_accepts_compatible_builder_signature():
        @validate_build_query
        class CompatibleBuilder:
            def build_query(
                self,
                database_version: Version | None,
                encryption: bool,
                exa_address_list: list[str],
                formatter: ExaFormatter,
            ) -> str:
                return ""

        assert validate_build_query(CompatibleBuilder) is CompatibleBuilder

    @staticmethod
    def test_rejects_incompatible_builder_signature():
        with pytest.raises(TypeError, match="does not implement"):

            @validate_build_query
            class IncompatibleBuilder:
                def build_query(
                    self,
                    database_version: Version | None,
                    encryption: bool,
                    exa_address_list: list[str],
                    formatter: ExaFormatter,
                    extra_argument: str,
                ) -> str:
                    return extra_argument
