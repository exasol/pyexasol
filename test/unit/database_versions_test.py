import pytest
from packaging.version import Version

from pyexasol.database_versions import (
    MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT,
    MIN_VERSION_FOR_TLS_PUBLIC_KEY,
)


@pytest.mark.parametrize(
    "feature_version",
    [
        MIN_VERSION_FOR_TLS_PUBLIC_KEY,
        MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT,
    ],
)
class TestDatabaseFeatureVersion:
    @staticmethod
    @pytest.mark.parametrize(
        "database_version, expected",
        [
            (None, False),
            (Version("7.1.19"), False),
        ],
    )
    def test_is_supported_by_below_minimum(feature_version, database_version, expected):
        assert feature_version.is_supported_by(database_version) is expected

    @staticmethod
    def test_is_supported_by_at_minimum(feature_version):
        assert feature_version.is_supported_by(feature_version.version)

    @staticmethod
    def test_is_supported_by_above_minimum(feature_version):
        assert feature_version.is_supported_by(
            Version(f"{feature_version.version.major + 1}.0.0")
        )
