import pytest
from packaging.version import Version

import pyexasol.database_versions as impl


@pytest.mark.parametrize(
    "feature_version",
    [
        impl.MIN_VERSION_FOR_TLS_PUBLIC_KEY,
        impl.MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT,
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


@pytest.mark.parametrize(
    "version, expected",
    [
        ("1.2.3", "1.2.3"),
        ("1.2.3.4", "1.2.3"),
        ("2025.1.13-p.2", "2025.1.13"),
        ("2025.1.13+p.2", "2025.1.13"),
        ("2025.1.13", "2025.1.13"),
    ],
)
def test_parse_success(version: str, expected: str) -> None:
    assert impl.parse(version) == Version(expected)


@pytest.mark.parametrize("version", ["1.2", "2025", ""])
def test_parse_failure(version: str) -> None:
    with pytest.raises(
        impl.ExasolVersionFormatError,
        match=f'Unsupported Exasol version "{version}".',
    ):
        impl.parse(version)
