"""Minimum Exasol database versions required for supported features."""

from dataclasses import dataclass

from packaging.version import Version


@dataclass(frozen=True)
class DatabaseFeatureVersion:
    """Document a database feature's minimum supported Exasol version."""

    version: Version
    description: str
    documentation_url: str

    def is_supported_by(self, database_version: Version | None) -> bool:
        """Return whether the given database version supports this feature."""
        return database_version is not None and database_version >= self.version


MIN_VERSION_FOR_TLS_PUBLIC_KEY = DatabaseFeatureVersion(
    version=Version("8.32.0"),
    description=(
        "Exasol 8.32.0 introduced certificate verification for IMPORT and "
        "EXPORT file connections, requiring the SHA-256 fingerprint of the "
        "server certificate's public key when encryption is enabled."
    ),
    documentation_url="https://docs.exasol.com/db/latest/changelogs/21747.htm",
)

MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT = DatabaseFeatureVersion(
    version=Version("2026.1.0"),
    description=(
        "Exasol 2026.1.0 introduced full native Parquet import support, "
        "including imports from local files."
    ),
    documentation_url="https://docs.exasol.com/db/latest/release_notes_db/2026.1.0.htm",
)
