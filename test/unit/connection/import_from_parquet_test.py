from unittest.mock import MagicMock

import pytest
from packaging.version import Version

from pyexasol import callback as cb
from pyexasol.database_versions import MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT

MIN_VERSION = MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT.version


@pytest.fixture
def connection(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    connection.import_from_callback = MagicMock()
    connection._import_from_native_parquet = MagicMock()
    return connection


def _set_db_version(connection, version):
    release = str(version) if version is not None else None
    connection.login_info = {"releaseVersion": release} if release else {}


class TestImportFromParquet:
    @staticmethod
    @pytest.mark.parametrize(
        "version",
        [
            None,
            Version("8.32.0"),
            Version("2025.1.9"),
        ],
    )
    def test_uses_callback_for_older_or_unknown_version(connection, version):
        _set_db_version(connection, version)

        result = connection.import_from_parquet(
            "data/*.parquet", "tbl", {"a": 1}, {"b": 2}
        )

        connection.import_from_callback.assert_called_once_with(
            cb.import_from_parquet, "data/*.parquet", "tbl", {"a": 1}, {"b": 2}
        )
        connection._import_from_native_parquet.assert_not_called()
        assert result is connection.import_from_callback.return_value

    @staticmethod
    @pytest.mark.parametrize(
        "version", [MIN_VERSION, Version("2026.1.1"), Version("2027.0.0")]
    )
    def test_uses_native_for_supported_version(connection, version):
        _set_db_version(connection, version)

        result = connection.import_from_parquet(
            "data/*.parquet", "tbl", {"a": 1}, {"b": 2}
        )

        connection._import_from_native_parquet.assert_called_once_with(
            "data/*.parquet", "tbl", {"a": 1}, {"b": 2}
        )
        connection.import_from_callback.assert_not_called()
        assert result is connection._import_from_native_parquet.return_value


class TestImportFromNativeParquet:
    @staticmethod
    def test_matches_no_files(mock_exaconnection_factory):
        connection = mock_exaconnection_factory()

        with pytest.raises(ValueError, match="does not match any files"):
            connection._import_from_native_parquet("not_a_valid_path", "tbl")
