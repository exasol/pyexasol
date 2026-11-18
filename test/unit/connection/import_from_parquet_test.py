from unittest.mock import (
    MagicMock,
    patch,
)

import pytest
from packaging.version import Version

from pyexasol import callback as cb
from pyexasol.database_versions import MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT

MIN_VERSION = MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT.version


@pytest.fixture
def connection(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    connection.import_from_callback = MagicMock()
    with patch("pyexasol.connection.ExaParquetImporter") as importer_cls:
        connection.native_import = importer_cls.return_value.import_from_native_parquet
        yield connection


def _set_db_version(connection, version):
    release = str(version) if version is not None else None
    connection.login_info = {"releaseVersion": release} if release else {}


def test_docstring_mentions_native_min_version():
    # Docstrings cannot interpolate the constant, so guard against drifting.
    from pyexasol import ExaConnection

    assert str(MIN_VERSION) in ExaConnection.import_from_parquet.__doc__


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
        connection.native_import.assert_not_called()
        assert result is connection.import_from_callback.return_value

    @staticmethod
    @pytest.mark.parametrize(
        "version", [MIN_VERSION, Version("2026.1.1"), Version("2027.0.0")]
    )
    def test_uses_native_for_supported_version(connection, version):
        _set_db_version(connection, version)

        result = connection.import_from_parquet(
            "data/*.parquet", "tbl", import_params={"b": 2}
        )

        connection.native_import.assert_called_once_with(
            "data/*.parquet", "tbl", {"b": 2}, 10
        )
        connection.import_from_callback.assert_not_called()
        assert result is connection.native_import.return_value

    @staticmethod
    def test_native_false_forces_callback_on_supported_version(connection):
        _set_db_version(connection, MIN_VERSION)

        result = connection.import_from_parquet(
            "data/*.parquet", "tbl", {"a": 1}, {"b": 2}, native=False
        )

        connection.import_from_callback.assert_called_once_with(
            cb.import_from_parquet, "data/*.parquet", "tbl", {"a": 1}, {"b": 2}
        )
        connection.native_import.assert_not_called()
        assert result is connection.import_from_callback.return_value

    @staticmethod
    def test_native_rejects_callback_params(connection):
        _set_db_version(connection, MIN_VERSION)

        with pytest.raises(ValueError, match="callback_params are not supported"):
            connection.import_from_parquet(
                "data/*.parquet", "tbl", callback_params={"batch_size": 1}
            )

        connection.native_import.assert_not_called()
        connection.import_from_callback.assert_not_called()
