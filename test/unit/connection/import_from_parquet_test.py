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
    with (
        patch("pyexasol.connection.ExaParquetImporter") as importer_cls,
        patch("pyexasol.connection.ensure_no_nested_columns") as ensure,
    ):
        connection.native_import = importer_cls.return_value.import_from_native_parquet
        connection.ensure_no_nested_columns = ensure
        yield connection


@pytest.fixture
def parquet_files(tmp_path):
    paths = [tmp_path / f"{i}.parquet" for i in range(2)]
    for path in paths:
        path.write_bytes(b"PAR1")
    return paths


@pytest.fixture
def source(tmp_path, parquet_files):
    return str(tmp_path / "*.parquet")


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
    def test_uses_callback_for_older_or_unknown_version(
        connection, source, parquet_files, version
    ):
        _set_db_version(connection, version)

        result = connection.import_from_parquet(source, "tbl", {"a": 1}, {"b": 2})

        connection.import_from_callback.assert_called_once_with(
            cb.import_from_parquet_files, parquet_files, "tbl", {"a": 1}, {"b": 2}
        )
        connection.native_import.assert_not_called()
        assert result is connection.import_from_callback.return_value

    @staticmethod
    @pytest.mark.parametrize(
        "version", [MIN_VERSION, Version("2026.1.1"), Version("2027.0.0")]
    )
    def test_uses_native_for_supported_version(
        connection, source, parquet_files, version
    ):
        _set_db_version(connection, version)

        result = connection.import_from_parquet(source, "tbl", import_params={"b": 2})

        connection.native_import.assert_called_once_with(
            parquet_files, "tbl", {"b": 2}, 1
        )
        connection.import_from_callback.assert_not_called()
        assert result is connection.native_import.return_value

    @staticmethod
    def test_native_false_forces_callback_on_supported_version(
        connection, source, parquet_files
    ):
        _set_db_version(connection, MIN_VERSION)

        result = connection.import_from_parquet(
            source, "tbl", {"a": 1}, {"b": 2}, native=False
        )

        connection.import_from_callback.assert_called_once_with(
            cb.import_from_parquet_files, parquet_files, "tbl", {"a": 1}, {"b": 2}
        )
        connection.native_import.assert_not_called()
        assert result is connection.import_from_callback.return_value

    @staticmethod
    def test_native_rejects_callback_params(connection, source, parquet_files):
        _set_db_version(connection, MIN_VERSION)

        with pytest.raises(ValueError, match="callback_params are not supported"):
            connection.import_from_parquet(
                source, "tbl", callback_params={"batch_size": 1}
            )

        connection.native_import.assert_not_called()
        connection.import_from_callback.assert_not_called()

    @staticmethod
    @pytest.mark.parametrize("version", [None, MIN_VERSION])
    def test_source_matching_no_files_fails_before_any_import(
        connection, tmp_path, version
    ):
        _set_db_version(connection, version)

        with pytest.raises(ValueError, match="does not match any files"):
            connection.import_from_parquet(str(tmp_path / "*.nothing"), "tbl")

        connection.native_import.assert_not_called()
        connection.import_from_callback.assert_not_called()


class TestImportFromParquetNestedColumns:
    @staticmethod
    def test_nested_columns_fail_before_callback_import(
        connection, source, parquet_files
    ):
        _set_db_version(connection, None)
        connection.ensure_no_nested_columns.side_effect = ValueError("hierarchical")

        with pytest.raises(ValueError, match="hierarchical"):
            connection.import_from_parquet(source, "tbl")

        connection.ensure_no_nested_columns.assert_called_once_with(parquet_files, None)
        connection.import_from_callback.assert_not_called()

    @staticmethod
    def test_requested_columns_are_passed(connection, source, parquet_files):
        _set_db_version(connection, None)

        connection.import_from_parquet(
            source, "tbl", callback_params={"columns": ["a"]}
        )

        connection.ensure_no_nested_columns.assert_called_once_with(
            parquet_files, ["a"]
        )
