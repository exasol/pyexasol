from unittest.mock import MagicMock

import pytest


@pytest.fixture
def connection(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    connection.export_to_callback = MagicMock()
    return connection


class TestExportToParquetChecksDestinationFirst:
    @staticmethod
    def test_fails_if_destination_is_not_a_directory(connection, tmp_path):
        dst = tmp_path / "file.parquet"
        dst.touch()

        with pytest.raises(ValueError, match="exists and is not a directory"):
            connection.export_to_parquet(dst=dst, query_or_table="tbl")

        connection.export_to_callback.assert_not_called()

    @staticmethod
    def test_fails_if_destination_directory_is_not_empty(connection, tmp_path):
        (tmp_path / "file.parquet").touch()

        with pytest.raises(ValueError, match="contains existing files"):
            connection.export_to_parquet(dst=tmp_path, query_or_table="tbl")

        connection.export_to_callback.assert_not_called()

    @staticmethod
    def test_exports_if_destination_is_valid(connection, tmp_path):
        connection.export_to_parquet(dst=tmp_path / "new", query_or_table="tbl")

        connection.export_to_callback.assert_called_once()
