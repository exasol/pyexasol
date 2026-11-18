from unittest.mock import (
    MagicMock,
    patch,
)

import pytest
from pydantic import ValidationError

from pyexasol.exceptions import ExaImportError
from pyexasol.transport.parquet.importer import ExaParquetImporter


class TestExaParquetImporterValidation:
    @staticmethod
    def test_matches_no_files(mock_exaconnection_factory):
        connection = mock_exaconnection_factory()

        with pytest.raises(ValueError, match="does not match any files"):
            ExaParquetImporter(connection).import_from_native_parquet(
                "not_a_valid_path", "tbl"
            )

    @staticmethod
    def test_rejects_unknown_import_params(mock_exaconnection_factory, tmp_path):
        connection = mock_exaconnection_factory()
        (tmp_path / "a.parquet").write_bytes(b"PAR1")

        with pytest.raises(ValidationError):
            ExaParquetImporter(connection).import_from_native_parquet(
                tmp_path, "tbl", import_params={"columns": ["A"]}
            )


class TestNativeParquetLoop:
    @staticmethod
    @pytest.fixture
    def files(tmp_path):
        paths = [tmp_path / f"{i}.parquet" for i in range(3)]
        for path in paths:
            path.write_bytes(b"PAR1")
        return paths

    @staticmethod
    @pytest.fixture
    def threads(mock_exaconnection_factory):
        connection = mock_exaconnection_factory()
        connection.logger = MagicMock()
        connection.abort_query = MagicMock()
        connection.ws_ipaddr = "127.0.0.1"
        connection.ws_port = 8563
        with (
            patch(
                "pyexasol.transport.parquet.importer.ExaParquetHttpThread"
            ) as http_cls,
            patch("pyexasol.transport.parquet.importer.ExaSQLThread") as sql_cls,
        ):
            yield connection, http_cls, sql_cls

    @staticmethod
    def test_one_tunnel_per_file_in_one_statement(threads, files):
        connection, http_cls, sql_cls = threads
        http_threads = [MagicMock() for _ in files]
        http_cls.side_effect = http_threads

        ExaParquetImporter(connection).import_from_native_parquet(files, "tbl")

        assert [c.kwargs["parquet_file"] for c in http_cls.call_args_list] == files
        sql_cls.assert_called_once()
        sql_thread = sql_cls.return_value
        sql_thread.set_exa_address_list.assert_called_once_with(
            [t.exa_address for t in http_threads]
        )
        sql_thread.join_with_exc.assert_called_once()
        for http_thread in http_threads:
            http_thread.start.assert_called_once()
            http_thread.terminate.assert_called_once()

    @staticmethod
    def test_splits_files_into_batches(threads, files):
        connection, http_cls, sql_cls = threads

        ExaParquetImporter(connection).import_from_native_parquet(
            files, "tbl", max_parallel_files=2
        )

        assert http_cls.call_count == 3
        assert sql_cls.call_count == 2
        addresses = [
            len(c.args[0])
            for c in sql_cls.return_value.set_exa_address_list.call_args_list
        ]
        assert addresses == [2, 1]

    @staticmethod
    def test_generates_one_certificate_shared_by_all_tunnels(threads, files):
        connection, http_cls, _ = threads
        connection.options["encryption"] = True

        with patch(
            "pyexasol.transport.parquet.importer.ExaTunnelServer.generate_adhoc_ssl_context"
        ) as generate:
            ExaParquetImporter(connection).import_from_native_parquet(
                files, "tbl", max_parallel_files=2
            )

        generate.assert_called_once()
        assert http_cls.call_count == 3  # spread over two batches
        assert {c.kwargs["ssl_context"] for c in http_cls.call_args_list} == {
            generate.return_value
        }

    @staticmethod
    def test_generates_no_certificate_without_encryption(threads, files):
        connection, http_cls, _ = threads
        connection.options["encryption"] = False

        with patch(
            "pyexasol.transport.parquet.importer.ExaTunnelServer.generate_adhoc_ssl_context"
        ) as generate:
            ExaParquetImporter(connection).import_from_native_parquet(files, "tbl")

        generate.assert_not_called()
        assert {c.kwargs["ssl_context"] for c in http_cls.call_args_list} == {None}

    @staticmethod
    def test_rejects_invalid_max_parallel_files(threads, files):
        connection, _, _ = threads

        with pytest.raises(ValueError, match="max_parallel_files"):
            ExaParquetImporter(connection).import_from_native_parquet(
                files, "tbl", max_parallel_files=0
            )

    @staticmethod
    def test_shares_one_builder_with_import_params(threads, files):
        connection, _, sql_cls = threads

        ExaParquetImporter(connection).import_from_native_parquet(
            files, ("SCH", "tbl"), import_params={"MaxRows": 10}, max_parallel_files=1
        )

        builders = {c.kwargs["query_builder"] for c in sql_cls.call_args_list}
        assert len(builders) == 1
        builder = builders.pop()
        assert builder.table == ("SCH", "tbl")
        assert builder.max_rows == 10

    @staticmethod
    def test_stops_after_failed_batch_and_raises_import_error(threads, files):
        connection, http_cls, sql_cls = threads
        sql_cls.return_value.join_with_exc.side_effect = [None, RuntimeError("boom")]
        sql_cls.return_value.is_alive.return_value = False

        with pytest.raises(ExaImportError):
            ExaParquetImporter(connection).import_from_native_parquet(
                files, "tbl", max_parallel_files=1
            )

        assert http_cls.call_count == 2  # third file is not attempted
        connection.abort_query.assert_not_called()

    @staticmethod
    def test_aborts_query_if_sql_thread_still_running(threads, files):
        connection, http_cls, sql_cls = threads
        sql_cls.return_value.join_with_exc.side_effect = KeyboardInterrupt
        sql_cls.return_value.is_alive.return_value = True

        with pytest.raises(ExaImportError):
            ExaParquetImporter(connection).import_from_native_parquet(files, "tbl")

        connection.abort_query.assert_called_once()
        http_cls.return_value.terminate.assert_called()
        assert sql_cls.return_value.join.call_count == 2  # timed, then final
