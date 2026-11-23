from pathlib import Path
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from pyexasol.exceptions import (
    ExaImportError,
    ExaNativeParquetImportError,
)
from pyexasol.transport.parquet.importer import (
    ExaParquetImporter,
    ensure_no_nested_columns,
    get_parquet_files,
)


class TestGetParquetFiles:
    @staticmethod
    def test_str_with_glob_works(tmp_path):
        tmp_file = tmp_path / "empty.parquet"
        tmp_file.touch()

        result = get_parquet_files(str(tmp_path / "*.parquet"))
        assert result == [tmp_file]

    @staticmethod
    def test_path_as_file_works(tmp_path):
        tmp_file = tmp_path / "empty.parquet"
        tmp_file.touch()

        result = get_parquet_files(tmp_file)
        assert result == [tmp_file]

    @staticmethod
    def test_path_as_directory_works(tmp_path):
        tmp_file = tmp_path / "empty.parquet"
        tmp_file.touch()

        result = get_parquet_files(tmp_path)
        assert result == [tmp_file]

    @staticmethod
    @pytest.mark.parametrize("source", ["not_a_valid_path", Path("empty_dir"), []])
    def test_matches_no_files(tmp_path, monkeypatch, source):
        (tmp_path / "empty_dir").mkdir()
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ValueError, match="does not match any files"):
            get_parquet_files(source)

    @staticmethod
    def test_list_path_not_valid_files():
        invalid_path = Path("not_a_valid_path_file")
        with pytest.raises(ValueError, match="contained entries which were not `Path`"):
            get_parquet_files([invalid_path])

    @staticmethod
    def test_not_a_valid_source_type():
        with pytest.raises(ValueError, match="is not a supported type"):
            get_parquet_files({"not_a_path"})


@pytest.mark.parquet
class TestEnsureNoNestedColumns:
    @staticmethod
    def test_hierarchical_data_is_not_supported(tmp_path):
        filepath = tmp_path / "hierarchical.parquet"
        pq.write_table(pa.Table.from_pydict({"list": [[1]]}), filepath)

        with pytest.raises(ValueError, match="is hierarchical which is not supported"):
            ensure_no_nested_columns([filepath], None)

    @staticmethod
    def test_nested_column_not_requested_passes(tmp_path):
        filepath = tmp_path / "file.parquet"
        pq.write_table(pa.Table.from_pydict({"flat": [1], "list": [[1]]}), filepath)

        ensure_no_nested_columns([filepath], ["flat"])

    @staticmethod
    def test_skipped_nested_column_passes(tmp_path):
        filepath = tmp_path / "file.parquet"
        pq.write_table(pa.Table.from_pydict({"flat": [1], "list": [[1]]}), filepath)

        ensure_no_nested_columns([filepath], skipped_column_indexes={1})

    @staticmethod
    def test_nested_column_not_skipped_fails(tmp_path):
        filepath = tmp_path / "file.parquet"
        pq.write_table(pa.Table.from_pydict({"flat": [1], "list": [[1]]}), filepath)

        with pytest.raises(ValueError, match="is hierarchical which is not supported"):
            ensure_no_nested_columns([filepath], skipped_column_indexes={0})

    @staticmethod
    def test_invalid_parquet_file_raises_error_naming_the_file(tmp_path):
        filepath = tmp_path / "corrupt.parquet"
        filepath.write_bytes(b"this is not a parquet file")

        with pytest.raises(ValueError, match="corrupt.parquet.*not a valid parquet"):
            ensure_no_nested_columns([filepath], None)


class TestExaParquetImporterValidation:
    @staticmethod
    @pytest.mark.parametrize("key", ["columns", "batch_size"])
    def test_rejects_callback_params_only_supported_by_callback_import(
        mock_exaconnection_factory, tmp_path, key
    ):
        connection = mock_exaconnection_factory()
        parquet_file = tmp_path / "a.parquet"
        parquet_file.write_bytes(b"PAR1")

        importer = ExaParquetImporter(connection)

        with pytest.raises(ValidationError, match=key):
            importer.import_from_native_parquet(
                [parquet_file], "tbl", callback_params={key: ["A"]}
            )

    @staticmethod
    def test_rejects_unknown_import_params(mock_exaconnection_factory, tmp_path):
        connection = mock_exaconnection_factory()
        parquet_file = tmp_path / "a.parquet"
        parquet_file.write_bytes(b"PAR1")

        importer = ExaParquetImporter(connection)

        with pytest.raises(ValidationError):
            importer.import_from_native_parquet(
                [parquet_file], "tbl", import_params={"columns": ["A"]}
            )


@pytest.fixture
def files(tmp_path):
    paths = [tmp_path / f"{i}.parquet" for i in range(3)]
    for path in paths:
        path.write_bytes(b"PAR1")
    return paths


@pytest.fixture
def threads(mock_exaconnection_factory):
    connection = mock_exaconnection_factory()
    connection.logger = MagicMock()
    connection.abort_query = MagicMock()
    connection.ws_ipaddr = "127.0.0.1"
    connection.ws_port = 8563
    connection.attr = {"autocommit": False}
    connection.set_autocommit = MagicMock()
    connection.commit = MagicMock()
    connection.rollback = MagicMock()
    # The parquet files in these tests are fake, so the nested column check is mocked.
    with (
        patch("pyexasol.transport.parquet.importer.ExaParquetHttpThread") as http_cls,
        patch("pyexasol.transport.parquet.importer.ExaSQLThread") as sql_cls,
        patch("pyexasol.transport.parquet.importer.ensure_no_nested_columns"),
    ):
        yield connection, http_cls, sql_cls


class TestExaParquetImporter:
    @staticmethod
    def test_one_tunnel_per_file_in_one_statement(threads, files):
        connection, http_cls, sql_cls = threads
        http_threads = [MagicMock() for _ in files]
        http_cls.side_effect = http_threads

        ExaParquetImporter(connection).import_from_native_parquet(
            files, "tbl", callback_params={"max_files_per_batch": len(files)}
        )

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
            files, "tbl", callback_params={"max_files_per_batch": 2}
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
                files, "tbl", callback_params={"max_files_per_batch": 2}
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
    @pytest.mark.parametrize("value", [0, -1, 2.5, "2", None, True, False])
    def test_rejects_invalid_max_files_per_batch(threads, files, value):
        connection, _, _ = threads

        importer = ExaParquetImporter(connection)

        with pytest.raises(ValidationError, match="max_files_per_batch"):
            importer.import_from_native_parquet(
                files, "tbl", callback_params={"max_files_per_batch": value}
            )

    @staticmethod
    def test_shares_one_builder_with_import_params(threads, files):
        connection, _, sql_cls = threads

        ExaParquetImporter(connection).import_from_native_parquet(
            files,
            ("SCH", "tbl"),
            import_params={"MaxRows": 10},
            callback_params={"max_files_per_batch": 1},
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

        importer = ExaParquetImporter(connection)

        with pytest.raises(ExaImportError):
            importer.import_from_native_parquet(
                files, "tbl", callback_params={"max_files_per_batch": 1}
            )

        assert http_cls.call_count == 2  # third file is not attempted
        connection.abort_query.assert_not_called()


class TestAutocommitHandling:
    @staticmethod
    def _events(connection):
        events = MagicMock()
        events.attach_mock(connection.set_autocommit, "set_autocommit")
        events.attach_mock(connection.commit, "commit")
        events.attach_mock(connection.rollback, "rollback")
        return events

    @staticmethod
    def test_autocommit_on_is_disabled_commits_once_and_restored(threads, files):
        connection, _, _ = threads
        connection.attr = {"autocommit": True}
        events = TestAutocommitHandling._events(connection)

        ExaParquetImporter(connection).import_from_native_parquet(
            files, "tbl", callback_params={"max_files_per_batch": 1}
        )

        assert events.mock_calls == [
            call.set_autocommit(False),
            call.commit(),
            call.set_autocommit(True),
        ]

    @staticmethod
    @pytest.mark.parametrize("max_files_per_batch", [3, 10])
    def test_single_batch_needs_no_transaction(threads, files, max_files_per_batch):
        connection, _, _ = threads
        connection.attr = {"autocommit": True}
        events = TestAutocommitHandling._events(connection)

        ExaParquetImporter(connection).import_from_native_parquet(
            files, "tbl", callback_params={"max_files_per_batch": max_files_per_batch}
        )

        assert events.mock_calls == []

    @staticmethod
    def test_single_file_needs_no_transaction(threads, files):
        connection, _, _ = threads
        connection.attr = {"autocommit": True}
        events = TestAutocommitHandling._events(connection)

        ExaParquetImporter(connection).import_from_native_parquet(files[:1], "tbl")

        assert events.mock_calls == []

    @staticmethod
    def test_autocommit_off_is_left_alone(threads, files):
        connection, _, _ = threads
        connection.attr = {"autocommit": False}
        events = TestAutocommitHandling._events(connection)

        ExaParquetImporter(connection).import_from_native_parquet(
            files, "tbl", callback_params={"max_files_per_batch": 1}
        )

        assert events.mock_calls == []

    @staticmethod
    def test_failure_rolls_back_and_restores_autocommit(threads, files):
        connection, http_cls, sql_cls = threads
        connection.attr = {"autocommit": True}
        sql_cls.return_value.join_with_exc.side_effect = [None, RuntimeError("boom")]
        sql_cls.return_value.is_alive.return_value = False
        events = TestAutocommitHandling._events(connection)

        importer = ExaParquetImporter(connection)

        with pytest.raises(ExaImportError):
            importer.import_from_native_parquet(
                files, "tbl", callback_params={"max_files_per_batch": 1}
            )

        assert events.mock_calls == [
            call.set_autocommit(False),
            call.rollback(),
            call.set_autocommit(True),
        ]

    @staticmethod
    def test_cleanup_errors_do_not_hide_import_error(threads, files):
        connection, _, sql_cls = threads
        connection.attr = {"autocommit": True}
        connection.rollback.side_effect = ConnectionError("rollback")
        connection.set_autocommit.side_effect = [None, ConnectionError("restore")]
        sql_cls.return_value.join_with_exc.side_effect = [None, RuntimeError("boom")]
        sql_cls.return_value.is_alive.return_value = False

        importer = ExaParquetImporter(connection)

        with pytest.raises(ExaImportError) as exc_info:
            importer.import_from_native_parquet(
                files, "tbl", callback_params={"max_files_per_batch": 1}
            )

        assert isinstance(exc_info.value.__cause__, RuntimeError)
        connection.rollback.assert_called_once()
        assert connection.set_autocommit.call_count == 2

    @staticmethod
    def test_restore_error_is_raised_if_import_succeeded(threads, files):
        connection, _, _ = threads
        connection.attr = {"autocommit": True}
        connection.set_autocommit.side_effect = [None, ConnectionError("restore")]

        importer = ExaParquetImporter(connection)

        with pytest.raises(ConnectionError, match="restore"):
            importer.import_from_native_parquet(
                files, "tbl", callback_params={"max_files_per_batch": 1}
            )

    @staticmethod
    def test_abort_query_error_does_not_hide_import_error(threads, files):
        connection, _, sql_cls = threads
        connection.abort_query.side_effect = ConnectionError("abort")
        sql_cls.return_value.join_with_exc.side_effect = RuntimeError("boom")
        sql_cls.return_value.is_alive.return_value = True

        importer = ExaParquetImporter(connection)

        with pytest.raises(ExaImportError) as exc_info:
            importer.import_from_native_parquet(
                files, "tbl", callback_params={"max_files_per_batch": 1}
            )

        assert isinstance(exc_info.value.__cause__, RuntimeError)
        assert sql_cls.return_value.join.call_count == 2

    @staticmethod
    def test_failure_with_autocommit_off_does_not_roll_back(threads, files):
        connection, _, sql_cls = threads
        connection.attr = {"autocommit": False}
        sql_cls.return_value.join_with_exc.side_effect = [None, RuntimeError("boom")]
        sql_cls.return_value.is_alive.return_value = False

        importer = ExaParquetImporter(connection)

        with pytest.raises(ExaImportError):
            importer.import_from_native_parquet(
                files, "tbl", callback_params={"max_files_per_batch": 1}
            )

        connection.rollback.assert_not_called()
        connection.commit.assert_not_called()
        connection.set_autocommit.assert_not_called()


class TestImportFromNativeParquetNestedColumns:
    @staticmethod
    def test_checks_nested_columns_with_skipped_indexes(threads, files):
        connection, _, sql_cls = threads

        with patch(
            "pyexasol.transport.parquet.importer.ensure_no_nested_columns"
        ) as ensure:
            ExaParquetImporter(connection).import_from_native_parquet(
                files, "tbl", import_params={"SkipCols": ["1", "3..4"]}
            )

        ensure.assert_called_once_with(
            files, skipped_column_indexes=frozenset({1, 3, 4})
        )

    @staticmethod
    def test_nested_columns_fail_before_import(threads, files):
        connection, http_cls, sql_cls = threads

        importer = ExaParquetImporter(connection)

        with patch(
            "pyexasol.transport.parquet.importer.ensure_no_nested_columns",
            side_effect=ValueError("hierarchical"),
        ):
            with pytest.raises(ValueError, match="hierarchical"):
                importer.import_from_native_parquet(files, "tbl")

        http_cls.assert_not_called()
        sql_cls.assert_not_called()


class TestImportNativeParquetBatch:
    @staticmethod
    def _import_batch(connection, files, ssl_context=None):
        ExaParquetImporter(connection)._import_native_parquet_batch(
            files, MagicMock(), ssl_context
        )

    @staticmethod
    def test_finishes_http_threads_after_statement(threads, files):
        connection, http_cls, sql_cls = threads
        http_threads = [MagicMock() for _ in files]
        http_cls.side_effect = http_threads

        TestImportNativeParquetBatch._import_batch(connection, files)

        sql_cls.return_value.join_with_exc.assert_called_once()
        for http_thread in http_threads:
            http_thread.terminate.assert_called_once()
            http_thread.join_with_exc.assert_called_once()
        connection.abort_query.assert_not_called()

    @staticmethod
    def test_aborts_query_if_sql_thread_still_running(threads, files):
        connection, http_cls, sql_cls = threads
        sql_cls.return_value.join_with_exc.side_effect = KeyboardInterrupt
        sql_cls.return_value.is_alive.return_value = True

        with pytest.raises(ExaImportError):
            TestImportNativeParquetBatch._import_batch(connection, files)

        connection.abort_query.assert_called_once()
        http_cls.return_value.terminate.assert_called()
        assert sql_cls.return_value.join.call_count == 2  # timed, then final

    @staticmethod
    def test_wraps_errors_of_all_threads_in_import_error(threads, files):
        connection, http_cls, sql_cls = threads
        http_thread = http_cls.return_value
        http_thread.exc = ConnectionError("http")
        sql_cls.return_value.exc = RuntimeError("sql")
        sql_cls.return_value.join_with_exc.side_effect = sql_cls.return_value.exc
        sql_cls.return_value.is_alive.return_value = False

        with pytest.raises(ExaImportError) as exc_info:
            TestImportNativeParquetBatch._import_batch(connection, files[:1])

        assert exc_info.value.__cause__ is sql_cls.return_value.exc
        assert http_thread.exc in exc_info.value.exceptions
        assert sql_cls.return_value.exc in exc_info.value.exceptions

    @staticmethod
    def test_error_names_files_of_failed_batch(threads, files):
        connection, http_cls, sql_cls = threads
        http_cls.return_value.exc = None
        sql_cls.return_value.exc = None
        sql_cls.return_value.join_with_exc.side_effect = RuntimeError("boom")
        sql_cls.return_value.is_alive.return_value = False

        with pytest.raises(ExaNativeParquetImportError) as exc_info:
            TestImportNativeParquetBatch._import_batch(connection, files[:2])

        assert isinstance(exc_info.value, ExaImportError)
        assert exc_info.value.files == [str(f) for f in files[:2]]
        assert all(str(f) in str(exc_info.value) for f in files[:2])

    @staticmethod
    def test_cleans_up_started_threads_if_starting_one_fails(threads, files):
        connection, http_cls, sql_cls = threads
        started = MagicMock()
        started.is_alive.return_value = False
        http_cls.side_effect = [started, OSError("cannot bind")]

        with pytest.raises(ExaImportError):
            TestImportNativeParquetBatch._import_batch(connection, files[:2])

        started.terminate.assert_called()
        sql_cls.assert_not_called()  # the statement is never started
        connection.abort_query.assert_not_called()

    @staticmethod
    def test_does_not_join_sql_thread_that_failed_to_start(threads, files):
        connection, _, sql_cls = threads
        sql_cls.return_value.start.side_effect = RuntimeError("cannot start thread")

        with pytest.raises(ExaImportError):
            TestImportNativeParquetBatch._import_batch(connection, files[:1])

        sql_cls.return_value.join.assert_not_called()
        connection.abort_query.assert_not_called()


class TestStartHttpThreads:
    @staticmethod
    def test_starts_one_thread_per_file_with_connection_settings(threads, files):
        connection, http_cls, _ = threads
        connection.options["encryption"] = True
        ssl_context = (MagicMock(), "sha")
        http_cls.side_effect = [MagicMock() for _ in files]
        http_threads = []

        ExaParquetImporter(connection)._start_http_threads(
            files, ssl_context, http_threads
        )

        assert len(http_threads) == len(files)
        assert [c.kwargs for c in http_cls.call_args_list] == [
            {
                "ipaddr": "127.0.0.1",
                "port": 8563,
                "parquet_file": file,
                "encryption": True,
                "ssl_context": ssl_context,
            }
            for file in files
        ]
        for http_thread in http_threads:
            http_thread.start.assert_called_once()

    @staticmethod
    def test_keeps_threads_started_before_a_failure(threads, files):
        connection, http_cls, _ = threads
        first = MagicMock()
        http_cls.side_effect = [first, OSError("cannot bind")]
        http_threads = []

        importer = ExaParquetImporter(connection)

        with pytest.raises(OSError, match="cannot bind"):
            importer._start_http_threads(files[:2], None, http_threads)

        assert http_threads == [first]


class TestStartSqlThread:
    @staticmethod
    def test_starts_sql_thread_with_builder_and_tunnel_addresses(threads):
        connection, _, sql_cls = threads
        import_builder = MagicMock()
        http_threads = [MagicMock(), MagicMock()]

        sql_thread = ExaParquetImporter(connection)._start_sql_thread(
            import_builder, http_threads
        )

        sql_cls.assert_called_once_with(
            connection=connection, compression=False, query_builder=import_builder
        )
        assert sql_thread is sql_cls.return_value
        sql_thread.set_exa_address_list.assert_called_once_with(
            [t.exa_address for t in http_threads]
        )
        sql_thread.start.assert_called_once()
        sql_thread.join_with_exc.assert_not_called()  # waiting is up to the caller


class TestAbortAfterFailure:
    @staticmethod
    @pytest.fixture
    def importer(threads):
        connection, _, _ = threads
        return ExaParquetImporter(connection)

    @staticmethod
    def test_joins_only_http_threads_that_are_still_alive(importer):
        alive, finished = MagicMock(), MagicMock()
        alive.is_alive.return_value = True
        finished.is_alive.return_value = False

        importer._abort_after_failure([alive, finished], None)

        alive.terminate.assert_called_once()
        finished.terminate.assert_called_once()
        alive.join.assert_called_once()
        finished.join.assert_not_called()

    @staticmethod
    def test_without_sql_thread_does_not_abort_query(importer):
        importer._abort_after_failure([], None)

        importer.connection.abort_query.assert_not_called()

    @staticmethod
    def test_does_not_abort_query_if_sql_thread_finished(importer):
        sql_thread = MagicMock()
        sql_thread.is_alive.return_value = False

        importer._abort_after_failure([], sql_thread)

        sql_thread.join.assert_called_once_with(1)
        importer.connection.abort_query.assert_not_called()

    @staticmethod
    def test_aborts_query_before_final_join_if_sql_thread_still_running(importer):
        sql_thread = MagicMock()
        sql_thread.is_alive.return_value = True
        calls = MagicMock()
        calls.attach_mock(sql_thread.join, "join")
        calls.attach_mock(importer.connection.abort_query, "abort_query")

        importer._abort_after_failure([], sql_thread)

        assert [c[0] for c in calls.mock_calls] == ["join", "abort_query", "join"]
