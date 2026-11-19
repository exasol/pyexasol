import os
import threading
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pytest
from pyarrow import parquet as pq

from pyexasol import database_versions
from pyexasol.exceptions import ExaImportError
from test.integration.import_and_export.helper import select_result


def prepare_parquet_table(list_dict: list[dict]) -> pa.Table:
    table = pa.Table.from_pylist(list_dict)
    table = table.set_column(2, "REGISTER_DT", table["REGISTER_DT"].cast(pa.date32()))
    table = table.set_column(
        3, "LAST_VISIT_TS", table["LAST_VISIT_TS"].cast(pa.timestamp("ns"))
    )
    # this is not ideal as the database & Python data use bool
    return table.set_column(4, "IS_GRADUATING", table["IS_GRADUATING"].cast(pa.int64()))


@pytest.mark.parquet
class TestExportToParquet:
    @staticmethod
    def test_export_single_file(connection, fill_table, tmp_path, table_name, all_data):
        expected = prepare_parquet_table(all_data.list_dict)

        filepath = tmp_path / "single_parquet_dir"
        connection.export_to_parquet(dst=filepath, query_or_table=table_name)

        assert len(list(filepath.glob("*"))) == 1
        # can be a single file name or directory name
        assert pq.read_table(filepath) == expected

    @staticmethod
    def test_export_multiple_files(
        connection, fill_table, tmp_path, table_name, all_data, number_entries
    ):
        expected = prepare_parquet_table(all_data.list_dict)
        rows_per_file = 5

        filepath = tmp_path / "multiple_parquet_dir"
        connection.export_to_parquet(
            dst=filepath,
            query_or_table=table_name,
            # parquet requires the user to set max_rows_per_group <= max_rows_per_file
            callback_params={
                "max_rows_per_file": rows_per_file,
                "max_rows_per_group": rows_per_file,
            },
        )

        assert len(list(filepath.glob("*"))) == number_entries / rows_per_file
        assert pq.read_table(filepath) == expected

    @staticmethod
    def test_export_fails_as_not_a_directory(connection, tmp_path, table_name):
        dst = tmp_path / "dummy"
        dst.mkdir()
        filepath = dst / "file.parquet"
        filepath.touch()

        with mock.patch("pyexasol.connection.ExaConnection.export_to_callback") as m:
            with pytest.raises(ValueError, match="exists and is not a directory"):
                connection.export_to_parquet(dst=filepath, query_or_table=table_name)
        assert m.call_count == 0

    @staticmethod
    def test_export_fails_due_to_non_empty_directory(connection, tmp_path, table_name):
        dst = tmp_path / "dummy"
        dst.mkdir()
        filepath = dst / "file.parquet"
        filepath.touch()

        with mock.patch("pyexasol.connection.ExaConnection.export_to_callback") as m:
            with pytest.raises(ValueError, match="contains existing files"):
                connection.export_to_parquet(dst=dst, query_or_table=table_name)

        assert m.call_count == 0


@pytest.fixture
def skip_if_native_parquet_import(connection):
    # ``columns`` is a (callback_params / import_params) feature of the callback based
    # import; the native Parquet import does not offer it yet.
    if database_versions.MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT.is_supported_by(
        connection.exasol_db_version
    ):
        pytest.skip("Selecting columns is not supported by the native Parquet import")


@pytest.mark.parquet
class TestImportFromParquet:
    @staticmethod
    def _create_parquet_file(path: Path, data: list[dict]):
        table = pa.Table.from_pylist(data)
        # The native Parquet import (newer databases) does not convert between types,
        # so the DECIMAL(10,2) column SCORE needs to be a decimal in the file as well.
        index = table.schema.get_field_index("SCORE")
        table = table.set_column(
            index,
            "SCORE",
            pa.array(
                [
                    None if v is None else Decimal(str(v))
                    for v in table["SCORE"].to_pylist()
                ],
                type=pa.decimal128(10, 2),
            ),
        )
        pq.write_table(table, path)

    def test_load_single_file(
        self, tmp_path, empty_table, connection, table_name, all_data
    ):
        filepath = tmp_path / "single_file.parquet"
        self._create_parquet_file(filepath, all_data.list_dict)

        connection.import_from_parquet(filepath, table_name)

        assert select_result(connection) == all_data.list_tuple()

    def test_load_specific_columns(
        self,
        skip_if_native_parquet_import,
        tmp_path,
        empty_table,
        connection,
        table_name,
        all_data,
        reduced_import_data,
    ):
        filepath = tmp_path / "single_file.parquet"
        # add a nested column, to cover edge case from user which would cause errors
        # if the user were not deselecting this column
        for row in all_data.list_dict:
            row["nested_column"] = [[0]] * len(row["FIRST_NAME"])
        self._create_parquet_file(filepath, all_data.list_dict)

        reduced_columns = ["FIRST_NAME", "LAST_NAME", "AGE", "SCORE"]

        connection.import_from_parquet(
            filepath,
            table_name,
            import_params={"columns": reduced_columns},
            callback_params={"columns": reduced_columns},
        )

        assert select_result(connection) == reduced_import_data.list_tuple()

    @pytest.mark.parametrize(
        "source_function",
        [
            pytest.param(lambda x: x, id="from_directory_specified_as_Path"),
            pytest.param(
                lambda x: str(x) + "/*.parquet", id="from_string_specified_with_glob"
            ),
        ],
    )
    def test_load_from_multiple_files(
        self,
        tmp_path,
        empty_table,
        connection,
        table_name,
        all_data,
        source_function: Callable,
    ):
        self._create_parquet_file(tmp_path / "first_file.parquet", all_data.list_dict)
        self._create_parquet_file(
            tmp_path / "second_file.parquet", [all_data.list_dict[-1]]
        )

        connection.import_from_parquet(
            source=source_function(tmp_path), table=table_name
        )

        expected = all_data.list_tuple() + [all_data.list_tuple()[-1]]
        assert select_result(connection) == expected


@pytest.fixture
def require_native_parquet_import(connection):
    if not database_versions.MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT.is_supported_by(
        connection.exasol_db_version
    ):
        pytest.skip("Requires the native Parquet import")


@pytest.mark.parquet
@pytest.mark.usefixtures("require_native_parquet_import")
class TestNativeImportFromParquetMultipleFilesAndErrors:
    @staticmethod
    def _write(path: Path, data: list[dict], score_type=None):
        table = pa.Table.from_pylist(data)
        index = table.schema.get_field_index("SCORE")
        values = [
            None if v is None else Decimal(str(v)) for v in table["SCORE"].to_pylist()
        ]
        if score_type is None:
            table = table.set_column(
                index, "SCORE", pa.array(values, type=pa.decimal128(10, 2))
            )
        else:  # e.g. double, which the native import cannot convert to DECIMAL
            table = table.set_column(
                index,
                "SCORE",
                pa.array(
                    [None if v is None else float(v) for v in values], type=score_type
                ),
            )
        pq.write_table(table, path)

    @staticmethod
    def _row_count(connection, table_name):
        return connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchval()

    @staticmethod
    def _import_threads():
        return [
            t
            for t in threading.enumerate()
            if t is not threading.current_thread() and t.name != "MainThread"
        ]

    @pytest.fixture
    def five_files(self, tmp_path, all_data):
        paths = []
        for i in range(5):
            path = tmp_path / f"{i}.parquet"
            self._write(path, all_data.list_dict)
            paths.append(path)
        return paths

    @pytest.mark.parametrize("max_files_per_batch", [1, 2, 5, 10])
    def test_batches_load_all_files(
        self,
        five_files,
        empty_table,
        connection,
        table_name,
        all_data,
        max_files_per_batch,
    ):
        connection.import_from_parquet(
            five_files,
            table_name,
            callback_params={"max_files_per_batch": max_files_per_batch},
        )

        assert sorted(select_result(connection)) == sorted(all_data.list_tuple() * 5)

    def test_corrupt_file_in_batch_raises_and_cleans_up(
        self, five_files, empty_table, connection, table_name
    ):
        five_files[2].write_bytes(b"this is not a parquet file")
        threads_before = self._import_threads()

        with pytest.raises(ExaImportError):
            connection.import_from_parquet(
                five_files, table_name, callback_params={"max_files_per_batch": 5}
            )

        assert self._import_threads() == threads_before
        # the connection is still usable afterwards
        assert self._row_count(connection, table_name) == 0

    def test_failure_in_later_batch_keeps_earlier_batches(
        self, five_files, empty_table, connection, table_name, all_data
    ):
        five_files[3].write_bytes(b"this is not a parquet file")

        with pytest.raises(ExaImportError):
            connection.import_from_parquet(
                five_files, table_name, callback_params={"max_files_per_batch": 2}
            )

        # batches [0, 1] were imported, batch [2, 3] failed, batch [4] never ran
        assert self._row_count(connection, table_name) == 2 * len(all_data.list_dict)

    def test_type_mismatch_raises(
        self, tmp_path, empty_table, connection, table_name, all_data
    ):
        good = tmp_path / "a.parquet"
        bad = tmp_path / "b.parquet"
        self._write(good, all_data.list_dict)
        self._write(bad, all_data.list_dict, score_type=pa.float64())

        with pytest.raises(ExaImportError):
            connection.import_from_parquet([good, bad], table_name)

        assert self._row_count(connection, table_name) == 0

    def test_missing_table_raises_and_cleans_up(self, five_files, connection):
        threads_before = self._import_threads()

        with pytest.raises(ExaImportError):
            connection.import_from_parquet(five_files, "THIS_TABLE_DOES_NOT_EXIST")

        assert self._import_threads() == threads_before

    @pytest.mark.skipif(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        reason="root can read files without permissions",
    )
    def test_unreadable_file_raises(
        self, five_files, empty_table, connection, table_name
    ):
        # the file passes the listing check, but cannot be opened by the tunnel
        five_files[1].chmod(0o000)

        try:
            with pytest.raises(ExaImportError):
                connection.import_from_parquet(five_files, table_name)
        finally:
            five_files[1].chmod(0o644)

        assert self._row_count(connection, table_name) == 0

    def test_connection_usable_after_failure(
        self, five_files, empty_table, connection, table_name, all_data
    ):
        five_files[0].write_bytes(b"this is not a parquet file")
        with pytest.raises(ExaImportError):
            connection.import_from_parquet(five_files, table_name)

        connection.import_from_parquet(five_files[1:], table_name)

        assert self._row_count(connection, table_name) == 4 * len(all_data.list_dict)

    @pytest.mark.parametrize("value", [0, -1])
    def test_invalid_max_files_per_batch(
        self, five_files, empty_table, connection, table_name, value
    ):
        with pytest.raises(ValueError, match="max_files_per_batch"):
            connection.import_from_parquet(
                five_files, table_name, callback_params={"max_files_per_batch": value}
            )
