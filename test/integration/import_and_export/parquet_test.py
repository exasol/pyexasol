import copy
import threading
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

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


def write_parquet_file(path: Path, data: list[dict], score_type=None) -> None:
    """
    Write ``data`` to a parquet file. The native Parquet import (newer databases)
    does not convert between types, so the DECIMAL(10,2) column SCORE is written as
    a decimal, unless another ``score_type`` (e.g. double) is requested.
    """
    table = pa.Table.from_pylist(data)
    index = table.schema.get_field_index("SCORE")
    values = [
        None if v is None else Decimal(str(v)) for v in table["SCORE"].to_pylist()
    ]
    if score_type is not None:
        values = [None if v is None else float(v) for v in values]  # type: ignore[misc]
    table = table.set_column(
        index,
        "SCORE",
        pa.array(values, type=score_type or pa.decimal128(10, 2)),
    )
    pq.write_table(table, path)


def is_native_parquet_import(connection) -> bool:
    return database_versions.MIN_VERSION_FOR_NATIVE_PARQUET_IMPORT.is_supported_by(
        connection.exasol_db_version
    )


@pytest.fixture
def require_native_parquet_import(connection):
    if not is_native_parquet_import(connection):
        pytest.skip("Requires the native Parquet import")


@pytest.fixture
def file_data_with_nested_column(all_data):
    # Add a nested column, to cover the edge case of a user whose file contains a
    # column that cannot be imported and must therefore be deselected or skipped.
    list_dict = copy.deepcopy(all_data.list_dict)
    for row in list_dict:
        row["nested_column"] = [[0]] * len(row["FIRST_NAME"])
    return list_dict


@pytest.fixture
def five_files(tmp_path, all_data):
    paths = []
    for i in range(5):
        path = tmp_path / f"{i}.parquet"
        write_parquet_file(path, all_data.list_dict)
        paths.append(path)
    return paths


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


@pytest.mark.parquet
class TestImportFromParquet:
    def test_load_single_file(
        self, tmp_path, empty_table, connection, table_name, all_data
    ):
        filepath = tmp_path / "single_file.parquet"
        write_parquet_file(filepath, all_data.list_dict)

        connection.import_from_parquet(filepath, table_name)

        assert select_result(connection) == all_data.list_tuple()

    @pytest.mark.parametrize(
        "max_files_per_batch",
        [
            pytest.param(2, id="smaller_than_number_of_files"),
            pytest.param(6, id="greater_than_number_of_files"),
        ],
    )
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

    def test_load_specific_columns_callback_import(
        self,
        tmp_path,
        empty_table,
        connection,
        table_name,
        file_data_with_nested_column,
        reduced_import_data,
    ):
        filepath = tmp_path / "single_file.parquet"
        write_parquet_file(filepath, file_data_with_nested_column)

        reduced_columns = ["FIRST_NAME", "LAST_NAME", "AGE", "SCORE"]

        # ``columns`` is a feature of the callback based import only
        connection.import_from_parquet(
            filepath,
            table_name,
            import_params={"columns": reduced_columns},
            callback_params={"columns": reduced_columns},
            native=False,
        )

        assert select_result(connection) == reduced_import_data.list_tuple()

    @pytest.mark.usefixtures("require_native_parquet_import")
    def test_skip_columns_native_import(
        self,
        tmp_path,
        empty_table,
        connection,
        table_name,
        all_data,
        file_data_with_nested_column,
    ):
        filepath = tmp_path / "single_file.parquet"
        write_parquet_file(filepath, file_data_with_nested_column)
        # the nested column is the last one in the file
        nested_index = len(file_data_with_nested_column[0]) - 1

        connection.import_from_parquet(
            filepath,
            table_name,
            import_params={"parquet_skip_cols": [str(nested_index)]},
        )

        assert select_result(connection) == all_data.list_tuple()

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
        write_parquet_file(tmp_path / "first_file.parquet", all_data.list_dict)
        write_parquet_file(tmp_path / "second_file.parquet", [all_data.list_dict[-1]])

        connection.import_from_parquet(
            source=source_function(tmp_path), table=table_name
        )

        expected = all_data.list_tuple() + [all_data.list_tuple()[-1]]
        assert select_result(connection) == expected


@pytest.mark.parquet
@pytest.mark.usefixtures("require_native_parquet_import")
class TestImportFromParquetErrors:
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

    @staticmethod
    def _make_rejected_by_database(path: Path, data: list[dict]):
        # A double cannot be converted to DECIMAL by the native import. The file
        # passes the up-front schema check, so the database is what rejects it.
        write_parquet_file(path, data, score_type=pa.float64())

    def test_failing_file_in_batch_raises_and_cleans_up(
        self, five_files, empty_table, connection, table_name, all_data
    ):
        self._make_rejected_by_database(five_files[2], all_data.list_dict)
        threads_before = self._import_threads()

        with pytest.raises(ExaImportError):
            connection.import_from_parquet(
                five_files, table_name, callback_params={"max_files_per_batch": 5}
            )

        assert self._import_threads() == threads_before
        # all files are in one statement, so none of them was imported
        assert self._row_count(connection, table_name) == 0

    def test_failure_in_later_batch_keeps_earlier_batches(
        self, five_files, empty_table, connection, table_name, all_data
    ):
        self._make_rejected_by_database(five_files[3], all_data.list_dict)

        with pytest.raises(ExaImportError):
            connection.import_from_parquet(
                five_files, table_name, callback_params={"max_files_per_batch": 2}
            )

        # batches [0, 1] were imported, batch [2, 3] failed, batch [4] never ran
        assert self._row_count(connection, table_name) == 2 * len(all_data.list_dict)
