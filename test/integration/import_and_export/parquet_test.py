import copy
import threading
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

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


STRUCT_TYPE = pa.struct([("x", pa.int64()), ("y", pa.int64())])
STRUCT_VALUE = {"x": 1, "y": 2}
MAP_TYPE = pa.map_(pa.string(), pa.int64())
MAP_VALUE = [("k", 1)]


class ExasolToolProcessCrash(Exception):
    """Exasol's import process crashed ("A tool process exited unexpectedly")."""


def write_columns_file(tmp_path: Path, columns: list[tuple]) -> Path:
    """Write a one-row file from ``(name, arrow type, value)`` columns."""
    filepath = tmp_path / "columns.parquet"
    schema = pa.schema([(name, type_) for name, type_, _ in columns])
    row = {name: value for name, _, value in columns}
    pq.write_table(pa.Table.from_pylist([row], schema=schema), filepath)
    return filepath


def create_table(connection, table_name: str, ddl_columns: str) -> None:
    connection.execute(f"CREATE OR REPLACE TABLE {table_name} ({ddl_columns})")


def import_skipping(connection, filepath, table_name, skip_cols: list[str]) -> None:
    """Import with ``SkipCols``, turning Exasol's process crash into its own error."""
    try:
        connection.import_from_parquet(
            filepath, table_name, import_params={"parquet_skip_cols": skip_cols}
        )
    except ExaImportError as e:
        if "tool process exited unexpectedly" in str(e):
            raise ExasolToolProcessCrash(str(e)) from e
        raise


MAP_CRASH_XFAIL = pytest.mark.xfail(
    raises=ExasolToolProcessCrash,
    strict=True,
    reason=(
        "Exasol 2026.1.0 crashes ('A tool process exited unexpectedly') when "
        "skipping a map that is not the last column"
    ),
)


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

    @pytest.mark.usefixtures("require_native_parquet_import")
    @pytest.mark.parametrize(
        "nested_type, nested_value",
        [
            pytest.param(pa.list_(pa.int64()), [1, 2], id="list_has_1_leaf"),
            pytest.param(
                pa.struct([("x", pa.int64()), ("y", pa.int64())]),
                {"x": 1, "y": 2},
                id="struct_has_2_leaves",
            ),
        ],
    )
    def test_skip_columns_counts_top_level_columns(
        self, tmp_path, connection, table_name, nested_type, nested_value
    ):
        """
        ``SkipCols`` indexes must be top-level columns, as assumed by
        ``skipped_column_indexes``, not Parquet leaf columns.

        A struct or a map is stored as several leaf columns, so with a nested column
        in the middle of the file, the two numberings differ for every column after it.
        Top-level numbering: ID=0, NESTED=1, NAME=2, UNUSED=3.
        Leaf numbering for a struct: ID=0, NESTED=1 and 2, NAME=3, UNUSED=4.
        With leaf numbering, ``1,3`` would skip only ``x`` and NAME, and the import
        would fail.

        A map (2 leaves) is not covered: Exasol 2026.1.0 crashes ("A tool process
        exited unexpectedly") if a map that is not the last column is skipped.
        """
        filepath = tmp_path / "nested_in_the_middle.parquet"
        schema = pa.schema(
            [
                ("ID", pa.int64()),
                ("NESTED", nested_type),
                ("NAME", pa.string()),
                ("UNUSED", pa.int64()),
            ]
        )
        pq.write_table(
            pa.Table.from_pylist(
                [{"ID": 1, "NESTED": nested_value, "NAME": "a", "UNUSED": 7}],
                schema=schema,
            ),
            filepath,
        )
        connection.execute(
            f"CREATE OR REPLACE TABLE {table_name} (ID DECIMAL(18,0), NAME VARCHAR(20))"
        )

        connection.import_from_parquet(
            filepath,
            table_name,
            import_params={"parquet_skip_cols": ["1", "3"]},
        )

        assert connection.execute(f"SELECT * FROM {table_name}").fetchall() == [
            (1, "a")
        ]

    @pytest.mark.usefixtures("require_native_parquet_import")
    @pytest.mark.parametrize(
        "nested_type, nested_value",
        [
            pytest.param(STRUCT_TYPE, STRUCT_VALUE, id="struct"),
            pytest.param(MAP_TYPE, MAP_VALUE, id="map"),
        ],
    )
    class TestSkipNestedColumnInTheMiddle:
        """
        File: ID=0, NESTED=1, NAME=2 (top-level numbering), table (ID, NAME).
        With leaf numbering, a struct or a map (2 leaves) would shift NAME to 3.
        """

        @staticmethod
        def prepare(tmp_path, connection, table_name, nested_type, nested_value):
            filepath = write_columns_file(
                tmp_path,
                [
                    ("ID", pa.int64(), 1),
                    ("NESTED", nested_type, nested_value),
                    ("NAME", pa.string(), "a"),
                ],
            )
            create_table(connection, table_name, "ID DECIMAL(18,0), NAME VARCHAR(20)")
            return filepath

        def test_skip_nested_column(
            self, request, tmp_path, connection, table_name, nested_type, nested_value
        ):
            if nested_type == MAP_TYPE:
                request.applymarker(MAP_CRASH_XFAIL)
            filepath = self.prepare(
                tmp_path, connection, table_name, nested_type, nested_value
            )

            import_skipping(connection, filepath, table_name, ["1"])

            assert connection.execute(f"SELECT * FROM {table_name}").fetchall() == [
                (1, "a")
            ]

        def test_range_counts_nested_column_as_one_column(
            self, tmp_path, connection, table_name, nested_type, nested_value
        ):
            # ``1..2`` skips NESTED and NAME, leaving only ID. With leaf numbering it
            # would use up the 2 leaves and import ID and NAME.
            filepath = self.prepare(
                tmp_path, connection, table_name, nested_type, nested_value
            )

            with pytest.raises(ExaImportError, match=r"ETL-6009.*source \(=1\)"):
                import_skipping(connection, filepath, table_name, ["1..2"])

        def test_not_skipping_the_nested_column_is_rejected_by_pyexasol(
            self, tmp_path, connection, table_name, nested_type, nested_value
        ):
            # 2 is NAME, so the nested column is still imported.
            filepath = self.prepare(
                tmp_path, connection, table_name, nested_type, nested_value
            )

            with pytest.raises(ValueError, match="hierarchical"):
                import_skipping(connection, filepath, table_name, ["2"])

    @pytest.mark.usefixtures("require_native_parquet_import")
    class TestSkipMapColumn:
        """
        File: ID=0, MAP=1, NAME=2, X=3 (top-level numbering), table (ID, NAME, X).
        None of the ways to skip a map that is not the last column works on
        Exasol 2026.1.0, and none treats the map as two columns.
        """

        @staticmethod
        def prepare(tmp_path, connection, table_name):
            filepath = write_columns_file(
                tmp_path,
                [
                    ("ID", pa.int64(), 1),
                    ("MAP", MAP_TYPE, MAP_VALUE),
                    ("NAME", pa.string(), "a"),
                    ("X", pa.int64(), 7),
                ],
            )
            create_table(
                connection,
                table_name,
                "ID DECIMAL(18,0), NAME VARCHAR(20), X DECIMAL(18,0)",
            )
            return filepath

        @pytest.mark.parametrize(
            "skip_cols",
            [
                pytest.param(["1"], id="1", marks=MAP_CRASH_XFAIL),
                # Out of range, so it behaves like ["1"].
                pytest.param(
                    ["1", "4"], id="1_and_out_of_range_4", marks=MAP_CRASH_XFAIL
                ),
            ],
        )
        def test_skip_only_the_map(self, tmp_path, connection, table_name, skip_cols):
            filepath = self.prepare(tmp_path, connection, table_name)

            import_skipping(connection, filepath, table_name, skip_cols)

            assert connection.execute(f"SELECT * FROM {table_name}").fetchall() == [
                (1, "a", 7)
            ]

        @pytest.mark.parametrize(
            "skip_cols, skipped",
            [
                pytest.param(["1", "2"], "MAP and NAME", id="1_2"),
                pytest.param(["1..2"], "MAP and NAME", id="1..2"),
                pytest.param(["1", "3"], "MAP and X", id="1_3"),
                pytest.param(["1..2", "4"], "MAP and NAME", id="1..2_and_4"),
            ],
        )
        def test_second_index_skips_another_column_not_a_second_map_part(
            self, tmp_path, connection, table_name, skip_cols, skipped
        ):
            # If a map counted as 2 columns, "1,2" would skip only the map and the
            # import would succeed. Instead, 2 columns are left (``skipped`` are gone).
            filepath = self.prepare(tmp_path, connection, table_name)

            # Exasol may append a "tool process exited unexpectedly" crash to the
            # ETL-6009 message (flaky), so call the import directly and only
            # check for the column-count error.
            with pytest.raises(ExaImportError, match=r"ETL-6009.*source \(=2\)"):
                connection.import_from_parquet(
                    filepath,
                    table_name,
                    import_params={"parquet_skip_cols": skip_cols},
                )

        def test_exasol_rejects_an_imported_map(self, tmp_path, connection, table_name):
            # Skipping NAME instead of the map. pyexasol's own check is bypassed,
            # to see what Exasol does.
            filepath = self.prepare(tmp_path, connection, table_name)

            with (
                patch("pyexasol.transport.parquet.importer.ensure_no_nested_columns"),
                pytest.raises(ExaImportError, match=r"ETL-2218.*unsupported"),
            ):
                import_skipping(connection, filepath, table_name, ["2"])

        def test_skip_map_in_the_last_position(self, tmp_path, connection, table_name):
            filepath = write_columns_file(
                tmp_path,
                [
                    ("ID", pa.int64(), 1),
                    ("NAME", pa.string(), "a"),
                    ("MAP", MAP_TYPE, MAP_VALUE),
                ],
            )
            create_table(connection, table_name, "ID DECIMAL(18,0), NAME VARCHAR(20)")

            import_skipping(connection, filepath, table_name, ["2"])

            assert connection.execute(f"SELECT * FROM {table_name}").fetchall() == [
                (1, "a")
            ]

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

    def test_failure_in_later_batch_rolls_back_earlier_batches(
        self, five_files, empty_table, connection, table_name, all_data
    ):
        self._make_rejected_by_database(five_files[3], all_data.list_dict)

        with pytest.raises(ExaImportError):
            connection.import_from_parquet(
                five_files, table_name, callback_params={"max_files_per_batch": 2}
            )

        # batches [0, 1] were imported, but batch [2, 3] failed, so all is rolled back
        assert self._row_count(connection, table_name) == 0
        assert connection.attr["autocommit"] is True

    def test_failure_in_later_batch_with_autocommit_off_leaves_transaction_to_user(
        self, five_files, empty_table, connection, table_name, all_data
    ):
        self._make_rejected_by_database(five_files[3], all_data.list_dict)
        connection.set_autocommit(False)

        with pytest.raises(ExaImportError):
            connection.import_from_parquet(
                five_files, table_name, callback_params={"max_files_per_batch": 2}
            )

        assert connection.attr["autocommit"] is False
        assert self._row_count(connection, table_name) == 2 * len(all_data.list_dict)
        connection.rollback()
        assert self._row_count(connection, table_name) == 0
