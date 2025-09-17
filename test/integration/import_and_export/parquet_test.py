from pathlib import Path

import pyarrow as pa
import pytest
from integration.import_and_export.helper import select_result
from pyarrow import parquet as pq


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


@pytest.mark.parquet
class TestImportFromParquet:
    @staticmethod
    def _create_parquet_file(path: Path, data: list[dict]):
        table = pa.Table.from_pylist(data)
        pq.write_table(table, path)

    def test_load_single_file(self, tmp_path, connection, table_name, all_data):
        filepath = tmp_path / "single_file.parquet"
        self._create_parquet_file(filepath, all_data.list_dict)

        connection.import_from_parquet(filepath, table_name)

        assert select_result(connection) == all_data.list_tuple()

    def test_load_specific_columns(
        self, tmp_path, connection, table_name, all_data, reduced_import_data
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

    def test_load_files_from_glob_into_empty_table(
        self, tmp_path, connection, table_name, all_data
    ):
        self._create_parquet_file(tmp_path / "first_file.parquet", all_data.list_dict)
        self._create_parquet_file(
            tmp_path / "second_file.parquet", [all_data.list_dict[-1]]
        )

        connection.import_from_parquet(tmp_path, table_name)

        expected = all_data.list_tuple() + [all_data.list_tuple()[-1]]
        assert select_result(connection) == expected
