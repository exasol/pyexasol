import csv
from pathlib import Path
from typing import Callable

import pandas as pd
import polars as pl
import pytest
from pyarrow import dataset

from pyexasol import ExaConnection


def get_csv_length(filepath: Path) -> int:
    with Path(filepath).open(mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        return len(list(reader))


def get_list_length(exported_list: list) -> int:
    return len(exported_list)


def get_pandas_dataframe_length(exported_pandas: pd.DataFrame) -> int:
    return exported_pandas.shape[0]


def get_polars_dataframe_length(exported_polars: pl.DataFrame) -> int:
    return exported_polars.shape[0]


@pytest.mark.parametrize(
    "export_method, file_length_function",
    [
        pytest.param("export_to_list", get_list_length, id="list"),
        pytest.param("export_to_pandas", get_pandas_dataframe_length, id="pandas"),
        pytest.param("export_to_polars", get_polars_dataframe_length, id="polars"),
    ],
)
def test_export_methods_to_memory(
    benchmark,
    benchmark_specs,
    tmp_source_directory,
    connection: ExaConnection,
    fill_sales_table,
    export_method: str,
    file_length_function: Callable,
):
    def func_to_be_measured():
        return getattr(connection, export_method)(
            query_or_table=fill_sales_table,
            query_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        )

    result = benchmark.pedantic(
        func_to_be_measured, iterations=1, rounds=benchmark_specs.rounds
    )

    number_rows = file_length_function(result)

    assert number_rows == benchmark_specs.final_data_size


@pytest.mark.parametrize(
    "export_method, file_extension, file_length_function",
    [
        pytest.param("export_to_file", "csv", get_csv_length, id="file"),
    ],
)
def test_export_methods_to_file(
    benchmark,
    benchmark_specs,
    tmp_source_directory,
    connection: ExaConnection,
    fill_sales_table,
    export_method: str,
    file_extension: str,
    file_length_function: Callable,
):
    export_dst = tmp_source_directory / f"test_data.{file_extension}"

    def func_to_be_measured():
        return getattr(connection, export_method)(
            dst=export_dst,
            query_or_table=fill_sales_table,
            query_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        )

    benchmark.pedantic(
        func_to_be_measured,
        iterations=1,
        rounds=benchmark_specs.rounds,
        warmup_rounds=benchmark_specs.warm_up_rounds,
    )

    number_rows = file_length_function(export_dst)
    assert number_rows == benchmark_specs.final_data_size


def test_export_to_parquet(
    benchmark,
    benchmark_specs,
    tmp_path,
    connection: ExaConnection,
    fill_sales_table,
):
    def func_to_be_measured():
        return connection.export_to_parquet(
            dst=tmp_path,
            query_or_table=fill_sales_table,
            query_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
            callback_params={"existing_data_behavior": "overwrite_or_ignore"},
        )

    benchmark.pedantic(
        func_to_be_measured,
        iterations=1,
        rounds=benchmark_specs.rounds,
        warmup_rounds=benchmark_specs.warm_up_rounds,
    )

    parquet_dataset = dataset.dataset(tmp_path)
    number_rows = sum(
        row_group.num_rows
        for fragment in parquet_dataset.get_fragments()
        for row_group in fragment.row_groups
    )
    assert number_rows == benchmark_specs.final_data_size


def test_export_to_parquet_without_parallel(
    benchmark,
    benchmark_specs,
    tmp_path,
    connection: ExaConnection,
    fill_sales_table,
):
    def func_to_be_measured():
        return connection.export_to_parquet(
            dst=tmp_path,
            query_or_table=fill_sales_table,
            query_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
            callback_params={
                "existing_data_behavior": "overwrite_or_ignore",
                "use_threads": False,
            },
        )

    benchmark.pedantic(
        func_to_be_measured,
        iterations=1,
        rounds=benchmark_specs.rounds,
        warmup_rounds=benchmark_specs.warm_up_rounds,
    )

    parquet_dataset = dataset.dataset(tmp_path)
    number_rows = sum(
        row_group.num_rows
        for fragment in parquet_dataset.get_fragments()
        for row_group in fragment.row_groups
    )
    assert number_rows == benchmark_specs.final_data_size


def test_export_to_parquet_with_multiple_files_with_thread(
    benchmark,
    benchmark_specs,
    tmp_path,
    connection: ExaConnection,
    fill_sales_table,
):
    def func_to_be_measured():
        return connection.export_to_parquet(
            dst=tmp_path,
            query_or_table=fill_sales_table,
            query_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
            callback_params={
                "existing_data_behavior": "overwrite_or_ignore",
                "max_rows_per_file": 1_000_000,
                "max_rows_per_group": 5_000,
            },
        )

    benchmark.pedantic(
        func_to_be_measured,
        iterations=1,
        rounds=benchmark_specs.rounds,
        warmup_rounds=benchmark_specs.warm_up_rounds,
    )

    parquet_dataset = dataset.dataset(tmp_path)
    number_rows = sum(
        row_group.num_rows
        for fragment in parquet_dataset.get_fragments()
        for row_group in fragment.row_groups
    )
    assert number_rows == benchmark_specs.final_data_size


def test_export_to_parquet_with_multiple_files_without_thread(
    benchmark,
    benchmark_specs,
    tmp_path,
    connection: ExaConnection,
    fill_sales_table,
):
    def func_to_be_measured():
        return connection.export_to_parquet(
            dst=tmp_path,
            query_or_table=fill_sales_table,
            query_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
            callback_params={
                "existing_data_behavior": "overwrite_or_ignore",
                "use_threads": False,
                "max_rows_per_file": 1_000_000,
                "max_rows_per_group": 5_000,
            },
        )

    benchmark.pedantic(
        func_to_be_measured,
        iterations=1,
        rounds=benchmark_specs.rounds,
        warmup_rounds=benchmark_specs.warm_up_rounds,
    )

    parquet_dataset = dataset.dataset(tmp_path)
    number_rows = sum(
        row_group.num_rows
        for fragment in parquet_dataset.get_fragments()
        for row_group in fragment.row_groups
    )
    assert number_rows == benchmark_specs.final_data_size
