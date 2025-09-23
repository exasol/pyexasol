import csv
from pathlib import Path
from typing import (
    Callable,
    Optional,
)

import pandas as pd
import polars as pl
import pytest
from pyarrow import dataset

from pyexasol import ExaConnection


def get_csv_length(filepath: Path) -> int:
    with Path(filepath).open(mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        return len(list(reader))


def get_parquet_length(filepath: Path) -> int:
    parquet_dataset = dataset.dataset(filepath)
    return sum(
        row_group.num_rows
        for fragment in parquet_dataset.get_fragments()
        for row_group in fragment.row_groups
    )


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
    connection: ExaConnection,
    columns,
    fill_sales_table,
    export_method: str,
    file_length_function: Callable,
):
    def func_to_be_measured():
        return getattr(connection, export_method)(
            query_or_table=fill_sales_table,
            query_params={"columns": columns},
        )

    result = benchmark.pedantic(
        func_to_be_measured, iterations=1, rounds=benchmark_specs.rounds
    )

    number_rows = file_length_function(result)
    assert number_rows == benchmark_specs.final_data_size


@pytest.mark.parametrize(
    "export_method, file_extension, file_length_function, callback_params",
    [
        pytest.param("export_to_file", "csv", get_csv_length, None, id="csv"),
        # writes files to directory so no file_extension is needed
        pytest.param(
            "export_to_parquet",
            "",
            get_parquet_length,
            {"existing_data_behavior": "overwrite_or_ignore"},
            id="parquet",
        ),
    ],
)
def test_export_methods_to_file(
    benchmark,
    benchmark_specs,
    tmp_path,
    connection: ExaConnection,
    columns,
    fill_sales_table,
    export_method: str,
    file_extension: str,
    file_length_function: Callable,
    callback_params: Optional[dict],
):
    export_dst = tmp_path / f"test_data{file_extension}"

    def func_to_be_measured():
        additional_params = {}
        if callback_params is not None:
            additional_params = {"callback_params": callback_params}

        return getattr(connection, export_method)(
            dst=export_dst,
            query_or_table=fill_sales_table,
            query_params={"columns": columns},
            **additional_params,
        )

    benchmark.pedantic(
        func_to_be_measured,
        iterations=1,
        rounds=benchmark_specs.rounds,
        warmup_rounds=benchmark_specs.warm_up_rounds,
    )

    number_rows = file_length_function(export_dst)
    assert number_rows == benchmark_specs.final_data_size
