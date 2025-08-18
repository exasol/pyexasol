import csv
from pathlib import Path
from typing import Callable

import pandas as pd
import polars as pl
import pytest

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
    "export_method, file_extension, file_length_function",
    [
        pytest.param("export_to_file", "csv", get_csv_length, id="file"),
        pytest.param("export_to_list", None, get_list_length, id="list"),
        pytest.param(
            "export_to_pandas", None, get_pandas_dataframe_length, id="pandas"
        ),
        pytest.param(
            "export_to_polars", None, get_polars_dataframe_length, id="polars"
        ),
    ],
)
def test_export_methods(
    benchmark,
    benchmark_specs,
    tmp_source_directory,
    connection: ExaConnection,
    fill_sales_table,
    export_method: str,
    file_extension: str,
    file_length_function: Callable,
):
    export_dst = None
    if file_extension:
        export_dst = tmp_source_directory / f"test_data.{file_extension}"

    def func_to_be_measured():
        kwargs = {
            "query_or_table": fill_sales_table,
            "query_params": {"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        }
        if file_extension:
            kwargs["dst"] = export_dst
        return getattr(connection, export_method)(**kwargs)

    result = benchmark.pedantic(
        func_to_be_measured, iterations=1, rounds=benchmark_specs.rounds
    )

    if file_extension:
        number_rows = file_length_function(export_dst)
    else:
        number_rows = file_length_function(result)

    assert number_rows == benchmark_specs.final_export_data_size
