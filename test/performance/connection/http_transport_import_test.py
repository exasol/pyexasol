import csv
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow.csv as py_csv
import pyarrow.parquet as pq
import pytest

from performance.connection.helper import create_empty_table
from pyexasol import ExaConnection


@pytest.fixture
def empty_import_into_table(connection: ExaConnection):
    table_name = "TMP_SALES_COPY"
    create_empty_table(connection=connection, table_name=table_name)

    yield table_name

    ddl = f"DROP TABLE IF EXISTS {table_name};"
    connection.execute(ddl)
    connection.commit()


@pytest.fixture(scope="session")
def create_csv(
    session_connection: ExaConnection, tmp_source_directory: Path, fill_sales_table
):
    csv_path = tmp_source_directory / "test_data.csv"
    session_connection.export_to_file(
        csv_path,
        f"SELECT SALES_TIMESTAMP, PRICE, CUSTOMER_NAME FROM {fill_sales_table}",
    )
    return csv_path


@pytest.fixture(scope="session")
def create_iterable(session_connection: ExaConnection, create_csv):
    with Path(create_csv).open(mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        return list(reader)


@pytest.fixture(scope="session")
def create_parquet(
    session_connection: ExaConnection, tmp_source_directory: Path, create_csv
):
    parquet_path = tmp_source_directory / "test_data.parquet"

    table = py_csv.read_csv(
        create_csv,
        read_options=py_csv.ReadOptions(
            column_names=["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"],
        ),
    )
    pq.write_table(table, parquet_path)

    return parquet_path


@pytest.fixture(scope="session")
def create_pandas_dataframe(session_connection: ExaConnection, create_csv):
    return pd.read_csv(create_csv, header=None)


@pytest.fixture(scope="session")
def create_polars_dataframe(session_connection: ExaConnection, create_csv):
    return pl.read_csv(
        create_csv, has_header=False, rechunk=False, infer_schema_length=10000
    )


@pytest.mark.parametrize(
    "import_method, data_creator",
    [
        pytest.param("import_from_file", "create_csv", id="file"),
        pytest.param("import_from_iterable", "create_iterable", id="iterable"),
        pytest.param("import_from_pandas", "create_pandas_dataframe", id="pandas"),
        pytest.param("import_from_parquet", "create_parquet", id="parquet"),
        pytest.param("import_from_polars", "create_polars_dataframe", id="polars"),
    ],
)
def test_import_methods(
    benchmark,
    benchmark_specs,
    connection: ExaConnection,
    request,
    empty_import_into_table,
    import_method: str,
    data_creator: str,
):
    data = request.getfixturevalue(data_creator)

    def func_to_be_measured():
        return getattr(connection, import_method)(
            data,
            table=empty_import_into_table,
            import_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        )

    benchmark.pedantic(
        func_to_be_measured,
        iterations=1,
        rounds=benchmark_specs.rounds,
        warmup_rounds=benchmark_specs.warm_up_rounds,
    )

    count_query = f"SELECT count(*) FROM {empty_import_into_table};"
    count = connection.execute(count_query).fetchval()
    assert count == benchmark_specs.final_import_data_size
