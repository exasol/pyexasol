from inspect import cleandoc
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pytest

from pyexasol import ExaConnection


def calculate_iterations(start_value: int, target_value: int) -> tuple[int, int]:
    """
    Calculate how many times we need to multiply start_value by 4
    to reach or exceed target_value

    Returns:
        tuple: (number_of_iterations, final_value)
    """
    current_value = start_value
    iterations = 0
    while current_value < target_value:
        current_value *= 4
        iterations += 1
    return iterations, current_value


INITIAL_SIZE = 1_000
ROUNDS = 5
ITERATIONS, FINAL_SIZE = calculate_iterations(
    start_value=INITIAL_SIZE, target_value=4_000_000
)


def create_empty_table(connection: ExaConnection, table_name: str):
    ddl = cleandoc(
        f"""
        CREATE OR REPLACE TABLE {table_name} (
            SALES_ID                DECIMAL(18,0) IDENTITY NOT NULL PRIMARY KEY,
            SALES_TIMESTAMP         TIMESTAMP,
            PRICE                   DECIMAL(9,2),
            CUSTOMER_NAME           VARCHAR(200)
          );
        """
    )
    connection.execute(ddl)
    connection.commit()


@pytest.fixture(scope="session")
def session_connection(connection_factory):
    con = connection_factory()
    yield con
    con.close()


@pytest.fixture(scope="session")
def tmp_source_directory(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("benchmark")


@pytest.fixture(scope="session")
def create_empty_sales_table(session_connection: ExaConnection) -> str:
    table_name = "SALES"
    create_empty_table(connection=session_connection, table_name=table_name)

    yield table_name

    ddl = f"DROP TABLE IF EXISTS {table_name};"
    session_connection.execute(ddl)
    session_connection.commit()


@pytest.fixture
def empty_import_into_table(connection: ExaConnection):
    table_name = "TMP_SALES_COPY"
    create_empty_table(connection=connection, table_name=table_name)

    yield table_name

    ddl = f"DROP TABLE IF EXISTS {table_name};"
    connection.execute(ddl)
    connection.commit()


@pytest.fixture(scope="session")
def fill_sales_table(create_empty_sales_table, session_connection: ExaConnection):
    initial_query = f"""
        INSERT INTO {create_empty_sales_table} (SALES_TIMESTAMP, PRICE, CUSTOMER_NAME)
        SELECT ADD_SECONDS(TIMESTAMP'2024-01-01 00:00:00', FLOOR(RANDOM() * 365 * 24 * 60 * 60)),
        RANDOM(1, 125),
        SUBSTRING(
        LPAD(TO_CHAR(FLOOR(RANDOM() * 1000000)), 6, '0'),
        1,
        FLOOR(RANDOM() * 3) + 5)
        FROM dual
        CONNECT BY level <= {INITIAL_SIZE}
    """
    session_connection.execute(initial_query)
    session_connection.commit()

    quadrupling_query = f"""
    INSERT INTO {create_empty_sales_table} (SALES_TIMESTAMP, PRICE, CUSTOMER_NAME)
        SELECT * FROM (
            SELECT SALES_TIMESTAMP, PRICE, CUSTOMER_NAME FROM {create_empty_sales_table}
            UNION ALL
            SELECT SALES_TIMESTAMP, PRICE, CUSTOMER_NAME FROM {create_empty_sales_table}
            UNION ALL
            SELECT SALES_TIMESTAMP, PRICE, CUSTOMER_NAME FROM {create_empty_sales_table}
        ) AS doubled_data;
    """
    for _ in range(ITERATIONS):
        session_connection.execute(quadrupling_query)
        session_connection.commit()

    return create_empty_sales_table


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
def create_parquet(
    session_connection: ExaConnection, tmp_source_directory: Path, create_csv
):
    parquet_path = tmp_source_directory / "test_data.parquet"

    table = csv.read_csv(
        create_csv,
        read_options=csv.ReadOptions(
            column_names=["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"],
        ),
    )
    pq.write_table(table, parquet_path)

    return parquet_path


@pytest.fixture(scope="session")
def create_pandas_dataframe(
    session_connection: ExaConnection, tmp_source_directory: Path, create_csv
):
    return pd.read_csv(create_csv, header=None)


@pytest.fixture(scope="session")
def create_polars_dataframe(
    session_connection: ExaConnection, tmp_source_directory: Path, create_csv
):
    return pl.read_csv(
        create_csv, has_header=False, rechunk=False, infer_schema_length=10000
    )


def test_import_from_file(
    benchmark,
    connection: ExaConnection,
    create_csv,
    empty_import_into_table,
):
    def func_to_be_measured():
        return connection.import_from_file(
            create_csv,
            table=empty_import_into_table,
            import_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        )

    benchmark.pedantic(func_to_be_measured, iterations=1, rounds=ROUNDS)

    count_query = f"SELECT count(*) FROM {empty_import_into_table};"
    count = connection.execute(count_query).fetchval()
    assert count == FINAL_SIZE * ROUNDS


def test_import_from_pandas(
    benchmark,
    connection: ExaConnection,
    create_pandas_dataframe,
    empty_import_into_table,
):
    def func_to_be_measured():
        return connection.import_from_pandas(
            create_pandas_dataframe,
            table=empty_import_into_table,
            import_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        )

    benchmark.pedantic(func_to_be_measured, iterations=1, rounds=ROUNDS)

    count_query = f"SELECT count(*) FROM {empty_import_into_table};"
    count = connection.execute(count_query).fetchval()
    assert count == FINAL_SIZE * ROUNDS


def test_import_from_polars(
    benchmark,
    connection: ExaConnection,
    create_polars_dataframe,
    empty_import_into_table,
):
    def func_to_be_measured():
        return connection.import_from_polars(
            create_polars_dataframe,
            table=empty_import_into_table,
            import_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        )

    benchmark.pedantic(func_to_be_measured, iterations=1, rounds=ROUNDS)

    count_query = f"SELECT count(*) FROM {empty_import_into_table};"
    count = connection.execute(count_query).fetchval()
    assert count == FINAL_SIZE * ROUNDS


def test_import_from_parquet(
    benchmark,
    connection: ExaConnection,
    create_parquet,
    empty_import_into_table,
):
    def func_to_be_measured():
        return connection.import_from_parquet(
            create_parquet,
            table=empty_import_into_table,
            import_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        )

    benchmark.pedantic(func_to_be_measured, iterations=1, rounds=ROUNDS)

    count_query = f"SELECT count(*) FROM {empty_import_into_table};"
    count = connection.execute(count_query).fetchval()
    assert count == FINAL_SIZE * ROUNDS
