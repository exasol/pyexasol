import ssl
from pathlib import Path

import pytest

from performance.connection.helper import create_empty_table
from pyexasol import ExaConnection


class BenchmarkSpecifications:
    def __init__(self):
        self.initial_data_size: int = 1_000
        self.target_data_size: int = 4_000_000
        self.rounds: int = 10
        # calculated fields - could move to a setter
        iterations, final_export_size = self.calculate_iterations()
        self.final_export_data_size = final_export_size
        # benchmark does not reset fixtures with scope of test during its rounds
        self.final_import_data_size: int = final_export_size * self.rounds
        self.iterations: int = iterations

    def calculate_iterations(self) -> tuple[int, int]:
        """
        Calculate how many times we need to multiply initial_data_size by 4
        to reach or exceed target_data_size

        Returns:
            tuple: (number_of_iterations, final_value)
        """
        current_value = self.initial_data_size
        iterations = 0
        while current_value < self.target_data_size:
            current_value *= 4
            iterations += 1
        return iterations, current_value


@pytest.fixture(scope="session")
def certification_type(request):
    return ssl.CERT_NONE


@pytest.fixture(scope="session")
def websocket_sslopt(certification_type):
    return {"cert_reqs": certification_type}


@pytest.fixture(scope="session")
def benchmark_specs():
    return BenchmarkSpecifications()


@pytest.fixture(scope="session")
def tmp_source_directory(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("benchmark")


@pytest.fixture(scope="session")
def session_connection(connection_factory):
    con = connection_factory()
    yield con
    con.close()


@pytest.fixture(scope="session")
def create_empty_sales_table(session_connection: ExaConnection):
    table_name = "SALES"
    create_empty_table(connection=session_connection, table_name=table_name)

    yield table_name

    ddl = f"DROP TABLE IF EXISTS {table_name};"
    session_connection.execute(ddl)
    session_connection.commit()


@pytest.fixture(scope="session")
def fill_sales_table(
    session_connection: ExaConnection, create_empty_sales_table, benchmark_specs
):
    initial_query = f"""
        INSERT INTO {create_empty_sales_table} (SALES_TIMESTAMP, PRICE, CUSTOMER_NAME)
        SELECT ADD_SECONDS(TIMESTAMP'2024-01-01 00:00:00', FLOOR(RANDOM() * 365 * 24 * 60 * 60)),
        RANDOM(1, 125),
        SUBSTRING(
        LPAD(TO_CHAR(FLOOR(RANDOM() * 1000000)), 6, '0'),
        1,
        FLOOR(RANDOM() * 3) + 5)
        FROM dual
        CONNECT BY level <= {benchmark_specs.initial_data_size}
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
    for _ in range(benchmark_specs.iterations):
        session_connection.execute(quadrupling_query)
        session_connection.commit()

    return create_empty_sales_table
