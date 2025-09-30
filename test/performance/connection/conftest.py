import os
import ssl
from pathlib import Path
from typing import Final

import pytest

from performance.connection.helper import create_empty_table
from pyexasol import ExaConnection


@pytest.fixture(scope="session")
def dsn(default_ipaddr, default_port):
    return os.environ.get("EXAHOST", f"{default_ipaddr}:{default_port}")


class BenchmarkSpecifications:
    def __init__(self):
        self.initial_data_size: Final[int] = 1_000
        # This value was chosen with the specified data creation to generate
        # around 1 GB of throughput.
        self.target_data_size: Final[int] = 4_000_000
        # In testing, it was noticed that first round always is faster (cache), so we
        # always do a warm-up round so that the aggregated values are not biased from
        # this.
        self.warm_up_rounds: Final[int] = 1
        # If other settings are changed, it's useful to look at the "data" values for
        # each run to see if any patterns emerge, like every 7th run is longer, as
        # there may be other unforeseen consequences -- like when DB flushing occurs --
        # in running a test multiple times. Such consequences can lead to a bias in the
        # aggregated values too.
        self.rounds: Final[int] = 15
        iterations, final_export_size = self.calculate_iterations()
        self.final_data_size: Final[int] = final_export_size
        self.iterations: Final[int] = iterations

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
def certificate_type(request):
    return ssl.CERT_NONE


@pytest.fixture(scope="session")
def websocket_sslopt(certificate_type):
    return {"cert_reqs": certificate_type}


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


@pytest.fixture(scope="session", autouse=True)
def set_up_initial_schema(connection_factory, schema):
    con = connection_factory(schema="")
    con.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
    con.execute(f"CREATE SCHEMA {schema};")
    con.close()


@pytest.fixture(scope="session")
def columns():
    return ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]


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
