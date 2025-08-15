from abc import (
    ABC,
    abstractmethod,
)
from inspect import cleandoc
from pathlib import Path
from typing import Optional

import pytest

from pyexasol import ExaConnection

"""pytest-benchmark
- only allows one usage of measurement per test run...
- SHOULD not include the preparation step, only the sending step
- can control how many round, etc. with pedantic
- by default will re-run multiple times without resetting fully -> this means for import
 that the data becomes much greater over time

need to prepare data in efficient/universal-way as input -> faker?, cheats with export?
worthwhile with import to create large file or just string together x rows, y times.

do test against
- csv -> direct DB call
- file
- iterable
- pandas
- parquet
- polars

discuss results with Torsten before too much development to see if right direction
"""

"""
Hmm, based on an AI response to the question, we have
Column sizes:
  SALES_ID            :   18 bytes
  SALES_TIMESTAMP     :   26 bytes
  PRICE               :   11 bytes
  CUSTOMER_NAME       :  200 bytes

CSV formatting overhead:
  quotes              :   10 bytes
  delimiters          :    4 bytes
  newline             :    2 bytes

Total row size: 271 bytes

Rows per GB: ~4 million

Batch processing test:
Time to generate 100,000 rows in batches: 1.34 seconds
Estimated time for 4M rows with batch processing: 50.77 seconds (0.01 hours)
"""

EXPORT_FROM_TABLE = "SALES"
IMPORT_INTO_TABLE = "TMP_SALES_COPY"
INITIAL_SIZE = 1_000
FINAL_SIZE = 4_000_000


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
def create_empty_sales_table(session_connection: ExaConnection):
    create_empty_table(connection=session_connection, table_name=EXPORT_FROM_TABLE)
    yield EXPORT_FROM_TABLE

    ddl = f"DROP TABLE IF EXISTS {EXPORT_FROM_TABLE};"
    session_connection.execute(ddl)
    session_connection.commit()


@pytest.fixture(scope="session")
def create_initial_sales_data(
    create_empty_sales_table, session_connection: ExaConnection
):
    query = f"""
        INSERT INTO {EXPORT_FROM_TABLE} (SALES_TIMESTAMP, PRICE, CUSTOMER_NAME)
        SELECT ADD_SECONDS(TIMESTAMP'2024-01-01 00:00:00', FLOOR(RANDOM() * 365 * 24 * 60 * 60)),
        RANDOM(1, 125),
        SUBSTRING(
        LPAD(TO_CHAR(FLOOR(RANDOM() * 1000000)), 6, '0'),
        1,
        FLOOR(RANDOM() * 3) + 5)
        FROM dual
        CONNECT BY level <= {INITIAL_SIZE}
    """
    session_connection.execute(query)
    session_connection.commit()


# def fill_table(connection: ExaConnection):
#     create_empty_table(connection=connection)
#     create_initial_data(connection=connection)
#     # then do table operations to get to final size
#
#
# fill_table(connection=connection)
class ImportBenchmark(ABC):
    """
    Interface class used for benchmarking various import methods.
    It's recommended in the pytest-benchmark tool to check the output
    to see that the call finished as expected. Many of the import
    functions, however, do not have a non-None result.
    """

    def __init__(self, connection: ExaConnection, source_directory: Path):
        self.connection = connection
        self.source_directory = source_directory
        self.export_from_table_name = EXPORT_FROM_TABLE
        self.import_into_table_name = IMPORT_INTO_TABLE

    @property
    @abstractmethod
    def source_file(self):
        pass

    @property
    def source_filepath(self) -> Optional[Path]:
        return self.source_directory / self.source_file

    def create_empty_table(self):
        create_empty_table(
            connection=self.connection, table_name=self.import_into_table_name
        )

    @abstractmethod
    def prepare_data(self):
        pass

    @abstractmethod
    def execute_import_command(self):
        pass

    def perform_import(self):
        self.execute_import_command()
        count_query = f"SELECT count(*) FROM {self.import_into_table_name};"
        return self.connection.execute(count_query).fetchval()


class ImportCsvBenchmark(ImportBenchmark):
    @property
    def source_file(self):
        return "sample_import.csv"

    def prepare_data(self):
        # save some time by only exporting the file once
        if self.source_filepath.is_file():
            return
        self.connection.export_to_file(
            self.source_filepath,
            f"SELECT SALES_TIMESTAMP, PRICE, CUSTOMER_NAME FROM {self.export_from_table_name}",
        )

    def execute_import_command(self):
        self.connection.import_from_file(
            self.source_filepath,
            table=self.import_into_table_name,
            import_params={"columns": ["SALES_TIMESTAMP", "PRICE", "CUSTOMER_NAME"]},
        )


# schema = pa.schema([
#     ("SALES_TIMESTAMP", pa.timestamp("ms")),
#     ("PRICE", pa.float64()),
#     ("CUSTOMER_NAME", pa.string())
# ])
#


class ImportParquetBenchmark(ImportBenchmark):
    @property
    def source_file(self):
        return "sample_import.parquet"

    def prepare_data(self):
        # save some time by only exporting the file once
        # bit more complicated as we don't already have an export to parquet,
        # not sure how "optimized" it should be here. could do to file & stream to build it?
        if self.source_filepath.is_file():
            return

        # with pq.ParquetWriter(self.source_file, schema) as writer:
        #     for batch in data_generator():
        #         table = pa.Table.from_pydict(batch, schema=schema)
        #         writer.write_table(table)
        # self.connection.export_to_file(
        #     self.source_filepath,
        #     f"-- SELECT SALES_TIMESTAMP, PRICE, CUSTOMER_NAME FROM {self.export_from_table_name}",
        # )

    def execute_import_command(self):
        self.connection.import_from_parquet(
            source=self.source_filepath,
            table=self.import_into_table_name,
        )


@pytest.mark.parametrize(
    "import_class",
    (
        pytest.param(ImportCsvBenchmark, id="csv"),
        # pytest.param(ImportParquetBenchmark, id="parquet"),
    ),
)
@pytest.mark.benchmark
def test_import(
    benchmark,
    import_class: ImportBenchmark,
    connection: ExaConnection,
    tmp_source_directory: Path,
    create_initial_sales_data,
):
    # currently, connection runs with NO_CERT & WITH_CERT, for local testing only
    # NO_CERT was used as WITH_CERT adds the complexity of a VM. considering
    # our main concern is the performance of the import functions, we might want to
    # only consider NO_CERT.

    def setup():
        # Reset state here; need at 0 so data doesn't increase throughout runs
        global import_object
        import_object = import_class(
            connection=connection, source_directory=tmp_source_directory
        )
        # need empty table made & to export data to required format
        import_object.create_empty_table()
        import_object.prepare_data()

    def func_to_be_measured():
        return import_object.perform_import()

    result = benchmark.pedantic(
        func_to_be_measured, setup=setup, iterations=1, rounds=5
    )

    # we universally return this result to indicate it resolved as expected
    # and to prevent accidents in implementation
    assert result == INITIAL_SIZE  # FINAL_SIZE
