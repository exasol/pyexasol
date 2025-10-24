import copy
from datetime import datetime
from inspect import cleandoc
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pytest
from integration.import_and_export.data_sample import DataSample, DATETIME_STR_FORMAT
from integration.import_and_export.helper import select_result
from pyarrow import parquet as pq

ALL_COLUMNS = [
    "FIRST_NAME",
    "LAST_NAME",
    "REGISTER_DT",
    "LAST_VISIT_TS",
    "IS_GRADUATING",
    "AGE",
    "SCORE",
    "WITH_QUOTES",
    "WITH_DOUBLE_QUOTES",
    "WITH_COMMA",
    "WITH_SEMICOLON",
    "WITH_PIPE",
    *[f"WITH_NEWLINE_{j + 1}" for j in range(10)],
]


@pytest.fixture
def empty_table(connection, table_name):
    ddl = cleandoc(
        f"""
        CREATE OR REPLACE TABLE {table_name}
        (
            FIRST_NAME      VARCHAR(200),
            LAST_NAME           VARCHAR(200),
            REGISTER_DT         DATE,
            LAST_VISIT_TS       TIMESTAMP,
            IS_GRADUATING       BOOLEAN,
            AGE                 INTEGER,
            SCORE               DECIMAL(10,2),
            WITH_QUOTES         VARCHAR(5000),
            WITH_DOUBLE_QUOTES  VARCHAR(5000),
            WITH_COMMA          VARCHAR(5000),
            WITH_SEMICOLON      VARCHAR(5000),
            WITH_PIPE           VARCHAR(5000),
            {','.join(f"WITH_NEWLINE_{j + 1}       VARCHAR(5000)" for j in range(10))}
        );
        """
    )
    connection.execute(ddl)
    connection.commit()

    yield table_name

    ddl = f"DROP TABLE IF EXISTS {table_name};"
    connection.execute(ddl)
    connection.commit()


@pytest.fixture
def number_entries():
    return 2000


@pytest.fixture
def data_dict(faker, number_entries) -> list[dict]:
    dates = [faker.date() for _ in range(number_entries)]
    data = [
        {
            "FIRST_NAME": faker.first_name(),
            "LAST_NAME": faker.last_name(),
            "REGISTER_DT": dates[i],
            # setting of faker's random seed did not seem to be
            # fixing seconds, so this was hard-coded instead of
            # using faker's datetime
            "LAST_VISIT_TS": datetime.strptime(
                f"{dates[i]} 12:00:00.900000", DATETIME_STR_FORMAT
            ),
            "IS_GRADUATING": faker.boolean(),
            "AGE": faker.random_int(min=18, max=65),
            "SCORE": faker.pyfloat(min_value=69, max_value=100, right_digits=2),
            "WITH_QUOTES": f"{faker.first_name()}'{faker.last_name()}",
            "WITH_DOUBLE_QUOTES": f'{faker.first_name()}"{faker.last_name()}',
            "WITH_COMMA": f"{faker.first_name()},{faker.last_name()}",
            "WITH_SEMICOLON": f"{faker.first_name()};{faker.last_name()}",
            "WITH_PIPE": f"{faker.first_name()}|{faker.last_name()}",
            **{
                f"WITH_NEWLINE_{j + 1}": f"{faker.first_name()}\n{faker.last_name()}\n"
                * (j + 1)
                for j in range(10)
            },
        }
        for i in range(number_entries)
    ]

    return sorted(data, key=lambda n: n["FIRST_NAME"])


@pytest.fixture
def all_data(data_dict):
    return DataSample(
        columns=ALL_COLUMNS,
        list_dict=copy.deepcopy(data_dict),
    )


@pytest.fixture
def fill_table(connection, table_name, empty_table, data_dict):
    insert = "INSERT INTO {} VALUES({});".format(
        table_name, ",".join([f"{{{C}}}" for C in ALL_COLUMNS])
    )
    for row in data_dict:
        connection.execute(insert, row)


def prepare_parquet_table(list_dict: list[dict]) -> pa.Table:
    table = pa.Table.from_pylist(list_dict)
    table = table.set_column(2, "REGISTER_DT", table["REGISTER_DT"].cast(pa.date32()))
    table = table.set_column(
        3, "LAST_VISIT_TS", table["LAST_VISIT_TS"].cast(pa.timestamp("ns"))
    )
    # this is not ideal as the database & Python data use bool
    return table.set_column(4, "IS_GRADUATING", table["IS_GRADUATING"].cast(pa.int64()))


@pytest.mark.parquet
class TestLargeExportToParquet:
    @staticmethod
    def test_export_single_file(connection, fill_table, tmp_path, table_name, all_data):
        expected = prepare_parquet_table(all_data.list_dict)

        filepath = tmp_path / "single_parquet_dir"
        connection.export_to_parquet(dst=filepath, query_or_table=table_name)

        assert len(list(filepath.glob("*"))) == 1
        # can be a single file name or directory name
        assert pq.read_table(filepath) == expected
