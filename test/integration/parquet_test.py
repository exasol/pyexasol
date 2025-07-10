from collections import defaultdict
from inspect import cleandoc
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pyarrow import parquet as pq


@pytest.fixture
def empty_table(connection):
    name = "USER_SCORES"
    ddl = cleandoc(
        f"""
        CREATE OR REPLACE TABLE {name}
        (
            NAME VARCHAR(200),
            AGE INTEGER,
            SCORE INTEGER
        );
        """
    )
    connection.execute(ddl)
    connection.commit()

    yield name

    ddl = f"DROP TABLE IF EXISTS {name};"
    connection.execute(ddl)
    connection.commit()


@pytest.mark.parquet
class TestImportFromParquet:
    @staticmethod
    def _assemble_data(faker, rows: int = 5) -> dict[str, list[Any]]:
        data = defaultdict(list)
        for _ in range(rows):
            data["name"].append(f"{faker.first_name()} {faker.last_name()}")
            data["age"].append(faker.random_int(min=18, max=65))
            data["score"].append(faker.random_int(min=69, max=100))
        return data

    @staticmethod
    def _create_parquet_file(path: Path, data: dict[str, list[Any]]):
        table = pa.Table.from_pydict(data)
        pq.write_table(table, path)

    @staticmethod
    def _get_table_rows(connection, table_name):
        result = connection.execute(f"SELECT NAME, AGE, SCORE FROM {table_name};")
        return result.fetchall()

    def test_load_single_file_into_empty_table(
        self, connection, empty_table, tmp_path, faker
    ):
        filepath = tmp_path / "single_file.parquet"
        data = self._assemble_data(faker)
        self._create_parquet_file(filepath, data)

        connection.import_from_parquet(filepath, empty_table)
        results = self._get_table_rows(connection, empty_table)

        assert results == list(zip(*data.values()))

    def test_load_files_from_glob_into_empty_table(
        self, connection, empty_table, tmp_path, faker
    ):
        first_data = self._assemble_data(faker)
        self._create_parquet_file(tmp_path / "first_file.parquet", first_data)

        second_data = self._assemble_data(faker)
        self._create_parquet_file(tmp_path / "second_file.parquet", second_data)

        connection.import_from_parquet(tmp_path, empty_table)
        results = self._get_table_rows(connection, empty_table)

        assert results == list(zip(*first_data.values())) + list(
            zip(*second_data.values())
        )
